//! The composability contract: what a host application can do with the
//! protocol without forking it.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use common::{ticket_for, wait_event};
use iroh_drop::builder::{DropBuilder, StackOptions};
use iroh_drop::policy::{DropPolicy, OfferContext, OfferDecider, OfferDecision};
use iroh_drop::{DropEvent, LocalBlobStatus};

/// A decider that only accepts offers from an allowlist — the canonical thing
/// a host wants and the protocol should not have to know about.
#[derive(Debug)]
struct Allowlist {
    allowed: Vec<iroh::EndpointId>,
    seen: AtomicUsize,
}

impl OfferDecider for Allowlist {
    fn decide(&self, _offer: &iroh_drop::message::OfferV1, ctx: &OfferContext) -> OfferDecision {
        self.seen.fetch_add(1, Ordering::SeqCst);
        if self.allowed.contains(&ctx.author) {
            OfferDecision::Accept
        } else {
            OfferDecision::Reject("not on my allowlist".into())
        }
    }
}

async fn protocol_with(
    decider: Option<Arc<dyn OfferDecider>>,
    policy: DropPolicy,
) -> iroh_drop::builder::DropProtocol {
    let mut builder = DropBuilder::from_options(StackOptions {
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .policy(policy);
    if let Some(decider) = decider {
        builder = builder.decider(decider);
    }
    builder.build().await.unwrap()
}

#[tokio::test]
async fn a_host_can_veto_offers_with_its_own_rules() {
    let proto_a = protocol_with(None, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    // B allows nobody, so A's offers are refused even though they are
    // perfectly valid protocol messages.
    let allowlist = Arc::new(Allowlist {
        allowed: vec![],
        seen: AtomicUsize::new(0),
    });
    let proto_b = protocol_with(
        Some(allowlist.clone()),
        DropPolicy {
            auto_fetch: true,
            ..DropPolicy::default()
        },
    )
    .await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let mut events_b = session_b.subscribe();
    wait_event(&mut events_b, "peer joined", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    session_a
        .publish_bytes("unwanted.txt".into(), Bytes::from_static(b"no thanks"))
        .await
        .unwrap();

    wait_event(&mut events_b, "veto", |ev| {
        matches!(ev, DropEvent::OfferRejected { .. })
    })
    .await;
    assert!(
        session_b.offers().is_empty(),
        "a vetoed offer must not be recorded at all"
    );
    assert!(allowlist.seen.load(Ordering::SeqCst) > 0, "decider ran");

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// `RecordOnly` keeps the inventory but refuses the automatic pull, which is
/// how a host implements "ask me first" without turning auto-fetch off.
#[tokio::test]
async fn record_only_remembers_without_fetching() {
    #[derive(Debug)]
    struct AskFirst;
    impl OfferDecider for AskFirst {
        fn decide(
            &self,
            _offer: &iroh_drop::message::OfferV1,
            _ctx: &OfferContext,
        ) -> OfferDecision {
            OfferDecision::RecordOnly
        }
    }

    let proto_a = protocol_with(None, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = protocol_with(
        Some(Arc::new(AskFirst)),
        DropPolicy {
            auto_fetch: true,
            ..DropPolicy::default()
        },
    )
    .await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let mut events_b = session_b.subscribe();
    wait_event(&mut events_b, "peer joined", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    let published = session_a
        .publish_bytes("later.txt".into(), Bytes::from_static(b"maybe later"))
        .await
        .unwrap();
    wait_event(&mut events_b, "offer", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;

    // Recorded, and deliberately not fetched.
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;
    let record = session_b
        .offers()
        .into_iter()
        .find(|r| r.offer.blob_hash == published.hash)
        .expect("the offer is in the inventory");
    assert_eq!(record.local_status, LocalBlobStatus::Missing);

    // The user can still fetch it explicitly.
    let result = session_b
        .fetch(published.hash, iroh_drop::FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(result.size, 11);

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// A host that already owns an endpoint, gossip, blobs and router can embed
/// drops with `from_parts` instead of letting us build a second stack.
#[tokio::test]
async fn from_parts_embeds_into_a_host_stack() {
    use iroh::protocol::Router;
    use iroh_blobs::store::mem::MemStore;
    use iroh_blobs::BlobsProtocol;
    use iroh_gossip::net::Gossip;

    // --- the host's own stack, built the way any iroh app would ---
    let lookup = iroh::address_lookup::memory::MemoryLookup::new();
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .relay_mode(iroh::RelayMode::Disabled)
        .address_lookup(lookup.clone())
        .bind()
        .await
        .unwrap();
    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, None);
    let gossip = Gossip::builder()
        .max_message_size(iroh_drop::message::MAX_MESSAGE_SIZE)
        .spawn(endpoint.clone());

    // --- drops added to it, sharing everything ---
    let stack = iroh_drop::builder::DropStack::from_parts(
        endpoint.clone(),
        gossip.clone(),
        blobs.clone(),
        iroh_blobs::api::Store::clone(&store),
        Some(lookup.clone()),
    );
    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .accept(iroh_drop::DROP_ALPN, stack.sync_handler())
        .spawn();

    let host_protocol = DropBuilder::new(Arc::new(stack)).build().await.unwrap();
    let session = host_protocol.create(Default::default()).await.unwrap();
    let published = session
        .publish_bytes("embedded.txt".into(), Bytes::from_static(b"host owned"))
        .await
        .unwrap();
    assert_eq!(published.name, "embedded.txt");

    // A joiner reaches the embedded drop, including its catch-up sync over
    // the ALPN the *host* registered.
    let joiner = protocol_with(None, DropPolicy::default()).await;
    let session_j = joiner
        .join(ticket_for(&session, endpoint.addr()))
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + common::TIMEOUT;
    loop {
        if session_j
            .offers()
            .iter()
            .any(|r| r.offer.blob_hash == published.hash)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the embedded stack never served its history"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let fetched = session_j
        .fetch(published.hash, iroh_drop::FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(fetched.size, 10);

    session_j.shutdown_no_announce().await;
    drop(session_j);
    drop(session);
    joiner.shutdown().await.ok();
    // The host shuts down its own stack; iroh-drop must not have taken it over.
    router.shutdown().await.unwrap();
    endpoint.close().await;
}
