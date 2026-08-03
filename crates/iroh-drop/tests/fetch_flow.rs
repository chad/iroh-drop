//! Fetch flows. The first test is the designated *real-carrier smoke
//! test*: it proves the whole loop — join via ticket, publish, fetch,
//! replication — over actual iroh-gossip, the carrier production uses.
//! Everything else runs on the in-memory carrier (see `common::mem_transport`),
//! which makes protocol logic fast and deterministic. `catch_up_sync.rs`
//! covers the sync side of the real carrier.

mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::DropError;
use iroh_drop::DropPolicy;
use iroh_drop::FetchOutput;
use iroh_drop::LocalBlobStatus;

#[tokio::test]
async fn two_peers_announce_and_fetch() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let mut events_a = session_a.subscribe();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let mut events_b = session_b.subscribe();

    // B sees A as a peer.
    wait_event(&mut events_b, "peer joined", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    let published = session_a
        .publish_bytes("hello.txt".into(), Bytes::from_static(b"hello drop"))
        .await
        .unwrap();

    // B receives the offer through gossip, with A as verified author.
    let ev = wait_event(&mut events_b, "offer", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;
    let DropEvent::OfferReceived { from, offer } = ev else {
        unreachable!()
    };
    assert_eq!(from, session_a.self_id());
    assert_eq!(offer.blob_hash, published.hash);
    assert_eq!(offer.name, "hello.txt");
    assert_eq!(offer.size, 10);

    // B fetches manually; the bytes verify against the advertised hash.
    let result = session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(result.size, 10);
    assert!(!result.already_local);
    assert_eq!(result.provider, Some(session_a.self_id()));

    // B announced availability; A now knows B as a provider (replication!).
    wait_event(&mut events_a, "provider available", |ev| {
        matches!(
            ev,
            DropEvent::ProviderAvailable { hash, peer }
                if *hash == published.hash && *peer == session_b.self_id()
        )
    })
    .await;

    // Local statuses reflect completion on both sides.
    for session in [&session_a, &session_b] {
        let offers = session.offers();
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].local_status, LocalBlobStatus::Complete);
    }
}

#[tokio::test]
async fn auto_fetch_flow() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = mem_protocol(
        &bus,
        DropPolicy {
            auto_fetch: true,
            ..Default::default()
        },
    )
    .await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything publishes, so no event races past us.
    let mut events_b = session_b.subscribe();

    let payload = vec![42u8; 256 * 1024];
    let published = session_a
        .publish_bytes("blob.bin".into(), Bytes::from(payload.clone()))
        .await
        .unwrap();

    // B auto-fetches without any manual call.
    wait_event(
        &mut events_b,
        "auto fetch complete",
        is_fetch_completed(published.hash),
    )
    .await;

    // The blob was exported into B's policy output directory.
    let offers = session_b.offers();
    assert_eq!(offers[0].local_status, LocalBlobStatus::Complete);
    let exported = std::fs::read("./downloads/blob.bin").expect("exported file");
    assert_eq!(exported, payload);
    std::fs::remove_file("./downloads/blob.bin").ok();
}

#[tokio::test]
async fn fetch_unknown_hash_fails_retryably() {
    // A short provider timeout keeps the failure snappy; the default ten
    // seconds per request round is for real networks with slow peers.
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = mem_protocol(
        &bus,
        DropPolicy {
            provider_timeout: std::time::Duration::from_millis(200),
            ..Default::default()
        },
    )
    .await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let missing = iroh_drop::BlobHash::from_bytes([0xee; 32]);
    let err = session_b
        .fetch(missing, FetchOutput::Store)
        .await
        .unwrap_err();
    assert!(matches!(err, DropError::Network(_)), "got {err:?}");

    // The session stays healthy afterwards.
    let published = session_a
        .publish_bytes("later.txt".into(), Bytes::from_static(b"later"))
        .await
        .unwrap();
    session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
}
