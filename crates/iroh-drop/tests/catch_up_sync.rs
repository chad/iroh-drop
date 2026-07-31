//! Catch-up sync: a peer that joins *after* offers were broadcast still
//! learns about them, by name, without anybody re-announcing.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{protocol, ticket_for, TIMEOUT};
use iroh_drop::policy::DropPolicy;
use iroh_drop::session::FetchOutput;

/// The core late-joiner story: A publishes before C exists. C joins and sees
/// the offer with its filename, and can fetch it by name.
#[tokio::test]
async fn late_joiner_learns_offers_by_name() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    // Published before anyone else is around: no live listener at all.
    let published = session_a
        .publish_bytes(
            "report.pdf".into(),
            Bytes::from_static(b"late joiner payload"),
        )
        .await
        .unwrap();

    let proto_c = protocol(DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    // Sync runs on join; poll until the inventory shows up.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let offer = loop {
        if let Some(record) = session_c
            .offers()
            .into_iter()
            .find(|o| o.offer.name == "report.pdf")
        {
            break record;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "late joiner never learned the offer through catch-up sync"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(offer.offer.blob_hash, published.hash);
    assert_eq!(offer.offer.size, 19);

    // Names are enough: no hash needed by the user.
    let hash = session_c.resolve("report.pdf").expect("resolve by name");
    let result = session_c.fetch(hash, FetchOutput::Store).await.unwrap();
    assert_eq!(result.hash, published.hash);

    session_c.shutdown_no_announce().await;
    drop(session_c);
    drop(session_a);
    proto_c.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// Sync serves the log of *other* authors too: C joins through B, which only
/// knows A's offer because it was gossiped to it.
#[tokio::test]
async fn sync_relays_other_authors_offers() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    // B must be a neighbor before the broadcast, or the offer is lost:
    // gossip has no history, which is exactly why sync exists.
    let mut events_b = session_b.subscribe();
    common::wait_event(&mut events_b, "peer joined", |ev| {
        matches!(ev, common::DropEvent::PeerJoined { .. })
    })
    .await;

    // B is live for this one: it arrives through gossip.
    let published = session_a
        .publish_bytes("shared.txt".into(), Bytes::from_static(b"relayed by B"))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while session_b.offers().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "B never saw the live offer"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // C bootstraps off B only, and must still learn A's offer.
    let proto_c = protocol(DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_b, proto_b.stack().addr()))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Some(record) = session_c
            .offers()
            .into_iter()
            .find(|o| o.offer.name == "shared.txt")
        {
            // The offer is still attributed to its original author.
            assert_eq!(record.offer.blob_hash, published.hash);
            assert_eq!(record.first_seen_from, session_a.self_id());
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "C never learned A's offer through B's sync log"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    session_c.shutdown_no_announce().await;
    drop(session_c);
    drop(session_b);
    drop(session_a);
    proto_c.shutdown().await.ok();
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}
