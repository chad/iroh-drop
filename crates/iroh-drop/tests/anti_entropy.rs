//! Anti-entropy: a neighbor that appears *after* join-time catch-up has
//! failed (or was never possible) still gets synced from. This is what makes
//! "stay in the group and see whatever is offered" survive both sides
//! restarting at different times: membership alone reconnects the swarm,
//! and the first neighbor-up pulls the missing history across.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{mem_protocol, ticket_for, MemBus, TIMEOUT};
use iroh::EndpointAddr;
use iroh_drop::policy::DropPolicy;
use iroh_drop::DropTicket;

/// B joins with a ticket whose only bootstrap is dead, so the join-time
/// catch-up goes nowhere and the swarm is empty. When B is then told about
/// A (the path a fresher ticket takes through the daemon's re-join), the
/// neighbor-up alone must deliver A's history — nobody re-publishes
/// anything.
#[tokio::test]
async fn a_new_neighbor_brings_the_history_we_missed() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let published = session_a
        .publish_bytes("one.txt".into(), Bytes::from_static(b"first"))
        .await
        .unwrap();

    // A bootstrap that answers nothing: a random id at a closed port.
    let dead = EndpointAddr::new(iroh::SecretKey::generate().public());
    let stale_ticket = DropTicket::new(
        *session_a.topic_id().as_bytes(),
        vec![dead],
        Default::default(),
    );
    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b.join(stale_ticket).await.unwrap();

    // Prove the premise: no neighbor, nothing learned.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        session_b.offers().is_empty(),
        "join-time catch-up should have found nothing through a dead bootstrap"
    );

    // The daemon's re-join path: pull the peer into the swarm. Both sides
    // see a neighbor…
    session_b
        .join_peers(vec![proto_a.stack().addr().id])
        .await
        .unwrap();

    // …and that alone must be enough for B to learn what A published.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Some(record) = session_b
            .offers()
            .into_iter()
            .find(|o| o.offer.name == "one.txt")
        {
            assert_eq!(record.offer.blob_hash, published.hash);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a new neighbor never brought the history we missed"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    session_b.shutdown_no_announce().await;
    session_a.shutdown_no_announce().await;
}

/// Reordering injection: frames held at the bus and released in reverse
/// order still converge to the same inventory — independent offers carry
/// no ordering assumptions.
#[tokio::test]
async fn reordered_frames_converge() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    // A's next two broadcasts are queued at the carrier, then delivered
    // second-first.
    bus.hold(session_a.topic_id(), session_a.self_id());
    let first = session_a
        .publish_bytes("first.txt".into(), Bytes::from_static(b"one"))
        .await
        .unwrap();
    let second = session_a
        .publish_bytes("second.txt".into(), Bytes::from_static(b"two"))
        .await
        .unwrap();
    bus.release_reversed(session_a.topic_id(), session_a.self_id());

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let offers = session_b.offers();
        let have_first = offers.iter().any(|o| o.offer.blob_hash == first.hash);
        let have_second = offers.iter().any(|o| o.offer.blob_hash == second.hash);
        if have_first && have_second {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "reordered frames never converged (first: {have_first}, second: {have_second})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
