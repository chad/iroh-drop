//! Anti-entropy: a neighbor that appears *after* join-time catch-up has
//! failed (or was never possible) still gets synced from. This is what makes
//! "stay in the group and see whatever is offered" survive both sides
//! restarting at different times: membership alone reconnects the swarm,
//! and the first neighbor-up pulls the missing history across.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{protocol, TIMEOUT};
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
    let proto_a = protocol(DropPolicy::default()).await;
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
    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b.join(stale_ticket).await.unwrap();

    // Prove the premise: no neighbor, nothing learned.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        session_b.offers().is_empty(),
        "join-time catch-up should have found nothing through a dead bootstrap"
    );

    // The daemon's re-join path: seed discovery, then pull the peer into
    // the swarm. Gossip connects, both sides see a neighbor…
    proto_b.stack().add_known_addr(proto_a.stack().addr());
    session_b
        .join_peers(vec![proto_a.stack().addr().id])
        .await
        .unwrap();

    // …and that alone must be enough for B to learn what A published.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        if let Some(record) = session_b.offers().into_iter().find(|o| o.offer.name == "one.txt") {
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
