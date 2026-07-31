//! Short tickets name peers by id and leave addresses to discovery.

mod common;

use common::{protocol, TIMEOUT};
use iroh_drop::policy::DropPolicy;

#[tokio::test]
async fn short_tickets_drop_addresses_but_keep_peers() {
    let protocol = protocol(DropPolicy::default()).await;
    let session = protocol.create(Default::default()).await.unwrap();

    let full = session.ticket();
    let short = session.short_ticket();

    // Same drop, same peers.
    assert_eq!(short.topic_id(), full.topic_id());
    assert_eq!(short.bootstrap_nodes().len(), full.bootstrap_nodes().len());
    assert_eq!(short.bootstrap_nodes()[0].id, full.bootstrap_nodes()[0].id);

    // The short one carries no socket addresses, and is meaningfully smaller.
    assert!(
        short.bootstrap_nodes().iter().all(|a| a.addrs.is_empty()),
        "a short ticket must not pin addresses"
    );
    assert!(
        !full.bootstrap_nodes()[0].addrs.is_empty(),
        "the full ticket should have the addresses we bound to"
    );
    let (short_len, full_len) = (short.to_string().len(), full.to_string().len());
    assert!(
        short_len < full_len,
        "short ticket ({short_len}) should be shorter than full ({full_len})"
    );

    // And it still round-trips through the string form.
    let parsed: iroh_drop::DropTicket = short.to_string().parse().unwrap();
    assert_eq!(parsed.topic_id(), short.topic_id());

    drop(session);
    protocol.shutdown().await.ok();
}

/// A ticket from a live peer must include that peer, or handing it on cannot
/// work once the original publisher is gone.
#[tokio::test]
async fn a_joiners_ticket_points_at_itself() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(common::ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let handed_on = session_b.ticket();
    assert!(
        handed_on
            .bootstrap_nodes()
            .iter()
            .any(|addr| addr.id == session_b.self_id()),
        "B's ticket must list B"
    );
    // With B first, whoever uses it reaches B even if A never answers.
    assert_eq!(handed_on.bootstrap_nodes()[0].id, session_b.self_id());

    // Sanity: still usable.
    let _: iroh_drop::DropTicket = handed_on.to_string().parse().unwrap();
    let _ = TIMEOUT;

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}
