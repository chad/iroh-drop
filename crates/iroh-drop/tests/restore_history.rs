//! The scenario that defines "a drop outlives its publisher": the last
//! surviving replica restarts, and a brand-new peer must still discover the
//! drop's contents by name and fetch them. Bytes alone are not enough — the
//! offer inventory and provider assertions have to survive too.

mod common;

use bytes::Bytes;
use common::*;
use iroh_drop::{DropBuilder, DropPolicy, DropTicket, FetchOutput, StackOptions};

#[tokio::test]
async fn restored_history_reconstructs_a_drop_after_cold_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store_b = dir.path().join("b-store");
    let identity_b = dir.path().join("b-identity");

    // A publishes; B joins and replicates.
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let payload = b"the report must survive us all".to_vec();
    let published = session_a
        .publish_bytes("report.pdf".into(), Bytes::from(payload.clone()))
        .await
        .unwrap();

    let proto_b = DropBuilder::from_options(StackOptions {
        store_path: Some(store_b.clone()),
        identity_path: Some(identity_b.clone()),
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let fetched = session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(fetched.size, payload.len() as u64);

    // B saves its history and disappears, along with A. Cold restart: no live
    // peer anywhere, only B's disk.
    let history = session_b.export_history();
    assert!(!history.is_empty(), "a replica has history worth keeping");
    let topic = session_b.topic_id();
    eprintln!(
        "[t] exported {} frames; shutting down both lifetimes",
        history.len()
    );
    session_b.shutdown_no_announce().await;
    // Drop the session: its Arc keeps the old store open, and the rebuild
    // below would wait on that store's lock forever.
    drop(session_b);
    proto_b.shutdown().await.unwrap();
    drop(session_a);
    proto_a.shutdown().await.unwrap();
    eprintln!("[t] cold state: no live peers, only B's disk");

    // B comes back: same store, same identity, empty memory. It rejoins and
    // restores what it knew.
    let proto_b2 = DropBuilder::from_options(StackOptions {
        store_path: Some(store_b.clone()),
        identity_path: Some(identity_b.clone()),
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    // Rejoin needs a ticket for the topic; no bootstrap peer is reachable and
    // that is fine — restore does not depend on the network.
    let lonely_ticket = DropTicket::new(*topic.as_bytes(), vec![], Default::default());
    eprintln!("[t] rebuilding B from disk");
    let session_b2 = proto_b2.join(lonely_ticket).await.unwrap();
    let applied = session_b2.restore_history(history).await;
    eprintln!("[t] restored {applied} frames");
    assert!(applied > 0, "history replayed into the fresh session");

    // The drop is back: the offer is known *by name*, and B serves again.
    let offers = session_b2.offers();
    assert_eq!(offers.len(), 1);
    assert!(
        offers[0].aliases.contains("report.pdf"),
        "the name survived, not just the hash: {:?}",
        offers[0].aliases
    );
    session_b2.reannounce(&published.hash).await.unwrap();

    // C is brand new and joins from the restarted B. It must learn the full
    // inventory from B's restored history and fetch the bytes from B's store.
    let proto_c = protocol(DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_b2, proto_b2.stack().addr()))
        .await
        .unwrap();
    let mut learned = false;
    for _ in 0..100 {
        if session_c
            .offers()
            .iter()
            .any(|o| o.aliases.contains("report.pdf"))
        {
            learned = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(learned, "C learned the inventory from restarted B");

    let got = session_c
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(got.size, payload.len() as u64);
    let bytes = session_c.read_bytes(published.hash, 1024).await.unwrap();
    assert_eq!(&bytes[..], &payload[..]);

    session_b2.shutdown_no_announce().await;
    session_c.shutdown_no_announce().await;
    proto_b2.shutdown().await.unwrap();
    proto_c.shutdown().await.unwrap();
}
