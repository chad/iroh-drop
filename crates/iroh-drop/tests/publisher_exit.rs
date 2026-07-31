mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::DropPolicy;
use iroh_drop::DropTicket;
use iroh_drop::FetchOutput;
use iroh_drop::LocalBlobStatus;
use std::time::Duration;

#[tokio::test]
async fn publisher_exits_new_peer_fetches_from_replica() {
    // The primary proof of the whole spec: a new peer retrieves a file after
    // the original publisher exits, served by a peer that retained the blob.
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let topic = *session_a.topic_id().as_bytes();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let mut events_b = session_b.subscribe();
    let mut events_a = session_a.subscribe();
    wait_event(&mut events_a, "A sees B", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    let payload = b"important data that must survive the publisher".to_vec();
    let published = session_a
        .publish_bytes("vital.bin".into(), Bytes::from(payload.clone()))
        .await
        .unwrap();

    session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    wait_event(
        &mut events_b,
        "fetch done",
        is_fetch_completed(published.hash),
    )
    .await;

    // Third peer C also joins and fetches before A exits (two replicas).
    let proto_c = protocol(DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    session_c
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The original publisher exits abruptly (no withdrawal announcement).
    let a_id = session_a.self_id();
    session_a.shutdown_no_announce().await;
    drop(session_a);
    drop(proto_a);

    // A brand-new peer D joins, bootstrapping from B only, knowing nothing
    // but the topic and the hash it wants.
    let proto_d = protocol(DropPolicy::default()).await;
    let ticket_d = DropTicket::new(topic, vec![proto_b.stack().addr()], Default::default());
    let session_d = proto_d.join(ticket_d).await.unwrap();

    // D has no offer and no providers: fetch broadcasts a request, B answers
    // with an availability announcement, D downloads from B.
    let result = session_d
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    // D was served by a replica (B or C), never by the departed A.
    let provider = result.provider.expect("served by someone");
    assert_ne!(provider, a_id);

    // D now serves the blob too, and knows of at least one other replica.
    let providers = session_d.providers(&published.hash);
    assert!(providers.contains(&session_d.self_id()));
    assert!(providers.contains(&provider));
    assert_eq!(
        session_d.offers()[0].local_status,
        LocalBlobStatus::Complete
    );
}
