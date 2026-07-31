mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::DropPolicy;
use iroh_drop::FetchOutput;
use iroh_drop::LocalBlobStatus;

#[tokio::test]
async fn oversized_offer_recorded_but_not_auto_fetched() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = protocol(DropPolicy {
        auto_fetch: true,
        max_blob_size: 16,
        ..Default::default()
    })
    .await;
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

    let published = session_a
        .publish_bytes("big.bin".into(), Bytes::from(vec![7u8; 1024]))
        .await
        .unwrap();

    // The offer is visible...
    wait_event(&mut events_b, "offer", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;
    // ...but policy-rejected for auto-fetch.
    wait_event(&mut events_b, "policy rejection", |ev| {
        matches!(ev, DropEvent::OfferRejected { reason, .. } if reason.to_string().contains("policy"))
    })
    .await;

    // Recorded as missing; a manual fetch still works.
    assert_eq!(session_b.offers()[0].local_status, LocalBlobStatus::Missing);
    session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(
        session_b.offers()[0].local_status,
        LocalBlobStatus::Complete
    );
}
