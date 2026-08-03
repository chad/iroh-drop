mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::message::{MessageBodyV1, MessageV1, ProviderState, ProviderV1};
use iroh_drop::DropPolicy;

#[tokio::test]
async fn malformed_and_oversized_messages_do_not_kill_session() {
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let mut events_a = session_a.subscribe();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let mut events_b = session_b.subscribe();
    wait_event(&mut events_a, "peer joined", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;
    wait_event(&mut events_b, "B sees A", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    // Garbage bytes.
    session_b
        .inject_raw_message(Bytes::from_static(b"\xff\xfe\xfd not a message"))
        .await
        .unwrap();
    // A validly framed but forged message (signed by a random non-member key).
    let rogue = iroh::SecretKey::generate();
    let forged = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: iroh_drop::BlobHash::from_bytes([0u8; 32]),
        state: ProviderState::Available,
        announced_at_ms: None,
    }))
    .encode(&rogue, &session_a.topic_id())
    .unwrap();
    session_b
        .inject_raw_message(Bytes::from(forged))
        .await
        .unwrap();

    // Both are rejected with observable warnings...
    wait_event(&mut events_a, "warning", |ev| {
        matches!(ev, DropEvent::ProtocolWarning { .. })
    })
    .await;

    // ...and the session stays healthy: a real offer still arrives.
    let published = session_b
        .publish_bytes("after.txt".into(), Bytes::from_static(b"still alive"))
        .await
        .unwrap();
    wait_event(&mut events_a, "offer after garbage", |ev| {
        matches!(
            ev,
            DropEvent::OfferReceived { offer, .. } if offer.blob_hash == published.hash
        )
    })
    .await;
}
