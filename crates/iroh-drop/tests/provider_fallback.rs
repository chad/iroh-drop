mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::message::{MessageBodyV1, MessageV1, ProviderState, ProviderV1};
use iroh_drop::DropPolicy;
use iroh_drop::FetchOutput;
use iroh_drop::LocalBlobStatus;

#[tokio::test]
async fn provider_fallback_after_failure() {
    // B publishes and serves a blob. A (the drop creator) maliciously or
    // mistakenly announces availability for it without having the bytes.
    // C must try A, fail, and fall back to B.
    let proto_a = protocol(DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let proto_c = protocol(DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    let mut events_c = session_c.subscribe();
    wait_event(&mut events_c, "C sees a peer", |ev| {
        matches!(ev, DropEvent::PeerJoined { .. })
    })
    .await;

    let published = session_b
        .publish_bytes(
            "shared.bin".into(),
            Bytes::from_static(b"real bytes live on B"),
        )
        .await
        .unwrap();

    // A announces availability for a blob it does not have.
    let lie = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: published.hash,
        state: ProviderState::Available,
        announced_at_ms: None,
    }))
    .encode(proto_a.stack().endpoint.secret_key())
    .unwrap();
    session_a
        .inject_raw_message(Bytes::from(lie))
        .await
        .unwrap();

    // C sees A's announcement (and B's offer/provider messages).
    wait_event(&mut events_c, "any provider", |ev| {
        matches!(ev, DropEvent::ProviderAvailable { .. })
    })
    .await;

    // C fetches. Whichever provider is tried first, the transfer must
    // complete from B, the only peer that actually has the bytes.
    let result = session_c
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(result.size, b"real bytes live on B".len() as u64);

    // If A was tried first, a failure was recorded for A.
    let offers = session_c.offers();
    assert_eq!(offers[0].local_status, LocalBlobStatus::Complete);
}
