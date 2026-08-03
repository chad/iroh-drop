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
    // C must try A, fail, and fall back to B. Runs on the in-memory
    // carrier: gossip delivery is instant, the blob transfers are real.
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let proto_c = mem_protocol(&bus, DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything announces, so no event can race past us.
    let mut events_c = session_c.subscribe();

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
    .encode(proto_a.stack().endpoint.secret_key(), &session_a.topic_id())
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
    assert_eq!(result.provider, Some(session_b.self_id()));

    // The bytes C ended up with are recorded complete.
    let offers = session_c.offers();
    assert_eq!(offers[0].local_status, LocalBlobStatus::Complete);
}

#[tokio::test]
async fn provider_announcement_lost_falls_back_to_offer_author() {
    // Deterministic carrier loss, injected at the bus: B's provider
    // announcement never reaches anyone. The newest-provider ordering
    // cannot help C, so the fetch must fall back to the offer's author.
    //
    // The topology makes the proof strict: A and B each neighbor C's
    // bootstrap only through the shared drop — if B's muted announcement
    // leaked through *any* path (relay, sync replay from A's log), C would
    // prefer B as the newest provider.
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let topic = session_a.topic_id();

    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    let published = session_a
        .publish_bytes(
            "pinned.bin".into(),
            Bytes::from_static(b"authoritative copy"),
        )
        .await
        .unwrap();

    // The carrier drops everything B says from here on: both the automatic
    // provider announcement when its fetch completes and the explicit
    // re-announcement. Blob transfer is not gossip, so the fetch itself
    // still works — B really does become a second provider nobody hears
    // about.
    bus.mute(topic, session_b.self_id());
    session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    session_b.reannounce(&published.hash).await.unwrap();
    bus.unmute(topic, session_b.self_id());

    // C joins after all of this. Its catch-up sync pulls A's retained
    // history, which cannot contain the dropped announcements either.
    let proto_c = mem_protocol(&bus, DropPolicy::default()).await;
    let session_c = proto_c
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();

    // C must fetch from A — the only provider it can know about.
    let result = session_c
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(
        result.provider,
        Some(session_a.self_id()),
        "the lost announcement must not influence provider choice"
    );
}
