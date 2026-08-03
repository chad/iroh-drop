//! What a peer can and cannot make us do.
//!
//! Each test here is an attack or a misbehaviour, and the assertion is that
//! the session survives it with bounded resources and keeps working
//! afterwards — surviving is the whole point.

mod common;

use bytes::Bytes;
use common::{mem_protocol, ticket_for, wait_event, MemBus, TIMEOUT};
use iroh_drop::hash::BlobHash;
use iroh_drop::message::{
    BodyEnvelopeV1, MessageBodyV1, MessageV1, OfferV1, ProviderState, ProviderV1,
};
use iroh_drop::policy::DropPolicy;
use iroh_drop::state::{MAX_ALIASES_PER_OFFER, MAX_OFFERS, MAX_OFFERS_PER_AUTHOR};
use iroh_drop::{DropEvent, ProtocolWarningKind, RejectReason};

/// A peer announcing offers as fast as it can gets throttled, the inventory
/// stays bounded, and the session recovers afterwards.
///
/// Note which wall stops it: per-peer rate limiting bites long before the
/// per-author quota does, which is the point of having both.
#[tokio::test]
async fn offer_floods_are_contained() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything is sent; on the in-memory carrier the
    // neighbor link already exists, so no peer-joined wait is needed.
    let mut events_b = session_b.subscribe();

    // A floods B with distinct offers as fast as gossip will carry them.
    let flood = 300;
    for i in 0..flood {
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&(i as u64).to_le_bytes());
        let offer = OfferV1 {
            blob_hash: BlobHash::from_bytes(seed),
            name: format!("flood-{i}.bin"),
            size: 1,
            media_type: None,
            created_at_ms: None,
            metadata: Default::default(),
        };
        let frame = MessageV1::new(MessageBodyV1::Offer(offer))
            .encode(
                &proto_a.stack().endpoint.secret_key().clone(),
                &session_a.topic_id(),
            )
            .unwrap();
        session_a.inject_raw_message(Bytes::from(frame)).await.ok();
    }

    // B pushes back rather than absorbing everything.
    wait_event(&mut events_b, "flood pushback", |ev| {
        matches!(
            ev,
            DropEvent::ProtocolWarning {
                warning: ProtocolWarningKind::RateLimited,
                ..
            }
        ) || matches!(
            ev,
            DropEvent::OfferRejected {
                reason: RejectReason::QuotaExceeded,
                ..
            }
        )
    })
    .await;

    let offers = session_b.offers().len();
    assert!(
        offers < flood,
        "B absorbed all {flood} flooded offers ({offers} recorded): no pushback happened"
    );
    // The tighter of the two bounds applies here, since one author sent
    // everything.
    assert!(
        offers <= MAX_OFFERS_PER_AUTHOR.min(MAX_OFFERS),
        "inventory grew past its bounds ({offers} offers)"
    );

    // The session is throttled, not broken: once the bucket refills, a real
    // offer gets through.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let published = session_a
        .publish_bytes(
            "after-the-flood.txt".into(),
            Bytes::from_static(b"still here"),
        )
        .await
        .unwrap();
    wait_event(&mut events_b, "offer after the flood", |ev| {
        matches!(ev, DropEvent::OfferReceived { offer, .. } if offer.blob_hash == published.hash)
    })
    .await;

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// A peer sending a kind we do not implement is ignored, not fatal — and the
/// session keeps processing real messages afterwards.
#[tokio::test]
async fn unknown_kinds_are_ignored_not_fatal() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything is sent; on the in-memory carrier the
    // neighbor link already exists, so no peer-joined wait is needed.
    let mut events_b = session_b.subscribe();

    // A kind from the future, correctly signed.
    let frame = MessageV1::with_envelope(BodyEnvelopeV1 {
        kind: 1234,
        payload: vec![1, 2, 3, 4],
    })
    .encode(
        &proto_a.stack().endpoint.secret_key().clone(),
        &session_a.topic_id(),
    )
    .unwrap();
    session_a
        .inject_raw_message(Bytes::from(frame))
        .await
        .unwrap();

    wait_event(&mut events_b, "unknown kind warning", |ev| {
        matches!(
            ev,
            DropEvent::ProtocolWarning {
                warning: ProtocolWarningKind::UnknownKind { kind: 1234 },
                ..
            }
        )
    })
    .await;

    // The session still works: a real offer lands right after.
    let published = session_a
        .publish_bytes("real.txt".into(), Bytes::from_static(b"a real offer"))
        .await
        .unwrap();
    let ev = wait_event(&mut events_b, "offer after unknown kind", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;
    let DropEvent::OfferReceived { offer, .. } = ev else {
        unreachable!()
    };
    assert_eq!(offer.blob_hash, published.hash);

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// A stale `Available`, replayed after a withdrawal, must not resurrect a
/// provider that has gone away.
#[tokio::test]
async fn stale_provider_replays_cannot_resurrect() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let proto_b = mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything is sent; on the in-memory carrier the
    // neighbor link already exists, so no peer-joined wait is needed.
    let mut events_b = session_b.subscribe();

    let published = session_a
        .publish_bytes("leaving.txt".into(), Bytes::from_static(b"bye"))
        .await
        .unwrap();
    wait_event(&mut events_b, "offer", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;

    let secret = proto_a.stack().endpoint.secret_key().clone();
    let hash = published.hash;
    // Timestamps are epoch milliseconds, and A's own publish already stamped
    // an announcement with "now", so these must be in the same era.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // A withdraws, one second in the future.
    let withdraw = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: hash,
        state: ProviderState::Withdrawing,
        announced_at_ms: Some(now + 1_000),
    }))
    .encode(&secret, &session_a.topic_id())
    .unwrap();
    session_a
        .inject_raw_message(Bytes::from(withdraw))
        .await
        .unwrap();
    wait_event(&mut events_b, "withdrawal", |ev| {
        matches!(ev, DropEvent::ProviderUnavailable { .. })
    })
    .await;
    assert!(
        !session_b.providers(&hash).contains(&session_a.self_id()),
        "A should be gone after withdrawing"
    );

    // Someone replays A's older availability claim, as a lagging catch-up log
    // would.
    let stale = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: hash,
        state: ProviderState::Available,
        announced_at_ms: Some(now),
    }))
    .encode(&secret, &session_a.topic_id())
    .unwrap();
    session_a
        .inject_raw_message(Bytes::from(stale))
        .await
        .unwrap();

    // Give it time to be (not) applied, then confirm nothing came back.
    tokio::time::sleep(std::time::Duration::from_millis(750)).await;
    assert!(
        !session_b.providers(&hash).contains(&session_a.self_id()),
        "a stale replay must not resurrect a withdrawn provider"
    );

    // A newer claim is honoured, so this is ordering, not deafness.
    let fresh = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: hash,
        state: ProviderState::Available,
        announced_at_ms: Some(now + 2_000),
    }))
    .encode(&secret, &session_a.topic_id())
    .unwrap();
    session_a
        .inject_raw_message(Bytes::from(fresh))
        .await
        .unwrap();
    wait_event(&mut events_b, "return", |ev| {
        matches!(ev, DropEvent::ProviderAvailable { .. })
    })
    .await;
    assert!(session_b.providers(&hash).contains(&session_a.self_id()));

    let _ = (TIMEOUT, MAX_ALIASES_PER_OFFER);
    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}
