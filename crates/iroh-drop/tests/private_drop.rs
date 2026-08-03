//! WS4's headline proof: private drops are sealed end to end.
//!
//! A private drop seals every frame under the ticket's drop key (wire
//! family 3). These tests stand up a private drop on the in-memory carrier
//! and prove:
//!
//! - a joiner **with** the key participates fully, fetch included;
//! - an **outsider** — a public-mode ticket on the sealed topic, which is
//!   everything a LAN sniffer or a ticket-less guesser could hold — learns
//!   nothing: no offers, no events, no sync pages, and cannot inject
//!   plaintext into the drop either;
//! - a **blind relay** — the sealed ticket with the key removed — retains
//!   and forwards ciphertext without ever reading it, and cannot publish;
//! - hostile bytes (garbage, wrong-key frames) are rejected without
//!   disturbing the session.

mod common;

use std::time::Duration;

use bytes::Bytes;
use iroh_drop::{CreateOptions, DropError, DropEvent, DropPolicy, DropTicket, FetchOutput};

use common::mem_transport::MemBus;

const OFFER_NAME: &str = "salary-bands-2025.xlsx";
const OFFER_BYTES: &[u8] = b"employee,band\nada,staff\n";

/// Full happy path: private drop, two key holders, publish + fetch.
#[tokio::test]
async fn private_drop_publish_and_fetch() {
    let bus = MemBus::new();
    let proto_a = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a
        .create(CreateOptions {
            private: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let ticket = session_a.ticket();
    assert!(ticket.drop_key().is_some(), "private tickets carry the key");

    let published = session_a
        .publish_bytes(OFFER_NAME.into(), Bytes::from_static(OFFER_BYTES))
        .await
        .unwrap();

    let proto_b = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b.join(ticket.clone()).await.unwrap();

    common::wait_until(
        || async {
            session_b
                .offers()
                .iter()
                .any(|o| o.offer.blob_hash == published.hash)
        },
        Duration::from_secs(2),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let result = session_b
        .fetch(
            published.hash,
            FetchOutput::Directory(dir.path().to_path_buf()),
        )
        .await
        .unwrap();
    let bytes = std::fs::read(result.path.unwrap()).unwrap();
    assert_eq!(bytes, OFFER_BYTES);
    assert_eq!(result.provider, Some(session_a.self_id()));
}

/// The done-when scenario: an actor holding everything *but* the key.
#[tokio::test]
async fn keyless_actor_learns_nothing() {
    let bus = MemBus::new();
    let proto_a = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a
        .create(CreateOptions {
            private: true,
            ..Default::default()
        })
        .await
        .unwrap();

    let published = session_a
        .publish_bytes(OFFER_NAME.into(), Bytes::from_static(OFFER_BYTES))
        .await
        .unwrap();

    // The outsider ticket: same topic, same bootstrap — public mode, no
    // key. This is what a LAN observer who learned the topic could
    // assemble, and what a confused member might paste together.
    let stripped = DropTicket::new(
        *session_a.topic_id().as_bytes(),
        session_a.ticket().bootstrap_nodes(),
        Default::default(),
    );
    assert!(stripped.drop_key().is_none());

    let proto_c = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_c = proto_c.join(stripped).await.unwrap();
    let mut events_c = session_c.subscribe();

    // Plenty of time for the live frame, the catch-up sync (refused), and
    // any follow-up traffic to arrive and be processed.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        session_c.offers().is_empty(),
        "key-less joiner must see no offers"
    );
    assert!(
        session_c.providers(&published.hash).is_empty(),
        "key-less joiner must see no provider state"
    );
    while let Ok(event) = events_c.try_recv() {
        if let DropEvent::OfferReceived { .. } = event {
            panic!("key-less joiner received an offer event");
        }
    }

    // Downgrade injection: the key-less actor publishes *plaintext* into
    // the same topic. The private session must reject it (family 2 in a
    // family-3 session) and stay undisturbed.
    let published_c = session_c
        .publish_bytes(
            "not-actually-secret.txt".into(),
            Bytes::from_static(b"garbage"),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !session_a
            .offers()
            .iter()
            .any(|o| o.offer.blob_hash == published_c.hash),
        "plaintext injected by a key-less actor must not land in the private drop"
    );
    assert!(
        session_a
            .offers()
            .iter()
            .any(|o| o.offer.blob_hash == published.hash),
        "the private session is undisturbed"
    );
}

/// A blind relay (sealed ticket, key removed) retains and relays what it
/// cannot read — and has nothing to say itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blind_relay_relays_without_reading() {
    let bus = MemBus::new();
    let proto_a = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a
        .create(CreateOptions {
            private: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let proto_b = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_b = proto_b.join(session_a.ticket().clone()).await.unwrap();
    common::wait_until(
        || async { session_b.peers().contains(&proto_a.stack().endpoint.id()) },
        Duration::from_secs(5),
    )
    .await;

    let relay_ticket = session_a.ticket().without_key();
    assert_eq!(relay_ticket.mode(), iroh_drop::ticket::DropMode::Sealed);
    assert!(relay_ticket.drop_key().is_none());

    let proto_r = common::mem_protocol(&bus, DropPolicy::default()).await;
    let relay = proto_r.join(relay_ticket).await.unwrap();
    let mut relay_events = relay.subscribe();
    common::wait_until(
        || async { relay.peers().len() >= 2 },
        Duration::from_secs(5),
    )
    .await;

    session_a
        .publish_bytes(OFFER_NAME.into(), Bytes::from_static(OFFER_BYTES))
        .await
        .unwrap();

    // The relay verifies and retains the sealed frame (that is what makes
    // it a relay at all) but never decodes a body, so it learns nothing:
    // no offers, no events.
    common::wait_until(
        || async { !relay.export_history().is_empty() },
        Duration::from_secs(5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(relay.offers().is_empty(), "blind relay sees no offers");
    while let Ok(ev) = relay_events.try_recv() {
        assert!(
            !matches!(ev, DropEvent::OfferReceived { .. }),
            "blind relay must never surface an offer"
        );
    }

    // And it has nothing to say: publishing is refused.
    let err = relay
        .publish_bytes("nope".into(), Bytes::from_static(b"x"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            DropError::Protocol(iroh_drop::ProtocolError::NoDropKey)
        ),
        "blind relay publish must fail with NoDropKey, got {err:?}"
    );
}

/// Hostile bytes against a sealed session: garbage and wrong-key frames
/// are rejected; the session keeps working.
#[tokio::test]
async fn sealed_session_shrugs_off_hostile_bytes() {
    let bus = MemBus::new();
    let proto_a = common::mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a
        .create(CreateOptions {
            private: true,
            ..Default::default()
        })
        .await
        .unwrap();

    // Raw garbage.
    session_a
        .inject_raw_message(Bytes::from_static(b"\xff\xfe\xfd not a frame"))
        .await
        .ok();
    // A syntactically valid family-3 frame sealed under the WRONG key.
    let wrong = iroh_drop::DropKey::generate();
    let forgery =
        iroh_drop::MessageV1::new(iroh_drop::MessageBodyV1::Request(iroh_drop::RequestV1 {
            blob_hash: iroh_drop::BlobHash::from_bytes([9u8; 32]),
        }))
        .encode_sealed(&iroh::SecretKey::generate(), &wrong, &session_a.topic_id())
        .unwrap();
    session_a
        .inject_raw_message(Bytes::from(forgery))
        .await
        .ok();

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(session_a.offers().is_empty());

    // Undisturbed: normal operation continues.
    let published = session_a
        .publish_bytes(OFFER_NAME.into(), Bytes::from_static(OFFER_BYTES))
        .await
        .unwrap();
    assert!(session_a
        .offers()
        .iter()
        .any(|o| o.offer.blob_hash == published.hash));
}
