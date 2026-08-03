//! The relay proof: an extension frame crosses, and is retained by, peers
//! that do not implement the extension.
//!
//! Topology, forced by offline mode with every ticket naming only B:
//!
//! ```text
//! A (presence)   C (presence)   D (late joiner, presence)
//!  \             |             /
//!   B (stock peer — knows nothing about the presence namespace)
//! ```
//!
//! Proven, in order:
//! 1. A's beacon reaches C **through B** — B relays what it cannot interpret.
//! 2. B stays healthy while carrying traffic it has no subscriber for
//!    (it can still publish offers that A receives). Unawareness is silent
//!    by design: the extension envelope is a known core kind, so a peer
//!    that serves no namespace has nothing to warn about.
//! 3. D, joining later, pulls the beacon from **B's retained history** via
//!    catch-up sync — B serves an extension it does not understand to a
//!    peer that does.
//!
//! Determinism note: gossip neighbor-ups can fire between `join()`
//! returning and `subscribe()`, so neighbor checks poll `peers()` and all
//! event receivers are created before the action they observe.

use std::time::Duration;

use iroh::EndpointId;
use iroh_drop::{
    DropBuilder, DropEvent, DropPolicy, DropProtocol, DropSession, ExtensionFrame, StackOptions,
};
use iroh_drop_presence as presence;
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(30);

async fn protocol() -> DropProtocol {
    DropBuilder::from_options(StackOptions {
        store_path: None,
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .policy(DropPolicy::default())
    .build()
    .await
    .expect("build protocol")
}

/// Poll until `session` knows `peer` (state-based; immune to event races).
async fn wait_knows(session: &DropSession, peer: EndpointId, what: &str) {
    tokio::time::timeout(TIMEOUT, async {
        while !session.peers().contains(&peer) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Wait for a matching event on an already-subscribed receiver.
async fn wait_on(
    rx: &mut broadcast::Receiver<DropEvent>,
    what: &str,
    pred: impl Fn(&DropEvent) -> bool,
) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let event = rx.recv().await.expect("event stream ended");
            if pred(&event) {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// The next decodable presence beacon on `rx`.
async fn next_presence(rx: &mut broadcast::Receiver<ExtensionFrame>) -> presence::Presence {
    loop {
        let frame = rx.recv().await.expect("extension stream ended");
        if let Some(p) = presence::decode(&frame) {
            return p;
        }
    }
}

#[tokio::test]
async fn extension_frames_relay_through_and_are_retained_by_stock_peers() {
    // B is the stock peer. It creates the drop; everyone else bootstraps
    // from B's ticket, so all paths between A, C, and D cross B.
    let proto_b = protocol().await;
    let session_b = proto_b
        .create(Default::default())
        .await
        .expect("create drop");
    let id_b = session_b.self_id();
    let ticket = session_b.ticket();
    // Subscribed before anything joins, so B's receiver misses nothing.
    let mut b_events = session_b.subscribe();

    let proto_a = protocol().await;
    let session_a = proto_a.join(ticket.clone()).await.expect("A joins");
    let id_a = session_a.self_id();
    let mut a_events = session_a.subscribe();

    let proto_c = protocol().await;
    let session_c = proto_c.join(ticket.clone()).await.expect("C joins");
    let id_c = session_c.self_id();

    // The mesh settles: A and C are each neighbors with B only.
    wait_knows(&session_a, id_b, "A to know B").await;
    wait_knows(&session_c, id_b, "C to know B").await;
    // B must know both before it can relay between them.
    wait_knows(&session_b, id_a, "B to know A").await;
    wait_knows(&session_b, id_c, "B to know C").await;

    // (1) C subscribes, then A beacons. The only path to C is A -> B -> C.
    let mut c_frames = presence::subscribe(&session_c);
    presence::announce(&session_a, "hello from A")
        .await
        .expect("announce");
    let got = tokio::time::timeout(TIMEOUT, next_presence(&mut c_frames))
        .await
        .expect("timed out waiting for C to receive the beacon");
    assert_eq!(got.author, id_a);
    assert_eq!(got.status, "hello from A");

    // (2) B had nothing to say about traffic it has no subscriber for, and
    // stayed healthy: it can still publish offers that A receives.
    let _ = &mut b_events;
    session_b
        .publish_bytes(
            "b.txt".to_string(),
            bytes::Bytes::from_static(b"stock peer still works"),
        )
        .await
        .expect("B publishes");
    wait_on(&mut a_events, "A to receive B's offer", |e| {
        matches!(e, DropEvent::OfferReceived { offer, from } if offer.name == "b.txt" && *from == id_b)
    })
    .await;

    // (3) D joins late. Gossip has no history, so the beacon can only reach
    // D one way: catch-up sync pulls B's retained log, which includes the
    // frame B could not decode. Each fresh subscription replays D's own
    // retained history, so once sync lands the frame, this loop finds it.
    let proto_d = protocol().await;
    let session_d = proto_d.join(ticket).await.expect("D joins");
    let replayed = tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut d_frames = presence::subscribe(&session_d);
            match tokio::time::timeout(Duration::from_millis(500), next_presence(&mut d_frames))
                .await
            {
                Ok(p) => return p,
                Err(_) => continue, // sync has not delivered it yet; replay again
            }
        }
    })
    .await
    .expect("timed out waiting for D to replay the beacon from B's history");
    assert_eq!(replayed.author, id_a);
    assert_eq!(replayed.status, "hello from A");

    // Over-sized extension payloads are rejected before they hit the wire.
    let err = session_a
        .send_extension(
            presence::PRESENCE_NAMESPACE,
            presence::PRESENCE_KIND_STATUS,
            presence::PRESENCE_SCHEMA_VERSION,
            bytes::Bytes::from(vec![0u8; iroh_drop::MAX_MESSAGE_SIZE]),
        )
        .await
        .expect_err("over-sized payloads are rejected");
    assert!(
        matches!(
            err,
            iroh_drop::DropError::Protocol(iroh_drop::ProtocolError::MessageTooLarge(_))
        ),
        "unexpected error: {err}"
    );
}
