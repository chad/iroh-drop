//! Shared helpers for the multi-peer integration tests.
//!
//! All stacks run in offline mode (no relays, no n0 address lookup); tickets
//! are constructed in-process with full bootstrap addresses. This exercises
//! the exact same session logic the CLI uses online.
#![allow(dead_code)]

pub mod fixtures;
pub mod mem_transport;

use std::sync::Arc;
use std::time::Duration;

use iroh::EndpointAddr;
use iroh_drop::{DropBuilder, DropPolicy, DropProtocol, DropSession, DropTicket, StackOptions};
use tokio::sync::broadcast;

pub use iroh_drop::{BlobHash, DropEvent};
pub use mem_transport::MemBus;

pub const TIMEOUT: Duration = Duration::from_secs(30);

pub async fn protocol(policy: DropPolicy) -> DropProtocol {
    DropBuilder::from_options(StackOptions {
        store_path: None,
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .policy(policy)
    .build()
    .await
    .expect("build protocol")
}

/// A protocol whose gossip carrier is the shared in-memory `bus`.
///
/// Sessions neighbor exactly the bootstrap peers in the tickets they join
/// with; delivery is instant; loss and reordering are injected through the
/// bus. Blob transfer and catch-up sync still run over the real loopback
/// endpoint, so fetch and anti-entropy logic are fully exercised. Tests
/// that must prove the *real* carrier use [`protocol`] — see
/// `fetch_flow.rs` and `catch_up_sync.rs`, the designated smoke tests.
pub async fn mem_protocol(bus: &Arc<MemBus>, policy: DropPolicy) -> DropProtocol {
    let bus = Arc::clone(bus);
    DropBuilder::from_options(StackOptions {
        store_path: None,
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .policy(policy)
    .transport_factory(Arc::new(move |stack, topic, bootstrap| {
        bus.register(&stack, topic, bootstrap)
    }))
    .build()
    .await
    .expect("build protocol")
}

pub fn ticket_for(session: &DropSession, bootstrap: EndpointAddr) -> DropTicket {
    DropTicket::new(
        *session.topic_id().as_bytes(),
        vec![bootstrap],
        Default::default(),
    )
}

/// Wait for a matching event, printing all events seen for debuggability.
pub async fn wait_event<F>(
    rx: &mut broadcast::Receiver<DropEvent>,
    what: &str,
    pred: F,
) -> DropEvent
where
    F: Fn(&DropEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {what}");
        let ev = tokio::time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .expect("event channel closed");
        eprintln!("[event] {ev:?}");
        if pred(&ev) {
            return ev;
        }
    }
}

/// Poll `check` until it returns true; panic after `within`. For state
/// that is delivered outside the event stream (or that must be observed
/// on the mem carrier, where events can fire before a late subscriber
/// attaches).
pub async fn wait_until<F, Fut>(check: F, within: std::time::Duration)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("condition not met within {within:?}");
}

pub fn is_fetch_completed(hash: BlobHash) -> impl Fn(&DropEvent) -> bool {
    move |ev| matches!(ev, DropEvent::FetchCompleted { hash: h, .. } if *h == hash)
}
