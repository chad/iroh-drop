//! Shared helpers for the multi-peer integration tests.
//!
//! All stacks run in offline mode (no relays, no n0 address lookup); tickets
//! are constructed in-process with full bootstrap addresses. This exercises
//! the exact same session logic the CLI uses online.
#![allow(dead_code)]

use std::time::Duration;

use iroh::EndpointAddr;
use iroh_drop::{DropBuilder, DropPolicy, DropProtocol, DropSession, DropTicket, StackOptions};
use tokio::sync::broadcast;

pub use iroh_drop::{BlobHash, DropEvent};

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

pub fn is_fetch_completed(hash: BlobHash) -> impl Fn(&DropEvent) -> bool {
    move |ev| matches!(ev, DropEvent::FetchCompleted { hash: h, .. } if *h == hash)
}
