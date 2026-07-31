//! The primary iroh-drop demo: four peers, one disappearing publisher.
//!
//! 1. A creates a drop and publishes a file.
//! 2. B and C join and auto-fetch it.
//! 3. A exits abruptly.
//! 4. D joins later and retrieves the file — from a *replica*, proving the
//!    drop survives its original publisher.
//!
//! Runs fully offline (direct connections on localhost) in one process:
//!
//! ```sh
//! cargo run -p iroh-drop --example four_peer_drop
//! ```
//!
//! Set `RUST_LOG=iroh_drop=debug` (or `iroh_gossip=debug`) for protocol logs.

use std::time::Duration;

use bytes::Bytes;
use iroh_drop::{
    DropBuilder, DropEvent, DropPolicy, DropProtocol, DropSession, DropTicket, FetchOutput,
    StackOptions,
};
use tokio::sync::broadcast;

const TIMEOUT: Duration = Duration::from_secs(30);

async fn peer(policy: DropPolicy) -> DropProtocol {
    DropBuilder::from_options(StackOptions {
        store_path: None,
        offline: true,
        ..Default::default()
    })
    .await
    .expect("stack")
    .policy(policy)
    .build()
    .await
    .expect("protocol")
}

/// A fresh ticket for the session's topic, bootstrapping from a live peer.
fn ticket_from(session: &DropSession, bootstrap: &DropProtocol) -> DropTicket {
    DropTicket::new(
        *session.topic_id().as_bytes(),
        vec![bootstrap.stack().addr()],
        Default::default(),
    )
}

async fn wait_for(
    who: &str,
    rx: &mut broadcast::Receiver<DropEvent>,
    what: &str,
    pred: impl Fn(&DropEvent) -> bool,
) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "{who}: timed out waiting for {what}");
        let event = tokio::time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("{who}: timed out waiting for {what}"))
            .expect("event channel closed");
        if pred(&event) {
            return;
        }
    }
}

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    println!("▶ iroh-drop: four peers, one disappearing publisher\n");

    // ── A creates the drop ────────────────────────────────────────────────
    let a = peer(DropPolicy::default()).await;
    let session_a = a.create(Default::default()).await.expect("create");
    println!(
        "A: created drop {} (endpoint {})",
        session_a.topic_id(),
        session_a.self_id().fmt_short()
    );

    // ── B and C join with auto-fetch enabled ──────────────────────────────
    let auto = DropPolicy {
        auto_fetch: true,
        ..Default::default()
    };
    let b = peer(auto.clone()).await;
    let session_b = b.join(ticket_from(&session_a, &a)).await.expect("B join");
    let c = peer(auto).await;
    let session_c = c.join(ticket_from(&session_a, &a)).await.expect("C join");
    let mut events_b = session_b.subscribe();
    let mut events_c = session_c.subscribe();
    println!("B: joined ({})", session_b.self_id().fmt_short());
    println!("C: joined ({})", session_c.self_id().fmt_short());

    // Give the swarm a moment to form before the announcement.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── A publishes a file ────────────────────────────────────────────────
    let content = b"iroh-drop demo: the publisher is optional once the drop has it.\n";
    let published = session_a
        .publish_bytes("demo.txt".into(), Bytes::from_static(content))
        .await
        .expect("publish");
    println!(
        "\nA: published \"demo.txt\" ({} bytes)\n   hash: {}\n",
        published.size, published.hash
    );

    // ── B and C auto-fetch it ─────────────────────────────────────────────
    for (who, rx) in [("B", &mut events_b), ("C", &mut events_c)] {
        wait_for(
            who,
            rx,
            "auto-fetch",
            |ev| matches!(ev, DropEvent::FetchCompleted { hash, .. } if *hash == published.hash),
        )
        .await;
        println!("{who}: auto-fetched and verified demo.txt — now serving it");
    }

    // ── A exits abruptly (no withdrawal, no goodbye) ──────────────────────
    println!("\nA: exiting abruptly (power cut, kill -9, laptop closed)...");
    let a_id = session_a.self_id();
    session_a.shutdown_no_announce().await;
    drop(session_a);
    drop(a);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── D joins later, knowing only the hash, bootstrapping from B ────────
    let d = peer(DropPolicy::default()).await;
    let session_d = d.join(ticket_from(&session_b, &b)).await.expect("D join");
    println!(
        "\nD: joined later ({}), wants {} — A is gone",
        session_d.self_id().fmt_short(),
        published.hash.fmt_short()
    );

    let result = session_d
        .fetch(published.hash, FetchOutput::Store)
        .await
        .expect("D fetch");
    let provider = result
        .provider
        .map(|p| p.fmt_short().to_string())
        .unwrap_or_else(|| "nobody?!".into());
    println!(
        "D: fetched {} bytes, verified against the hash",
        result.size
    );
    println!("D: served by {provider} — a replica, not the original publisher");

    assert_eq!(result.size as usize, content.len());
    assert_ne!(provider, a_id.fmt_short().to_string());

    // Tidy up so the example exits cleanly: stop sessions, drop the
    // handles (unsubscribing the gossip topics), then close the stacks.
    session_d.shutdown_no_announce().await;
    session_b.shutdown_no_announce().await;
    session_c.shutdown_no_announce().await;
    drop(session_d);
    drop(session_b);
    drop(session_c);
    d.shutdown().await.ok();
    b.shutdown().await.ok();
    c.shutdown().await.ok();
    println!("\n✔ the drop outlived its publisher");
}
