//! The app's data path, without a window.
//!
//! `egui::Context` works headlessly, so the real worker thread, the real daemon
//! connection and the real consent queue can all be driven from a test. What is
//! *not* covered is painting — deliberately, because that is the part where a
//! bug is visible to the naked eye and everything else is not.

use std::sync::Arc;
use std::time::{Duration, Instant};

use iroh_drop_daemon::{connect, ControlListener, Hello, Service, ServiceOptions};
use iroh_drop_gui::bridge::{self, Cmd};
use serde_json::json;

const TIMEOUT: Duration = Duration::from_secs(60);

fn options(dir: &std::path::Path, name: &str) -> ServiceOptions {
    let download_dir = dir.join(format!("{name}-downloads"));
    std::fs::create_dir_all(&download_dir).expect("download dir");
    ServiceOptions {
        store_path: Some(dir.join(format!("{name}-store"))),
        identity_path: Some(dir.join(format!("{name}-identity"))),
        offline: true,
        mdns: false,
        download_dir,
        auto_accept: false,
        link_base: None,
    }
}

/// Bring up a daemon on a socket.
async fn serve(dir: &std::path::Path, name: &str, sock: &std::path::Path) -> Arc<Service> {
    let service = Service::new(options(dir, name)).await.expect("service");
    let listener = ControlListener::bind(Arc::clone(&service), sock)
        .await
        .expect("bind");
    tokio::spawn(listener.serve());
    service
}

/// A daemon hosting one published file, plus a ticket for it. Published before
/// anyone joins, so receivers exercise catch-up sync.
async fn publish_one(
    dir: &std::path::Path,
    name: &str,
    payload: &std::path::Path,
) -> (Arc<Service>, String) {
    let service = Service::new(options(dir, name)).await.expect("service");
    let client =
        iroh_drop_daemon::Client::connect_memory(&service, Hello::observer("sender"), None)
            .await
            .expect("client");
    let drop = client
        .call("drop.create", json!({"name": name}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket = client
        .call("drop.ticket", json!({"drop": drop, "full": true}))
        .await
        .expect("ticket")["ticket"]
        .as_str()
        .expect("ticket string")
        .to_string();
    client
        .call(
            "offer.publish",
            json!({"drop": drop, "path": payload.to_str().expect("utf8")}),
        )
        .await
        .expect("publish");
    std::mem::forget(client);
    (service, ticket)
}

/// Poll shared state until a predicate holds. The worker runs on its own
/// thread, so this is how a test observes it.
fn wait_until<T>(
    bridge: &bridge::Bridge,
    what: &str,
    mut probe: impl FnMut(&bridge::UiState) -> Option<T>,
) -> T {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        {
            let state = bridge.state.lock().expect("state");
            if let Some(value) = probe(&state) {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}; last error: {:?}",
                state.error
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Pasting a link is itself consent: the files arrive with no extra clicks, and
/// exactly once.
///
/// This also pins the race that matters. The consent question for an offer can
/// be broadcast before `drop.join` has even returned a handle, so the worker
/// must buffer it and still recognise the drop as solicited.
#[test]
fn pasting_a_link_downloads_without_extra_clicks() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("holiday.txt");
    std::fs::write(&payload, b"a picture of a dog\n").expect("write payload");

    let sender_runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (sender, ticket) = sender_runtime.block_on(publish_one(tmp.path(), "sender", &payload));

    let recv_runtime = tokio::runtime::Runtime::new().expect("runtime");
    let sock = tmp.path().join("recv-run").join("control.sock");
    let receiver = recv_runtime.block_on(serve(tmp.path(), "recv", &sock));

    // A headless egui context is enough for the worker to request repaints.
    let bridge = bridge::spawn(egui::Context::default(), Some(sock.clone()), true);
    wait_until(&bridge, "the app to connect", |s| s.connected.then_some(()));

    bridge.send(Cmd::Receive(format!("here you go: {ticket}")));

    let transfer = wait_until(&bridge, "the transfer to finish", |s| {
        s.transfers.iter().find(|t| t.finished).cloned()
    });
    assert_eq!(transfer.failed, None, "transfer should have succeeded");
    assert!(!transfer.saved_to.is_empty(), "we should know where it went");
    assert_eq!(
        std::fs::read(&transfer.saved_to[0]).expect("read saved file"),
        b"a picture of a dog\n"
    );

    // No prompt for something the user just asked for.
    {
        let state = bridge.state.lock().expect("state");
        assert!(
            state.incoming.is_empty(),
            "asking for a drop is consent; a prompt here trains people to click yes"
        );
    }

    // Exactly one copy. A second fetch would land as `holiday-<hash>.txt`.
    let downloads = tmp.path().join("recv-downloads");
    let names: Vec<String> = std::fs::read_dir(&downloads)
        .expect("read downloads")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(names, vec!["holiday.txt".to_string()], "downloaded twice");

    recv_runtime.block_on(Arc::clone(&receiver).shutdown());
    sender_runtime.block_on(Arc::clone(&sender).shutdown());
}

/// An *unsolicited* offer prompts, and declining writes nothing.
///
/// The receiver joins through a separate client, so the app never marks the drop
/// as something the user asked for — which is what a stranger pushing a file at
/// you actually looks like.
#[test]
fn declining_an_unsolicited_offer_writes_nothing() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("unwanted.txt");
    std::fs::write(&payload, b"spam\n").expect("write payload");

    let sender_runtime = tokio::runtime::Runtime::new().expect("runtime");
    let (sender, ticket) = sender_runtime.block_on(publish_one(tmp.path(), "sender2", &payload));

    let recv_runtime = tokio::runtime::Runtime::new().expect("runtime");
    let sock = tmp.path().join("recv2-run").join("control.sock");
    let receiver = recv_runtime.block_on(serve(tmp.path(), "recv2", &sock));

    // The app is the UI, and answers questions.
    let bridge = bridge::spawn(egui::Context::default(), Some(sock.clone()), true);
    wait_until(&bridge, "the app to connect", |s| s.connected.then_some(()));

    // Something else joins the drop: not the user, not through the app.
    recv_runtime.block_on(async {
        let other = connect(&sock, Hello::observer("someone-else"), None)
            .await
            .expect("second client");
        other
            .call("drop.join", json!({"ticket": ticket}))
            .await
            .expect("join");
        // Hold the connection open long enough for the offer to arrive.
        tokio::time::sleep(Duration::from_secs(3)).await;
        std::mem::forget(other);
    });

    let incoming = wait_until(&bridge, "a consent prompt", |s| s.incoming.first().cloned());
    assert_eq!(incoming.name, "unwanted.txt");

    let downloads = tmp.path().join("recv2-downloads");
    assert_eq!(
        std::fs::read_dir(&downloads).expect("read downloads").count(),
        0,
        "nothing may be written before consent"
    );

    bridge.send(Cmd::Answer {
        id: incoming.id,
        accept: false,
    });
    wait_until(&bridge, "the prompt to clear", |s| {
        s.incoming.is_empty().then_some(())
    });

    // Give a wrongly-started fetch time to betray itself.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        std::fs::read_dir(&downloads).expect("read downloads").count(),
        0,
        "declining must write nothing"
    );

    recv_runtime.block_on(Arc::clone(&receiver).shutdown());
    sender_runtime.block_on(Arc::clone(&sender).shutdown());
}

/// With no daemon to connect to, the app hosts in-process rather than failing.
#[test]
fn falls_back_to_hosting_in_process() {
    let tmp = tempfile::tempdir().expect("tmp");
    let missing = tmp.path().join("nothing-here").join("control.sock");

    let bridge = bridge::spawn(egui::Context::default(), Some(missing), true);
    wait_until(&bridge, "the embedded service to come up", |s| {
        s.connected.then_some(())
    });
    let state = bridge.state.lock().expect("state");
    assert!(state.error.is_none(), "fallback must not surface an error");
    assert!(
        state.log.iter().any(|line| line.contains("hosting")),
        "the user must be told the window is doing the hosting: {:?}",
        state.log
    );
}
