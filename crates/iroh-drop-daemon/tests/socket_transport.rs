//! The same API, over a real Unix socket.
//!
//! The in-memory tests cover the service; these cover the only part they can't:
//! framing, permissions, and the consent round-trip crossing a process
//! boundary in both directions.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use iroh_drop_daemon::{connect, ControlListener, Hello, Service, ServiceOptions};
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

/// A full transfer between two daemons, every frame crossing a socket —
/// including the `Ask`/`Res` consent exchange, which travels daemon → client.
#[tokio::test]
async fn transfer_over_sockets() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("holiday.txt");
    std::fs::write(&payload, b"photos of a dog\n").expect("write payload");

    let a = Service::new(options(tmp.path(), "a")).await.expect("service a");
    let b = Service::new(options(tmp.path(), "b")).await.expect("service b");

    let sock_a = tmp.path().join("a").join("control.sock");
    let sock_b = tmp.path().join("b").join("control.sock");
    let listener_a = ControlListener::bind(Arc::clone(&a), &sock_a)
        .await
        .expect("bind a");
    let listener_b = ControlListener::bind(Arc::clone(&b), &sock_b)
        .await
        .expect("bind b");

    // The directory mode is the real guard; check it is what we promised.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir_mode = std::fs::metadata(sock_a.parent().expect("parent"))
            .expect("dir metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "socket directory must be user-only");
    }

    tokio::spawn(listener_a.serve());
    tokio::spawn(listener_b.serve());

    let client_a = connect(&sock_a, Hello::ui("test-a"), None)
        .await
        .expect("connect a");
    let client_b = connect(
        &sock_b,
        Hello::ui("test-b"),
        Some(iroh_drop_daemon::Client::accept_all()),
    )
    .await
    .expect("connect b");

    assert_eq!(client_a.hello["api"], 1);

    let drop_a = client_a
        .call("drop.create", json!({"name": "holiday"}))
        .await
        .expect("drop.create")["drop"]
        .clone();
    let ticket = client_a
        .call("drop.ticket", json!({"drop": drop_a, "full": true}))
        .await
        .expect("drop.ticket");

    // What a person is given must be a link they can click, not a ticket they
    // have to understand. It also must not contain a placeholder host, which is
    // how this shipped broken the first time.
    let link = ticket["link"].as_str().expect("link");
    assert!(
        link.starts_with("iroh-drop://receive/drop1"),
        "not a usable link: {link}"
    );
    assert!(!link.contains('<'), "unsubstituted placeholder in {link}");
    // No web link unless a base URL was configured; we did not configure one.
    assert!(ticket["web_link"].is_null());

    client_b
        .call("drop.join", json!({"ticket": ticket["ticket"]}))
        .await
        .expect("drop.join");

    tokio::time::sleep(Duration::from_millis(750)).await;

    client_a
        .call(
            "offer.publish",
            json!({"drop": drop_a, "path": payload.to_str().expect("utf8")}),
        )
        .await
        .expect("offer.publish");

    let materialized = client_b
        .wait_for(TIMEOUT, |env| env.e == "fetch.materialized")
        .await
        .expect("b materialized");
    let paths: Vec<std::path::PathBuf> =
        serde_json::from_value(materialized.p["paths"].clone()).expect("paths");
    assert_eq!(
        std::fs::read(&paths[0]).expect("read fetched"),
        b"photos of a dog\n"
    );

    Arc::clone(&a).shutdown().await;
    Arc::clone(&b).shutdown().await;
}

/// Binding twice is refused, so two daemons cannot fight over one identity.
#[tokio::test]
async fn second_daemon_is_refused() {
    let tmp = tempfile::tempdir().expect("tmp");
    let sock = tmp.path().join("run").join("control.sock");

    let first = Service::new(options(tmp.path(), "first"))
        .await
        .expect("service");
    let listener = ControlListener::bind(Arc::clone(&first), &sock)
        .await
        .expect("bind");
    tokio::spawn(listener.serve());

    let second = Service::new(options(tmp.path(), "second"))
        .await
        .expect("service");
    let err = ControlListener::bind(Arc::clone(&second), &sock)
        .await
        .expect_err("second bind should fail");
    assert_eq!(err.code, "already_running");

    Arc::clone(&first).shutdown().await;
    Arc::clone(&second).shutdown().await;
}

/// A socket left behind by a crash is replaced, not treated as fatal.
#[tokio::test]
async fn stale_socket_is_reclaimed() {
    let tmp = tempfile::tempdir().expect("tmp");
    let dir = tmp.path().join("run");
    std::fs::create_dir_all(&dir).expect("dir");
    let sock = dir.join("control.sock");
    // A regular file where the socket should be: connect() fails, so it is
    // stale by definition.
    std::fs::write(&sock, b"leftover").expect("write stale");

    let service = Service::new(options(tmp.path(), "s")).await.expect("service");
    let listener = ControlListener::bind(Arc::clone(&service), &sock)
        .await
        .expect("should reclaim the stale path");
    tokio::spawn(listener.serve());

    let client = connect(&sock, Hello::observer("test"), None)
        .await
        .expect("connect");
    assert_eq!(
        client
            .call("daemon.status", json!({}))
            .await
            .expect("status")["offline"],
        true
    );

    service.shutdown().await;
}

/// A garbage line does not take down a working connection.
#[tokio::test]
async fn garbage_line_is_ignored() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let tmp = tempfile::tempdir().expect("tmp");
    let sock = tmp.path().join("run").join("control.sock");
    let service = Service::new(options(tmp.path(), "s")).await.expect("service");
    let listener = ControlListener::bind(Arc::clone(&service), &sock)
        .await
        .expect("bind");
    tokio::spawn(listener.serve());

    let stream = tokio::net::UnixStream::connect(&sock).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    write_half
        .write_all(b"this is not json\n\n{\"t\":\"req\",\"id\":1,\"m\":\"hello\",\"p\":{\"client\":\"raw\",\"api\":1,\"roles\":[\"observer\"]}}\n")
        .await
        .expect("write");

    let reply = lines
        .next_line()
        .await
        .expect("read")
        .expect("a line after the garbage");
    let frame: serde_json::Value = serde_json::from_str(&reply).expect("json");
    assert_eq!(frame["t"], "res");
    assert_eq!(frame["p"]["api"], 1);

    service.shutdown().await;
}

/// The whole point, through real daemons and real sockets: the sender's daemon
/// dies, a third peer joins afterwards using a *replica's* ticket, and still
/// gets the file — served by the replica, verified against the hash.
#[tokio::test]
async fn drop_outlives_its_publisher() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("scones.txt");
    std::fs::write(&payload, b"Recipe for scones\n").expect("write payload");

    // Chad shares; Mum receives and becomes a provider; Aunt arrives later.
    let chad = Service::new(options(tmp.path(), "chad")).await.expect("chad");
    let mum = Service::new(options(tmp.path(), "mum")).await.expect("mum");

    let sock_chad = tmp.path().join("chad-run").join("control.sock");
    let sock_mum = tmp.path().join("mum-run").join("control.sock");
    tokio::spawn(
        ControlListener::bind(Arc::clone(&chad), &sock_chad)
            .await
            .expect("bind chad")
            .serve(),
    );
    tokio::spawn(
        ControlListener::bind(Arc::clone(&mum), &sock_mum)
            .await
            .expect("bind mum")
            .serve(),
    );

    let c_chad = connect(&sock_chad, Hello::control("chad"), None)
        .await
        .expect("connect chad");
    let c_mum = connect(
        &sock_mum,
        Hello::ui("mum"),
        Some(iroh_drop_daemon::Client::accept_all()),
    )
    .await
    .expect("connect mum");

    let drop_chad = c_chad
        .call("drop.create", json!({"name": "scones"}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket_chad = c_chad
        .call("drop.ticket", json!({"drop": drop_chad, "full": true}))
        .await
        .expect("ticket")["ticket"]
        .clone();
    let drop_mum = c_mum
        .call("drop.join", json!({"ticket": ticket_chad}))
        .await
        .expect("join")["drop"]
        .clone();

    tokio::time::sleep(Duration::from_millis(750)).await;
    c_chad
        .call(
            "offer.publish",
            json!({"drop": drop_chad, "path": payload.to_str().expect("utf8")}),
        )
        .await
        .expect("publish");
    c_mum
        .wait_for(TIMEOUT, |env| env.e == "fetch.materialized")
        .await
        .expect("mum received it");

    // A ticket from a peer that is still running lists that peer first.
    let ticket_mum = c_mum
        .call("drop.ticket", json!({"drop": drop_mum, "full": true}))
        .await
        .expect("mum ticket")["ticket"]
        .clone();

    // Chad's daemon goes away entirely — no withdrawal, no goodbye.
    let chad_id = chad.endpoint_id();
    drop(c_chad);
    Arc::clone(&chad).shutdown().await;
    drop(chad);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Aunt has never spoken to Chad and never will.
    let aunt = Service::new(options(tmp.path(), "aunt")).await.expect("aunt");
    let sock_aunt = tmp.path().join("aunt-run").join("control.sock");
    tokio::spawn(
        ControlListener::bind(Arc::clone(&aunt), &sock_aunt)
            .await
            .expect("bind aunt")
            .serve(),
    );
    let c_aunt = connect(&sock_aunt, Hello::control("aunt"), None)
        .await
        .expect("connect aunt");
    c_aunt
        .call("drop.join", json!({"ticket": ticket_mum}))
        .await
        .expect("aunt join");

    // Catch-up sync means Aunt learns the file *by name*, though it was
    // announced before she arrived and its author is gone.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let items = loop {
        let listed = c_aunt
            .call("offer.list", json!({"drop": "d1"}))
            .await
            .expect("offer.list");
        let items = listed["items"].as_array().cloned().unwrap_or_default();
        if !items.is_empty() {
            break items;
        }
        assert!(std::time::Instant::now() < deadline, "aunt saw no offers");
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(items[0]["name"], "scones.txt");

    c_aunt
        .call("offer.fetch", json!({"drop": "d1", "pick": "1"}))
        .await
        .expect("aunt fetch");
    let completed = c_aunt
        .wait_for(TIMEOUT, |env| env.e == "fetch.completed")
        .await
        .expect("aunt completed the fetch");

    // Served by a replica, not the original publisher.
    let provider = completed.p["provider"].as_str().expect("provider");
    assert_eq!(provider, mum.endpoint_id(), "Mum should have served it");
    assert_ne!(provider, chad_id, "Chad is gone and cannot have served it");

    let materialized = c_aunt
        .wait_for(TIMEOUT, |env| env.e == "fetch.materialized")
        .await
        .expect("aunt materialized");
    let paths: Vec<std::path::PathBuf> =
        serde_json::from_value(materialized.p["paths"].clone()).expect("paths");
    assert_eq!(
        std::fs::read(&paths[0]).expect("read"),
        b"Recipe for scones\n"
    );

    Arc::clone(&mum).shutdown().await;
    Arc::clone(&aunt).shutdown().await;
}

/// A UI that has gone away must not swallow consent questions.
///
/// Regression test for a real bug, and one that only appears over a socket: when
/// a client's process dies, the daemon's writer task is still parked holding the
/// receiving end of that client's channel, so `Sender::is_closed()` reports
/// `false` and the channel looks healthy. The router therefore handed questions
/// to a ghost, the frame was consumed by a write to a dead socket, and the offer
/// sat unanswered until it timed out.
///
/// In practice: restart the app, and the next file anyone sent you disappeared.
#[tokio::test]
async fn a_departed_ui_does_not_swallow_questions() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("second-chance.txt");
    std::fs::write(&payload, b"it should still arrive\n").expect("write payload");

    let sender = Service::new(options(tmp.path(), "sender3"))
        .await
        .expect("sender");
    let receiver = Service::new(options(tmp.path(), "receiver3"))
        .await
        .expect("receiver");

    let sock_send = tmp.path().join("send3-run").join("control.sock");
    let sock_recv = tmp.path().join("recv3-run").join("control.sock");
    tokio::spawn(
        ControlListener::bind(Arc::clone(&sender), &sock_send)
            .await
            .expect("bind sender")
            .serve(),
    );
    tokio::spawn(
        ControlListener::bind(Arc::clone(&receiver), &sock_recv)
            .await
            .expect("bind receiver")
            .serve(),
    );

    let client_a = connect(&sock_send, Hello::control("sender"), None)
        .await
        .expect("sender client");

    // A UI attaches and then its process "dies" — the socket goes away without
    // any goodbye, exactly as `pkill` would leave it.
    let ghost = connect(&sock_recv, Hello::ui("ghost"), None)
        .await
        .expect("ghost");
    drop(ghost);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The replacement UI accepts everything.
    let live = connect(
        &sock_recv,
        Hello::ui("live"),
        Some(iroh_drop_daemon::Client::accept_all()),
    )
    .await
    .expect("live client");

    let drop_a = client_a
        .call("drop.create", json!({}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket = client_a
        .call("drop.ticket", json!({"drop": drop_a, "full": true}))
        .await
        .expect("ticket")["ticket"]
        .clone();
    live.call("drop.join", json!({"ticket": ticket}))
        .await
        .expect("join");

    tokio::time::sleep(Duration::from_millis(750)).await;
    client_a
        .call(
            "offer.publish",
            json!({"drop": drop_a, "path": payload.to_str().expect("utf8")}),
        )
        .await
        .expect("publish");

    // Generously shorter than CONSENT_TIMEOUT: if the ghost swallowed the
    // question, nothing happens at all and this is the assertion that says so.
    let materialized = live
        .wait_for(Duration::from_secs(25), |env| env.e == "fetch.materialized")
        .await
        .expect("the surviving UI should have been asked");
    let paths: Vec<std::path::PathBuf> =
        serde_json::from_value(materialized.p["paths"].clone()).expect("paths");
    assert_eq!(
        std::fs::read(&paths[0]).expect("read"),
        b"it should still arrive\n"
    );

    Arc::clone(&sender).shutdown().await;
    Arc::clone(&receiver).shutdown().await;
}
