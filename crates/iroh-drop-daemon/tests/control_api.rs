//! End-to-end tests of the control API over real endpoints, fully offline.
//!
//! These drive two daemons in one process through the same frames a Unix
//! socket would carry, so the transport is the only untested part.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use iroh_drop_daemon::{Client, Hello, Service, ServiceOptions};
use serde_json::json;

const TIMEOUT: Duration = Duration::from_secs(60);

fn options(dir: &std::path::Path, name: &str) -> ServiceOptions {
    let download_dir = dir.join(format!("{name}-downloads"));
    std::fs::create_dir_all(&download_dir).expect("download dir");
    ServiceOptions {
        store_path: Some(dir.join(format!("{name}-store"))),
        identity_path: Some(dir.join(format!("{name}-identity"))),
        // The most decentralized posture: no relay, no DNS, no pkarr.
        offline: true,
        mdns: false,
        download_dir,
        auto_accept: false,
        link_base: None,
    }
}

async fn full_ticket(client: &Client, drop: &serde_json::Value) -> String {
    client
        .call("drop.ticket", json!({"drop": drop, "full": true}))
        .await
        .expect("drop.ticket")["ticket"]
        .as_str()
        .expect("ticket string")
        .to_string()
}

/// A file offered to a daemon with a UI attached is fetched after consent,
/// and lands on disk.
#[tokio::test]
async fn consented_offer_is_fetched() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("report.pdf");
    std::fs::write(&payload, b"pretend this is a PDF\n").expect("write payload");

    let a = Service::new(options(tmp.path(), "a")).await.expect("service a");
    let b = Service::new(options(tmp.path(), "b")).await.expect("service b");

    let client_a = Client::connect_memory(&a, Hello::ui("test-a"), None)
        .await
        .expect("client a");
    // B has a UI that says yes. This is the only thing that makes a fetch
    // happen — auto_fetch is off in the daemon's policy.
    let client_b = Client::connect_memory(&b, Hello::ui("test-b"), Some(Client::accept_all()))
        .await
        .expect("client b");

    assert_eq!(client_a.hello["api"], 1);
    assert!(client_a.hello["methods"]
        .as_array()
        .expect("methods array")
        .iter()
        .any(|m| m == "offer.fetch"));

    let drop_a = client_a
        .call("drop.create", json!({"name": "test drop"}))
        .await
        .expect("drop.create")["drop"]
        .clone();
    let ticket = full_ticket(&client_a, &drop_a).await;

    let drop_b = client_b
        .call("drop.join", json!({"ticket": ticket}))
        .await
        .expect("drop.join")["drop"]
        .clone();

    // Let the gossip mesh form before announcing.
    tokio::time::sleep(Duration::from_millis(750)).await;

    client_a
        .call(
            "offer.publish",
            json!({"drop": drop_a, "path": payload.to_str().expect("utf8 path")}),
        )
        .await
        .expect("offer.publish");

    // B: offer arrives, consent is asked and granted, fetch runs.
    let materialized = client_b
        .wait_for(TIMEOUT, |env| env.e == "fetch.materialized")
        .await
        .expect("b materialized the offer");

    let paths: Vec<PathBuf> =
        serde_json::from_value(materialized.p["paths"].clone()).expect("paths");
    assert_eq!(paths.len(), 1, "one file expected");
    assert!(paths[0].exists(), "{} should exist", paths[0].display());
    assert_eq!(
        std::fs::read(&paths[0]).expect("read fetched"),
        b"pretend this is a PDF\n"
    );

    // Both sides now list the same item by name, and B can serve it.
    let items = client_b
        .call("offer.list", json!({"drop": drop_b}))
        .await
        .expect("offer.list")["items"]
        .clone();
    assert_eq!(items[0]["name"], "report.pdf");
    assert_eq!(items[0]["status"], "available");

    // Events are replayable: a UI that crashed can rebuild the truth.
    let replay = client_b
        .call("events.replay", json!({"from": 0}))
        .await
        .expect("events.replay");
    assert_eq!(replay["truncated"], false);
    let names: Vec<String> = replay["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|e| e["e"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names.contains(&"offer.received".to_string()));
    assert!(names.contains(&"fetch.completed".to_string()));

    Arc::clone(&a).shutdown().await;
    Arc::clone(&b).shutdown().await;
}

/// With no UI attached there is nobody to consent, so nothing is fetched.
/// Silence is never consent.
#[tokio::test]
async fn offer_without_a_ui_is_declined() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("unsolicited.bin");
    std::fs::write(&payload, b"you did not ask for this").expect("write payload");

    let a = Service::new(options(tmp.path(), "a")).await.expect("service a");
    let b = Service::new(options(tmp.path(), "b")).await.expect("service b");

    let client_a = Client::connect_memory(&a, Hello::ui("test-a"), None)
        .await
        .expect("client a");
    // Observer only: receives events, is never asked, cannot consent.
    let client_b = Client::connect_memory(&b, Hello::observer("test-b"), None)
        .await
        .expect("client b");

    let drop_a = client_a
        .call("drop.create", json!({}))
        .await
        .expect("drop.create")["drop"]
        .clone();
    let ticket = full_ticket(&client_a, &drop_a).await;
    let drop_b = client_b
        .call("drop.join", json!({"ticket": ticket}))
        .await
        .expect("drop.join")["drop"]
        .clone();

    tokio::time::sleep(Duration::from_millis(750)).await;

    client_a
        .call(
            "offer.publish",
            json!({"drop": drop_a, "path": payload.to_str().expect("utf8 path")}),
        )
        .await
        .expect("offer.publish");

    let declined = client_b
        .wait_for(TIMEOUT, |env| env.e == "offer.declined")
        .await
        .expect("b declined the offer");
    assert_eq!(declined.p["reason"], "no consent");

    // The offer is still *known* — it can be fetched later, on purpose.
    let items = client_b
        .call("offer.list", json!({"drop": drop_b}))
        .await
        .expect("offer.list")["items"]
        .clone();
    assert_eq!(items[0]["name"], "unsolicited.bin");
    assert_eq!(
        items[0]["status"], "missing",
        "nothing should have been downloaded"
    );

    Arc::clone(&a).shutdown().await;
    Arc::clone(&b).shutdown().await;
}

/// Unknown methods get a clean error, not a dropped connection — the same
/// courtesy the wire protocol extends to unknown control ops.
#[tokio::test]
async fn unknown_method_is_survivable() {
    let tmp = tempfile::tempdir().expect("tmp");
    let service = Service::new(options(tmp.path(), "s")).await.expect("service");
    let client = Client::connect_memory(&service, Hello::ui("test"), None)
        .await
        .expect("client");

    let err = client
        .call("drop.teleport", json!({}))
        .await
        .expect_err("should fail");
    assert_eq!(err.code, "unsupported");

    // The connection still works.
    let status = client
        .call("daemon.status", json!({}))
        .await
        .expect("status after error");
    assert_eq!(status["offline"], true);

    service.shutdown().await;
}

/// A configured link base also yields an `https` link, with the ticket in the
/// fragment so the page never sees it.
#[tokio::test]
async fn a_configured_base_adds_a_web_link() {
    let tmp = tempfile::tempdir().expect("tmp");
    let mut options = options(tmp.path(), "web");
    options.link_base = Some("https://drop.example/".into());

    let service = Service::new(options).await.expect("service");
    let client = Client::connect_memory(&service, Hello::observer("test"), None)
        .await
        .expect("client");
    let drop = client
        .call("drop.create", json!({}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket = client
        .call("drop.ticket", json!({"drop": drop}))
        .await
        .expect("ticket");

    let bare = ticket["ticket"].as_str().expect("ticket");
    let web = ticket["web_link"].as_str().expect("web link");
    // Fragment, not path or query: browsers never send it to the server, so the
    // page can be a static file that learns nothing.
    assert_eq!(web, format!("https://drop.example/#{bare}"));
    assert!(ticket["link"].as_str().expect("link").starts_with("iroh-drop://"));

    service.shutdown().await;
}

/// With `auto_accept` on and no UI attached at all, an incoming offer is fetched
/// rather than left to time out. This is what makes a windowless helper keep
/// sharing: the alternative is that closing the app silently stops all
/// transfers.
#[tokio::test]
async fn a_windowless_helper_still_serves() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("always-on.txt");
    std::fs::write(&payload, b"served with nobody watching\n").expect("write payload");

    let sender = Service::new(options(tmp.path(), "always-sender"))
        .await
        .expect("sender");
    // The receiver has no UI and auto_accept on: it should serve anyway.
    let mut receiver_options = options(tmp.path(), "always-receiver");
    receiver_options.auto_accept = true;
    let receiver = Service::new(receiver_options).await.expect("receiver");

    let client_a = Client::connect_memory(&sender, Hello::observer("sender"), None)
        .await
        .expect("sender client");
    // Only an *observer* on the receiver — never a UI.
    let client_b = Client::connect_memory(&receiver, Hello::observer("watcher"), None)
        .await
        .expect("watcher");

    let drop_a = client_a
        .call("drop.create", json!({}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket = full_ticket(&client_a, &drop_a).await;
    client_b
        .call("drop.join", json!({"ticket": ticket}))
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

    // No one was asked, and yet the file arrives.
    let materialized = client_b
        .wait_for(TIMEOUT, |env| env.e == "fetch.materialized")
        .await
        .expect("the windowless helper should have served the file");
    let paths: Vec<PathBuf> =
        serde_json::from_value(materialized.p["paths"].clone()).expect("paths");
    assert_eq!(
        std::fs::read(&paths[0]).expect("read"),
        b"served with nobody watching\n"
    );

    Arc::clone(&sender).shutdown().await;
    Arc::clone(&receiver).shutdown().await;
}

/// auto_accept must not bypass a live UI. The helper runs with
/// --accept-when-no-ui so it keeps serving after the window closes — but while
/// the window is *open*, an unsolicited offer has to be asked, not silently
/// fetched. This is a consent-bypass regression test: the flag is "when no UI",
/// not "always".
#[tokio::test]
async fn auto_accept_does_not_bypass_a_live_ui() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("ask-me-first.txt");
    std::fs::write(&payload, b"consent is not optional\n").expect("write payload");

    let sender = Service::new(options(tmp.path(), "consent-sender"))
        .await
        .expect("sender");
    let mut receiver_options = options(tmp.path(), "consent-receiver");
    receiver_options.auto_accept = true; // the helper's posture
    let receiver = Service::new(receiver_options).await.expect("receiver");

    let client_a = Client::connect_memory(&sender, Hello::observer("sender"), None)
        .await
        .expect("sender client");

    // A UI that explicitly REFUSES. If auto_accept bypassed it, the file would
    // arrive anyway; if the UI is asked, the offer is declined.
    let refuser = Client::connect_memory(
        &receiver,
        Hello::ui("refuser"),
        Some(std::sync::Arc::new(|_q, _p| Some(json!({"accept": false})))),
    )
    .await
    .expect("refuser client");
    let watcher = Client::connect_memory(&receiver, Hello::observer("watcher"), None)
        .await
        .expect("watcher");

    let drop_a = client_a
        .call("drop.create", json!({}))
        .await
        .expect("create")["drop"]
        .clone();
    let ticket = full_ticket(&client_a, &drop_a).await;
    watcher
        .call("drop.join", json!({"ticket": ticket}))
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

    // The UI said no, so the offer is declined — never fetched.
    let declined = refuser
        .wait_for(TIMEOUT, |env| env.e == "offer.declined")
        .await
        .expect("the live UI's refusal should have won");
    assert_eq!(declined.p["reason"], json!("declined"));

    Arc::clone(&sender).shutdown().await;
    Arc::clone(&receiver).shutdown().await;
}
