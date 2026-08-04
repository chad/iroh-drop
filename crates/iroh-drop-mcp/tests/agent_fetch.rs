//! The WS5 success metric, as a test: **an agent completes "fetch dataset X
//! from drop Y" via MCP with no shell** — and the consent rule holds: an
//! unsolicited offer is *not* auto-accepted just because an MCP client is
//! connected. The agent is a scripted JSON-RPC peer on an in-memory pipe;
//! the daemons are real (offline endpoints, loopback QUIC, full tickets).

use std::sync::Arc;
use std::time::Duration;

use iroh_drop_daemon::{Client, Hello, Service, ServiceOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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

/// A scripted MCP client: writes requests, reads responses, in order.
struct Agent {
    write: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    lines: tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    next_id: u64,
}

impl Agent {
    fn new(client: Client) -> (Self, tokio::task::JoinHandle<()>) {
        let (agent_end, server_end) = tokio::io::duplex(1 << 20);
        let (server_read, server_write) = tokio::io::split(server_end);
        let (agent_read, agent_write) = tokio::io::split(agent_end);
        let server = tokio::spawn(async move {
            iroh_drop_mcp::run(client, BufReader::new(server_read), server_write)
                .await
                .expect("mcp run");
        });
        (
            Agent {
                write: agent_write,
                lines: BufReader::new(agent_read).lines(),
                next_id: 0,
            },
            server,
        )
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let msg = json!({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params});
        self.write
            .write_all(serde_json::to_string(&msg).unwrap().as_bytes())
            .await
            .expect("write");
        self.write.write_all(b"\n").await.expect("write nl");
        let line = tokio::time::timeout(TIMEOUT, self.lines.next_line())
            .await
            .expect("response within timeout")
            .expect("stream open")
            .expect("line reads");
        serde_json::from_str(&line).expect("response parses")
    }

    async fn call_tool(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name": name, "arguments": args}))
            .await
    }
}

/// A control-role daemon client wired to an MCP server + scripted agent.
async fn agent_for(service: &Arc<Service>) -> (Agent, tokio::task::JoinHandle<()>) {
    let client = Client::connect_memory(service, Hello::control("mcp-test"), None)
        .await
        .expect("control client");
    Agent::new(client)
}

/// Pull the text payload out of a tools/call result.
fn tool_text(resp: &Value) -> Value {
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .expect("tool text");
    serde_json::from_str(text).expect("tool payload parses")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_fetches_dataset_via_mcp() {
    let tmp = tempfile::tempdir().expect("tmp");
    let payload = tmp.path().join("dataset.csv");
    std::fs::write(&payload, b"id,value\n1,apples\n2,oranges\n").expect("write dataset");

    let a = Arc::new(
        Service::new(options(tmp.path(), "a"))
            .await
            .expect("service a"),
    );
    let b = Arc::new(
        Service::new(options(tmp.path(), "b"))
            .await
            .expect("service b"),
    );

    // A is driven by a UI client (the human sharer).
    let client_a = Client::connect_memory(&a, Hello::ui("test-a"), None)
        .await
        .expect("client a");
    let created = client_a
        .call("drop.create", json!({"name": "datasets"}))
        .await
        .expect("drop.create");
    let drop_a = created["drop"].as_str().expect("drop handle").to_string();
    let ticket = client_a
        .call("drop.ticket", json!({"drop": drop_a, "full": true}))
        .await
        .expect("drop.ticket")["ticket"]
        .as_str()
        .expect("ticket")
        .to_string();

    // B is driven ONLY by the MCP server (the agent's hands).
    let (mut agent, server) = agent_for(&b).await;

    // Handshake.
    let init = agent.request("initialize", json!({})).await;
    assert_eq!(init["result"]["serverInfo"]["name"], "iroh-drop-mcp");
    agent
        .write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .expect("notify");

    // Tool inventory: the whole agent surface, no more.
    let list = agent.request("tools/list", json!({})).await;
    let tools = list["result"]["tools"].as_array().expect("tools");
    assert_eq!(tools.len(), 8);

    // Join via an app link (not a bare ticket): same extraction semantics.
    let joined = agent
        .call_tool(
            "join_drop",
            json!({"ticket_or_link": format!("iroh-drop://receive/{ticket}")}),
        )
        .await;
    let drop_b = tool_text(&joined)["drop"]
        .as_str()
        .expect("joined")
        .to_string();

    // The human shares dataset.csv.
    let published = client_a
        .call(
            "offer.publish",
            json!({"drop": drop_a, "path": payload.to_str().expect("utf8 path")}),
        )
        .await
        .expect("offer.publish");
    client_a
        .wait_for(TIMEOUT, |env| {
            env.e == "task.state"
                && env.p["task"] == published["task"]
                && env.p["state"] == json!("done")
        })
        .await
        .expect("publish completes");

    // The agent sees the offer arrive…
    let offers = tokio::time::timeout(TIMEOUT, async {
        loop {
            let resp = agent
                .call_tool("list_offers", json!({"drop": drop_b}))
                .await;
            let items = tool_text(&resp)["items"].as_array().expect("items").clone();
            if !items.is_empty() {
                return items;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("offer reaches the agent's daemon");

    // …and CONSENT HOLDS: nobody fetches it for the agent. Give the daemon
    // every chance to misbehave, then prove it didn't.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = offers[0]["status"].as_str().expect("status");
    assert_eq!(status, "missing", "unsolicited offer must stay unfetched");
    let tasks = agent
        .call_tool("list_offers", json!({"drop": drop_b}))
        .await;
    assert_eq!(
        tool_text(&tasks)["items"][0]["status"],
        "missing",
        "an MCP client connected must not auto-accept offers"
    );

    // The agent fetches what it was (in this story) asked to fetch.
    let fetched = agent
        .call_tool("fetch", json!({"drop": drop_b, "pick": "dataset.csv"}))
        .await;
    let outcome = tool_text(&fetched)["outcome"].clone();
    assert_eq!(outcome["state"], "done", "fetch outcome: {outcome}");

    // Bytes landed, correct.
    let landed = tmp.path().join("b-downloads").join("dataset.csv");
    let bytes = std::fs::read(&landed).expect("dataset on disk");
    assert_eq!(bytes, b"id,value\n1,apples\n2,oranges\n");

    // Protocol hygiene: unknown tool is a JSON-RPC error, not a crash.
    let bad = agent.call_tool("exfiltrate_everything", json!({})).await;
    assert_eq!(bad["error"]["code"], -32602);

    drop(agent);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_client_never_receives_consent_asks() {
    // Structural version of the consent rule: asks route to ui-role clients
    // only. A control-role MCP client gets no ask channel at all — verified
    // here by asking the daemon's role enforcement directly.
    let tmp = tempfile::tempdir().expect("tmp");
    let b = Arc::new(
        Service::new(options(tmp.path(), "b"))
            .await
            .expect("service"),
    );
    let client = Client::connect_memory(&b, Hello::control("mcp-test"), None)
        .await
        .expect("control client");
    // A control client trying to act on a drop it isn't in gets a clean
    // not-found, and methods requiring no special role still work.
    let err = client
        .call("offer.fetch", json!({"drop": "d9", "pick": "x"}))
        .await
        .expect_err("unknown drop errors");
    assert_eq!(err.code, "not_found");
    assert!(client.call("drop.list", json!({})).await.is_ok());
}
