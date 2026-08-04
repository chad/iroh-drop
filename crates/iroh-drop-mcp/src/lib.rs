//! An MCP (Model Context Protocol) server fronting a running iroh-drop
//! daemon. This is the third consumer of the daemon's control API — after
//! the CLI and the GUI — and it gets **no privileged hooks**: everything an
//! agent can do here is a method any client can call.
//!
//! Transport: newline-delimited JSON-RPC 2.0 on stdio (the MCP stdio
//! transport). stdout carries protocol messages only; diagnostics go to
//! stderr.
//!
//! Consent rule for agents (also written down in `docs/daemon-api.md`): an
//! MCP client connects with the **control** role, never the ui role, so the
//! daemon will not route consent questions to it — unsolicited offers keep
//! waiting for a human. The agent's own `fetch` call *is* the ask-and-answer
//! in one step, for the requested item only.

use std::time::Duration;

use iroh_drop_daemon::{Client, Envelope, Hello};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

/// MCP protocol revision this server speaks.
pub const PROTOCOL_VERSION: &str = "2025-03-26";

/// Server identity reported in the `initialize` handshake.
pub const SERVER_NAME: &str = "iroh-drop-mcp";

/// How long `fetch` / `share_files` wait for a task to finish before
/// handing the task id back for polling via `drop_events`.
const TASK_WAIT: Duration = Duration::from_secs(300);

/// Attach to a running daemon at the default socket path and serve MCP on
/// stdin/stdout until EOF.
pub async fn serve_stdio(socket: &std::path::Path) -> Result<(), iroh_drop_daemon::ApiError> {
    let client = iroh_drop_daemon::connect(socket, Hello::control(SERVER_NAME), None).await?;
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    run(client, stdin, stdout)
        .await
        .map_err(|e| iroh_drop_daemon::ApiError::new("stdio", e.to_string()))
}

/// Run the JSON-RPC loop over any byte pair until the reader hits EOF.
/// Kept transport-generic so tests can drive it over in-memory pipes.
pub async fn run<R, W>(client: Client, reader: R, mut writer: W) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncBufRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                write_msg(
                    &mut writer,
                    &error(Value::Null, -32700, &format!("parse error: {e}")),
                )
                .await?;
                continue;
            }
        };
        // Notifications (no id) are never answered.
        let Some(id) = msg.get("id").cloned() else {
            continue;
        };
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let reply = dispatch(&client, id, method, msg.get("params").cloned()).await;
        write_msg(&mut writer, &reply).await?;
    }
    Ok(())
}

async fn write_msg<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(msg)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

fn result(id: Value, r: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": r})
}

fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

/// A tool call that worked: content is the daemon's JSON, as text.
fn tool_ok(v: Value) -> Value {
    json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&v).unwrap_or_default()}],
    })
}

/// A tool call that failed at the application level (bad drop handle,
/// ambiguous name, fetch failed, …): `isError` per the MCP spec.
fn tool_err(msg: impl std::fmt::Display) -> Value {
    json!({
        "content": [{"type": "text", "text": msg.to_string()}],
        "isError": true,
    })
}

async fn dispatch(client: &Client, id: Value, method: &str, params: Option<Value>) -> Value {
    match method {
        "initialize" => result(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION")},
                "instructions": INSTRUCTIONS,
            }),
        ),
        "ping" => result(id, json!({})),
        "tools/list" => result(id, json!({"tools": tools()})),
        "tools/call" => {
            let name = params
                .as_ref()
                .and_then(|p| p.get("name"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let args = params
                .as_ref()
                .and_then(|p| p.get("arguments").cloned())
                .unwrap_or_else(|| json!({}));
            match call_tool(client, name, args).await {
                Ok(v) => result(id, tool_ok(v)),
                Err(ToolError::UnknownTool) => error(id, -32602, &format!("unknown tool {name}")),
                Err(ToolError::Daemon(msg)) => result(id, tool_err(msg)),
            }
        }
        _ => error(id, -32601, &format!("no such method {method}")),
    }
}

enum ToolError {
    UnknownTool,
    Daemon(String),
}

impl From<iroh_drop_daemon::ApiError> for ToolError {
    fn from(e: iroh_drop_daemon::ApiError) -> Self {
        ToolError::Daemon(format!("{}: {}", e.code, e.msg))
    }
}

async fn call_tool(client: &Client, name: &str, args: Value) -> Result<Value, ToolError> {
    match name {
        "list_drops" => Ok(client.call("drop.list", json!({})).await?),
        "create_drop" => {
            let name = args.get("name").cloned().unwrap_or(Value::Null);
            let mut v = client.call("drop.create", json!({"name": name})).await?;
            // The shareable link is what an agent wants; mint it like `share` does.
            if let Some(ticket) = v.get("ticket").and_then(Value::as_str) {
                let link = v
                    .get("link")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("iroh-drop://receive/{ticket}"));
                v.as_object_mut()
                    .map(|m| m.insert("link".into(), Value::String(link)));
            }
            Ok(v)
        }
        "join_drop" => {
            let text = args
                .get("ticket_or_link")
                .and_then(Value::as_str)
                .ok_or_else(|| ToolError::Daemon("ticket_or_link is required".into()))?;
            let ticket = ticket_from_text(text)
                .ok_or_else(|| ToolError::Daemon("no drop ticket found in the input".into()))?;
            Ok(client.call("drop.join", json!({"ticket": ticket})).await?)
        }
        "leave_drop" => {
            let drop = require(&args, "drop")?;
            Ok(client
                .call(
                    "drop.leave",
                    json!({"drop": drop, "forget": args.get("forget").cloned().unwrap_or(json!(false))}),
                )
                .await?)
        }
        "list_offers" => {
            let drop = require(&args, "drop")?;
            Ok(client.call("offer.list", json!({"drop": drop})).await?)
        }
        "share_files" => {
            let drop = require(&args, "drop")?;
            let paths = args
                .get("paths")
                .and_then(Value::as_array)
                .ok_or_else(|| ToolError::Daemon("paths (array) is required".into()))?;
            let name = args.get("name").and_then(Value::as_str);
            let mut published = Vec::new();
            for path in paths {
                let Some(path) = path.as_str() else {
                    return Err(ToolError::Daemon("paths must be strings".into()));
                };
                if name.is_some() && paths.len() > 1 {
                    return Err(ToolError::Daemon(
                        "name only makes sense for a single file".into(),
                    ));
                }
                let started = client
                    .call(
                        "offer.publish",
                        json!({"drop": drop, "path": path, "name": name}),
                    )
                    .await?;
                let task = started
                    .get("task")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let done = wait_task(client, &task).await?;
                published.push(json!({"path": path, "task": task, "outcome": done}));
            }
            Ok(json!({"published": published}))
        }
        "fetch" => {
            // The agent's call IS the consent: one explicit item, fetched on
            // request. Unsolicited offers are never auto-fetched (the daemon
            // only routes consent asks to ui-role clients, and we are not one).
            let drop = require(&args, "drop")?;
            let pick = require(&args, "pick")?;
            let out = args.get("out").and_then(Value::as_str);
            let started = client
                .call(
                    "offer.fetch",
                    json!({"drop": drop, "pick": pick, "out": out}),
                )
                .await?;
            let task = started
                .get("task")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let outcome = wait_task(client, &task).await?;
            Ok(json!({"task": task, "outcome": outcome}))
        }
        "drop_events" => {
            let from = args.get("from").and_then(Value::as_u64).unwrap_or(0);
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(50)
                .min(200) as usize;
            let mut v = client.call("events.replay", json!({"from": from})).await?;
            if let Some(events) = v.get_mut("events").and_then(Value::as_array_mut) {
                if events.len() > limit {
                    let keep = events.split_off(events.len() - limit);
                    *events = keep;
                }
            }
            Ok(v)
        }
        _ => Err(ToolError::UnknownTool),
    }
}

/// Wait for a daemon task's terminal state, returning the last known state.
/// A timeout is not an error: the agent gets the task id back and can poll
/// with `drop_events`.
async fn wait_task(client: &Client, task: &str) -> Result<Value, ToolError> {
    let pred = |env: &Envelope| {
        env.e == "task.state"
            && env.p.get("task").and_then(Value::as_str) == Some(task)
            && env.p.get("state").and_then(Value::as_str) != Some("running")
    };
    match client.wait_for(TASK_WAIT, pred).await {
        Ok(env) => {
            let state = env.p.get("state").cloned().unwrap_or(json!("unknown"));
            let error = env.p.get("error").cloned().unwrap_or(Value::Null);
            Ok(json!({"state": state, "error": error}))
        }
        Err(e) if e.code == "timeout" => Ok(json!({
            "state": "running",
            "note": "still working after 5 minutes; poll drop_events for task.state",
        })),
        Err(e) => Err(e.into()),
    }
}

fn require<'a>(args: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::Daemon(format!("{key} is required")))
}

/// Pull a ticket out of whatever was pasted: a bare `drop2…`, an
/// `iroh-drop://receive/…` link, an `https://host/#drop2…` link, or chat
/// chatter containing one. Same semantics as the CLI's extractor — `drop1`
/// is still located so the daemon can reject it with its precise version
/// error.
pub fn ticket_from_text(input: &str) -> Option<&str> {
    let start = input
        .find("drop2")
        .into_iter()
        .chain(input.find("drop1"))
        .min()?;
    let end = input[start..]
        .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .map_or(input.len(), |offset| start + offset);
    let candidate = &input[start..end];
    (candidate.len() > 32).then_some(candidate)
}

fn tools() -> Vec<Value> {
    let drop_prop = json!({"type": "string", "description": "Daemon-local drop handle (e.g. \"d3\") from list_drops, create_drop, or join_drop"});
    vec![
        json!({
            "name": "list_drops",
            "description": "List drops this daemon participates in: handle, name, peer count, offer count.",
            "inputSchema": {"type": "object", "properties": {}},
        }),
        json!({
            "name": "create_drop",
            "description": "Create a new drop. Returns the handle, the bearer ticket, and a shareable link.",
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string", "description": "Display name for the drop"}},
            },
        }),
        json!({
            "name": "join_drop",
            "description": "Join a drop. Accepts a bare drop2… ticket, an iroh-drop:// link, an https://…#drop2… link, or text containing one.",
            "inputSchema": {
                "type": "object",
                "properties": {"ticket_or_link": {"type": "string"}},
                "required": ["ticket_or_link"],
            },
        }),
        json!({
            "name": "leave_drop",
            "description": "Leave a drop. With forget=true also stops serving its content.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drop": drop_prop,
                    "forget": {"type": "boolean", "description": "Also forget local state (default false)"},
                },
                "required": ["drop"],
            },
        }),
        json!({
            "name": "list_offers",
            "description": "List what a drop offers: numbered items with name, hash, size, and status (missing / fetching / failed / available).",
            "inputSchema": {
                "type": "object",
                "properties": {"drop": drop_prop},
                "required": ["drop"],
            },
        }),
        json!({
            "name": "share_files",
            "description": "Publish local files into a drop so its members can fetch them. Waits for each publish and reports the outcome.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drop": drop_prop,
                    "paths": {"type": "array", "items": {"type": "string"}, "description": "Absolute paths to publish"},
                    "name": {"type": "string", "description": "Display name (single file only)"},
                },
                "required": ["drop", "paths"],
            },
        }),
        json!({
            "name": "fetch",
            "description": "Fetch one offered item by number, name, or hash prefix. This call IS the user's consent for that item; unsolicited offers are never fetched automatically. Waits for completion (up to 5 minutes) and reports the outcome.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "drop": drop_prop,
                    "pick": {"type": "string", "description": "Item number, file name, or hash prefix from list_offers"},
                    "out": {"type": "string", "description": "Output directory (default: the daemon's download dir)"},
                },
                "required": ["drop", "pick"],
            },
        }),
        json!({
            "name": "drop_events",
            "description": "Bounded tail of the daemon's event log (peer joins, offers, progress, completions).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "from": {"type": "integer", "description": "Replay events with seq >= from (default 0)"},
                    "limit": {"type": "integer", "description": "Max events to return (default 50, max 200)"},
                },
            },
        }),
    ]
}

/// System-style instructions handed to the agent in `initialize`, so the
/// consent rule travels with the server instead of depending on the prompt.
const INSTRUCTIONS: &str = "\
iroh-drop is a consent-based file-sharing network. You may list drops and \
offers freely, and you may publish or fetch ONLY what the user asked for: a \
fetch call is treated as the user's explicit consent for that one item. \
Never fetch unsolicited offers, and never join a drop the user did not give \
you a ticket for.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_extraction_matches_cli_semantics() {
        let bare = "drop2aimfofis3yfxv6oqyama7tbogzvqj2fkxfgrqkcpjnnrgsj5q";
        assert_eq!(ticket_from_text(bare), Some(bare));
        assert_eq!(
            ticket_from_text(&format!("iroh-drop://receive/{bare}")),
            Some(bare)
        );
        assert_eq!(
            ticket_from_text(&format!("open https://iroh-drop.boxd.sh/#{bare} please")),
            Some(bare)
        );
        assert_eq!(ticket_from_text("drop2short"), None);
        assert_eq!(ticket_from_text("no ticket here"), None);
    }
}
