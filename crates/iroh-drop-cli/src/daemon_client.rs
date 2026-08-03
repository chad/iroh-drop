//! Commands that talk to a running `iroh-dropd` instead of owning a session.
//!
//! The difference users feel: `send` **returns**. The daemon keeps the drop
//! alive and keeps serving the bytes, so closing the terminal is no longer the
//! same as withdrawing the file. That is also what makes you a real replica for
//! other people rather than one in principle.

use anyhow::{bail, Context, Result};
use iroh_drop_daemon::{connect, default_socket_path, Client, Hello};
use serde_json::{json, Value};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use crate::ui;

/// Attach to the daemon, or explain how to start one.
///
/// Only `watch` connects as a `ui`. This matters: the daemon routes each
/// consent question to the *first* live UI client, so a one-shot command that
/// registered as a UI without a prompt would silently decline offers meant for
/// somebody's `watch` session in another terminal.
async fn attach(
    socket: Option<PathBuf>,
    hello: Hello,
    ask: Option<iroh_drop_daemon::AskHandler>,
) -> Result<Client> {
    let path = socket.unwrap_or_else(default_socket_path);
    match connect(&path, hello, ask).await {
        Ok(client) => Ok(client),
        Err(e) if e.code == "no_daemon" => bail!(
            "no daemon at {}.\n  start one with:  iroh-dropd\n  ({e})",
            path.display()
        ),
        Err(e) => bail!("{e}"),
    }
}

fn as_str(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// `iroh-drop send <paths>` — hand the files to the daemon and exit.
pub async fn send(
    paths: Vec<PathBuf>,
    name: Option<String>,
    qr: bool,
    socket: Option<PathBuf>,
) -> Result<()> {
    let client = attach(socket, Hello::control("iroh-drop send"), None).await?;

    let created = client
        .call("drop.create", json!({"name": name}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let drop = created["drop"].clone();

    for path in &paths {
        let path = std::fs::canonicalize(path)
            .with_context(|| format!("cannot find {}", path.display()))?;
        let started = client
            .call(
                "offer.publish",
                json!({"drop": drop, "path": path.to_str().context("path is not UTF-8")?}),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let task = as_str(&started, "task");

        // Publishing imports and hashes the bytes, so it is not instant for a
        // big tree. Wait for this file before starting the next one.
        let done = client
            .wait_for(std::time::Duration::from_secs(3600), |env| {
                env.e == "task.state"
                    && env.p["task"] == task.as_str()
                    && env.p["state"] != "running"
            })
            .await
            .map_err(|e| anyhow::anyhow!("publishing {}: {e}", path.display()))?;
        if done.p["state"] != "done" {
            bail!(
                "could not share {}: {}",
                path.display(),
                as_str(&done.p, "error")
            );
        }
        println!("  added {}", path.display());
    }

    let ticket = client
        .call("drop.ticket", json!({"drop": drop}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // A link, not a ticket: it opens the app when clicked, and nobody has to
    // learn a new noun.
    let link = as_str(&ticket, "link");
    let web = ticket.get("web_link").and_then(Value::as_str);

    println!("\nSend this to whoever should get the files:\n\n  {link}\n");
    if let Some(web) = web {
        println!("Or, for anyone without the app:\n\n  {web}\n");
    }
    if qr {
        // QR carries the link too, so a phone camera opens the app.
        ui::print_qr(&link);
    }
    println!(
        "The daemon keeps serving these files. Stop with:\n  iroh-drop drops --forget {}",
        as_str(&created, "drop")
    );
    Ok(())
}

/// `iroh-drop watch` — show what is happening, and answer consent prompts.
///
/// This is the receiving end: the daemon asks before anything is written to
/// disk, and a prompt that is not answered is a refusal.
pub async fn watch(accept_all: bool, socket: Option<PathBuf>) -> Result<()> {
    // The handler runs on the client's reader task, so it must not block on
    // stdin. Prompts are queued to a dedicated thread instead.
    let (ask_tx, ask_rx) = std::sync::mpsc::channel::<(Value, std::sync::mpsc::Sender<bool>)>();
    let ask_tx = Arc::new(ask_tx);

    let handler: iroh_drop_daemon::AskHandler = if accept_all {
        Arc::new(|_q, _p| Some(json!({"accept": true})))
    } else {
        let ask_tx = Arc::clone(&ask_tx);
        Arc::new(move |_q, p: Value| {
            let (reply_tx, reply_rx) = std::sync::mpsc::channel();
            if ask_tx.send((p, reply_tx)).is_err() {
                return None;
            }
            // Deny on any failure: silence is never consent.
            match reply_rx.recv() {
                Ok(true) => Some(json!({"accept": true})),
                _ => None,
            }
        })
    };

    std::thread::spawn(move || {
        while let Ok((offer, reply)) = ask_rx.recv() {
            let name = as_str(&offer, "name");
            let size = as_str(&offer, "human_size");
            let from = as_str(&offer, "from");
            let short: String = from.chars().take(10).collect();
            println!();
            // Everything in an offer is untrusted display metadata. Quote the
            // name so a filename cannot impersonate our own output.
            println!("  Incoming file: {:?}  ({size})", name);
            println!("  From peer:     {short}");
            print!("  Accept? [y/N] ");
            let _ = std::io::stdout().flush();
            let mut line = String::new();
            let yes = match std::io::stdin().read_line(&mut line) {
                Ok(_) => matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes"),
                Err(_) => false,
            };
            let _ = reply.send(yes);
        }
    });

    let client = attach(socket, Hello::ui("iroh-drop watch"), Some(handler)).await?;
    let status = client
        .call("daemon.status", json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!(
        "Watching. Files land in {}",
        as_str(&status, "download_dir")
    );
    if accept_all {
        println!("⚠ --yes is on: anything offered will be downloaded without asking.");
    }
    println!("Ctrl-C to stop.\n");

    let mut events = client.events();
    loop {
        match events.recv().await {
            Ok(env) => print_event(&env.e, &env.p),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                println!("  (missed {n} events)")
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                bail!("the daemon went away")
            }
        }
    }
}

fn print_event(name: &str, p: &Value) {
    match name {
        "offer.received" => println!(
            "  offered  {:?}  ({})",
            as_str(p, "name"),
            as_str(p, "human_size")
        ),
        "offer.declined" => println!("  declined {} ", as_str(p, "reason")),
        "fetch.progress" => {
            let done = p["downloaded"].as_u64().unwrap_or(0);
            match p["total"].as_u64() {
                Some(total) if total > 0 => {
                    print!("\r  receiving… {}%", done * 100 / total);
                    let _ = std::io::stdout().flush();
                }
                _ => {}
            }
        }
        "fetch.materialized" => {
            let paths = p["paths"].as_array().cloned().unwrap_or_default();
            println!("\r  ✔ saved {} file(s)", paths.len());
            for path in paths {
                if let Some(path) = path.as_str() {
                    println!("      {path}");
                }
            }
        }
        "fetch.failed" => println!("\r  ✗ failed: {}", as_str(p, "error")),
        "peer.joined" => println!("  a peer joined"),
        _ => {}
    }
}

/// `iroh-drop get <ticket>` — join a drop through the daemon and fetch.
pub async fn get(
    ticket: String,
    pick: Option<String>,
    out: Option<PathBuf>,
    socket: Option<PathBuf>,
) -> Result<()> {
    // An observer, not a UI: asking for a file *is* the consent, and being a
    // UI here would download it a second time through the consent path.
    let client = attach(socket, Hello::control("iroh-drop get"), None).await?;
    let ticket = crate::read_ticket(&ticket)?.to_string();

    let joined = client
        .call("drop.join", json!({"ticket": ticket}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let drop = joined["drop"].clone();
    println!("Joined. Looking for files…");

    // Contents may arrive by catch-up sync or by a live announcement.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let items = loop {
        let listed = client
            .call("offer.list", json!({"drop": drop}))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let items = listed["items"].as_array().cloned().unwrap_or_default();
        if !items.is_empty() {
            break items;
        }
        if std::time::Instant::now() > deadline {
            bail!("nothing was offered in this drop after 60s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };

    println!();
    for item in &items {
        println!(
            "  {}  {:<32} {:>10}  {}",
            item["n"],
            as_str(item, "name"),
            as_str(item, "human_size"),
            as_str(item, "kind")
        );
    }
    println!();

    let picks: Vec<String> = match pick {
        Some(pick) => vec![pick],
        None => items.iter().map(|i| i["n"].to_string()).collect(),
    };

    for pick in picks {
        let mut params = json!({"drop": drop, "pick": pick});
        if let Some(out) = &out {
            params["out"] = json!(out.to_str().context("output path is not UTF-8")?);
        }
        let started = client
            .call("offer.fetch", params)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let task = as_str(&started, "task");
        let done = client
            .wait_for(std::time::Duration::from_secs(3600), |env| {
                env.e == "task.state"
                    && env.p["task"] == task.as_str()
                    && env.p["state"] != "running"
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if done.p["state"] == "done" {
            println!("  ✔ got item {pick}");
        } else {
            println!("  ✗ item {pick}: {}", as_str(&done.p, "error"));
        }
    }

    println!("\nYou are now also serving these files, so others can get them from you.");
    Ok(())
}

/// `iroh-drop join <ticket>` — enter a drop without fetching anything.
///
/// The counterpart to `get`: this is how an *unsolicited* offer reaches you, so
/// whatever UI is attached does the asking. Useful on its own (stay in a drop
/// and be prompted as things appear) and the only way to exercise the consent
/// prompt deliberately.
pub async fn join(ticket: String, socket: Option<PathBuf>) -> Result<()> {
    let client = attach(socket, Hello::control("iroh-drop join"), None).await?;
    let ticket = crate::read_ticket(&ticket)?.to_string();
    let joined = client
        .call("drop.join", json!({"ticket": ticket}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if joined["already"].as_bool() == Some(true) {
        println!(
            "Already in that group as {} — one membership, however many times you join.",
            as_str(&joined, "drop")
        );
    } else {
        println!(
            "Joined as {}. You stay in the group until you leave it (`iroh-drop leave {}`); \
             \nanything offered shows up in the app, or with `iroh-drop watch`.",
            as_str(&joined, "drop"),
            as_str(&joined, "drop")
        );
    }
    Ok(())
}

/// `iroh-drop drops` — what the daemon is hosting.
/// Leave a drop — the explicit end of membership. Everything before this is
/// sticky by default: joining once means staying, across restarts, until
/// `leave` (or `drops --forget`) says otherwise.
pub async fn leave(drop: String, socket: Option<PathBuf>) -> Result<()> {
    let client = attach(socket, Hello::control("iroh-drop leave"), None).await?;

    if drop == "all" {
        let listed = client
            .call("drop.list", json!({}))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let handles: Vec<String> = listed["drops"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|r| r["drop"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if handles.is_empty() {
            println!("Nothing to leave.");
            return Ok(());
        }
        for handle in &handles {
            client
                .call("drop.leave", json!({"drop": handle}))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("Left {handle}.");
        }
        return Ok(());
    }

    // Say what we are leaving, not just a handle.
    let listed = client
        .call("drop.list", json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let name = listed["drops"]
        .as_array()
        .and_then(|rows| {
            rows.iter()
                .find(|r| r["drop"].as_str() == Some(drop.as_str()))
                .and_then(|r| r["name"].as_str().map(str::to_string))
        })
        .unwrap_or_else(|| "received files".to_string());
    client
        .call("drop.leave", json!({"drop": drop}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("Left {drop} ({name}). The group keeps going without you.");
    Ok(())
}

pub async fn drops(
    forget: Option<String>,
    ticket: Option<String>,
    socket: Option<PathBuf>,
) -> Result<()> {
    // --forget and --ticket mutate (or reveal a capability), so they need
    // the control role; a plain listing stays an observer.
    let hello = if forget.is_some() || ticket.is_some() {
        Hello::control("iroh-drop drops")
    } else {
        Hello::observer("iroh-drop drops")
    };
    let client = attach(socket, hello, None).await?;

    // A ticket from a peer that is still running is the reliable way to bring
    // in latecomers, and it lists that peer first — so this is how a drop
    // keeps working after the original sender is gone.
    if let Some(handle) = ticket {
        let ticket = client
            .call("drop.ticket", json!({"drop": handle}))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("{}", as_str(&ticket, "link"));
        return Ok(());
    }

    if let Some(handle) = forget {
        client
            .call("drop.leave", json!({"drop": handle}))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        println!("Stopped hosting {handle}.");
        return Ok(());
    }

    let status = client
        .call("daemon.status", json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!(
        "daemon {}  {}  downloads: {}",
        as_str(&status, "endpoint_id"),
        if status["offline"] == true {
            "LAN only (no relays)"
        } else {
            "internet + LAN"
        },
        as_str(&status, "download_dir")
    );

    let listed = client
        .call("drop.list", json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let drops = listed["drops"].as_array().cloned().unwrap_or_default();
    if drops.is_empty() {
        println!("\nNo drops. Share something with:  iroh-drop send <file>");
        return Ok(());
    }
    println!();
    for d in drops {
        println!(
            "  {}  {:<20} {} file(s), {}, {} peer(s)",
            as_str(&d, "drop"),
            d["name"].as_str().unwrap_or("received"),
            d["files"],
            as_str(&d, "human_size"),
            d["peers"]
        );
    }
    println!("\nDrops persist across restarts. Leave one with:  iroh-drop leave <id>");
    Ok(())
}

/// Resolve a socket override that may be a directory.
pub fn socket_arg(value: Option<PathBuf>) -> Option<PathBuf> {
    value.map(|p| {
        if p.is_dir() {
            p.join("control.sock")
        } else {
            p
        }
    })
}
