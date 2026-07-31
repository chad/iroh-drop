//! The tokio side of the app: owns the daemon connection, updates shared state,
//! and asks egui to repaint.
//!
//! The UI thread never awaits anything. It pushes [`Cmd`]s and reads
//! [`UiState`], which is the only thing the two threads share.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh_drop_daemon::{
    connect, default_socket_path, Client, Envelope, Hello, Service, ServiceOptions,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// How long after asking for a drop's contents we treat its offers as already
/// consented to.
///
/// Pasting a link *is* consent for what is in that drop — prompting again would
/// be theatre, and worse, it trains people to click yes. But a drop you joined
/// last month suddenly pushing you a 4 GiB file is a different question, so the
/// grace is deliberately short.
pub const RECEIVE_GRACE: Duration = Duration::from_secs(60);

/// What the UI asks the daemon to do.
#[derive(Debug)]
pub enum Cmd {
    /// Share these paths as one new drop.
    Send(Vec<PathBuf>),
    /// Join a drop from a ticket or a link, and fetch everything in it.
    Receive(String),
    /// Answer a consent question.
    Answer {
        /// The question's correlation id.
        id: u64,
        /// Whether the human said yes.
        accept: bool,
    },
    /// Stop hosting a drop.
    Forget(String),
    /// Fetch an offer sitting in one of our groups. Asking is consent — the
    /// same rule as answering yes to a live question.
    Fetch {
        /// The drop the offer lives in.
        drop: String,
        /// The listing number offer.fetch expects.
        pick: String,
        /// Display name, for the log line.
        name: String,
    },
}

/// One thing being sent or received.
#[derive(Clone, Debug)]
pub struct Transfer {
    /// Display name, from untrusted offer metadata.
    pub name: String,
    /// Bytes transferred so far.
    pub done: u64,
    /// Advertised total, when known.
    pub total: Option<u64>,
    /// Whether it finished, successfully or not.
    pub finished: bool,
    /// Why it failed, if it did.
    pub failed: Option<String>,
    /// Where the bytes landed.
    pub saved_to: Vec<String>,
}

impl Transfer {
    /// Fraction complete, when the size is known.
    pub fn fraction(&self) -> Option<f32> {
        match self.total {
            Some(total) if total > 0 => Some((self.done as f32 / total as f32).clamp(0.0, 1.0)),
            _ => None,
        }
    }
}

/// A pending "shall I accept this?" question.
#[derive(Clone, Debug)]
pub struct Incoming {
    /// Correlation id to answer with.
    pub id: u64,
    /// Offered name. Untrusted; always render it quoted.
    pub name: String,
    /// Human-readable size.
    pub size: String,
    /// Shortened sender id.
    pub from: String,
    /// When the daemon stops accepting an answer. After this the card must go:
    /// an Accept button that cannot work is worse than no button.
    pub expires_at: Instant,
}

/// One drop this daemon is hosting.
#[derive(Clone, Debug)]
pub struct DropRow {
    /// Daemon-local handle, e.g. `d1`.
    pub handle: String,
    /// Display name.
    pub name: String,
    /// How many items are offered.
    pub files: u64,
    /// How many peers are connected.
    pub peers: u64,
    /// True when we created the drop; false for a group we joined.
    pub mine: bool,
}

/// A file offered in one of our groups that we have not fetched. Membership
/// is sticky, so this row is too: it stays, fetchable, until fetched or
/// until we leave the group.
#[derive(Clone, Debug)]
pub struct AvailableRow {
    /// The drop the offer lives in.
    pub drop: String,
    /// The group's display name.
    pub group: String,
    /// The listing number offer.fetch expects.
    pub pick: String,
    /// Display name, from untrusted offer metadata.
    pub name: String,
    /// Human-readable size.
    pub size: String,
}

/// Everything the window renders.
#[derive(Default)]
pub struct UiState {
    /// Whether the daemon connection is up.
    pub connected: bool,
    /// Whether the daemon refuses relays and public lookup.
    pub lan_only: bool,
    /// Our own stable endpoint id.
    pub endpoint_id: String,
    /// Where accepted files land.
    pub download_dir: String,
    /// The ticket for the most recent send, ready to hand out.
    pub share_link: Option<String>,
    /// A short label while something is in flight.
    pub busy: Option<String>,
    /// The last thing that went wrong, in plain language.
    pub error: Option<String>,
    /// Consent questions awaiting an answer.
    pub incoming: Vec<Incoming>,
    /// Transfers, oldest first.
    pub transfers: Vec<Transfer>,
    /// Drops this daemon hosts.
    pub drops: Vec<DropRow>,
    /// Offered in our groups, not yet fetched.
    pub available: Vec<AvailableRow>,
    /// Human-readable notes.
    pub log: Vec<String>,
}

impl UiState {
    fn note(&mut self, line: impl Into<String>) {
        self.log.push(line.into());
        if self.log.len() > 200 {
            self.log.remove(0);
        }
    }
}

/// Handle the UI holds.
pub struct Bridge {
    /// Shared state the window renders.
    pub state: Arc<Mutex<UiState>>,
    tx: mpsc::UnboundedSender<Cmd>,
}

impl Bridge {
    /// Send a command; ignore failure, since a dead worker already shows as
    /// disconnected.
    pub fn send(&self, cmd: Cmd) {
        let _ = self.tx.send(cmd);
    }
}

/// Start the worker thread and return the UI's handle to it.
///
/// `socket` overrides the daemon path. When no daemon is reachable, the app runs
/// one in-process — so a user who never installs a background service still has
/// a working app, they just stop serving when they close the window.
pub fn spawn(egui_ctx: egui::Context, socket: Option<PathBuf>, lan_only: bool) -> Bridge {
    let state = Arc::new(Mutex::new(UiState::default()));
    let (tx, rx) = mpsc::unbounded_channel();
    let worker_state = Arc::clone(&state);

    std::thread::Builder::new()
        .name("iroh-drop-worker".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(e) => {
                    let mut state = worker_state.lock().expect("state");
                    state.error = Some(format!("cannot start the network runtime: {e}"));
                    egui_ctx.request_repaint();
                    return;
                }
            };
            runtime.block_on(run(worker_state, egui_ctx, rx, socket, lan_only));
        })
        .expect("spawn worker");

    Bridge { state, tx }
}

async fn run(
    state: Arc<Mutex<UiState>>,
    ctx: egui::Context,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    socket: Option<PathBuf>,
    lan_only: bool,
) {
    // Keep the embedded service alive if we had to start one.
    let mut _embedded: Option<Arc<Service>> = None;

    // Three tiers, most useful first: an already-running helper; a helper we
    // start ourselves (so files outlive this window); hosting in-process.
    let mut started_helper = false;
    let client = match attach(&socket).await {
        Ok(client) => client,
        Err(_) if start_helper(&socket, lan_only).await => {
            started_helper = true;
            match attach(&socket).await {
                Ok(client) => client,
                Err(e) => {
                    let mut state = state.lock().expect("state");
                    state.error = Some(format!("the background helper did not answer: {e}"));
                    ctx.request_repaint();
                    return;
                }
            }
        }
        Err(_) => match embedded(lan_only).await {
            Ok((service, client)) => {
                _embedded = Some(service);
                {
                    let mut state = state.lock().expect("state");
                    state.note(
                        "No background daemon found, so this window is hosting. \
                         Files stop being available when you close it.",
                    );
                }
                client
            }
            Err(e) => {
                let mut state = state.lock().expect("state");
                state.error = Some(format!("cannot start: {e}"));
                ctx.request_repaint();
                return;
            }
        },
    };
    let client = Arc::new(client);

    {
        let mut state = state.lock().expect("state");
        if started_helper {
            state.note("Started the background helper, so shared files stay available.");
        }
        state.connected = true;
        state.endpoint_id = client.hello["endpoint_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
    }
    refresh_status(&client, &state).await;
    ctx.request_repaint();

    let mut events = client.events();
    let mut asks = client.asks();
    // hash → display name, so progress events can be labelled.
    let mut names: HashMap<String, String> = HashMap::new();
    // Drops the user explicitly asked to receive, and when.
    let mut requested: HashMap<String, Instant> = HashMap::new();

    // Cards must vanish when they expire, even with no traffic to trigger it.
    let mut prune = tokio::time::interval(Duration::from_secs(1));
    prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = prune.tick() => {
                let mut state = state.lock().expect("state");
                let before = state.incoming.len();
                let now = Instant::now();
                state.incoming.retain(|i| i.expires_at > now);
                if state.incoming.len() != before {
                    state.note("An offer expired before anyone answered it.");
                    drop(state);
                    ctx.request_repaint();
                }
            }
            cmd = rx.recv() => {
                match cmd {
                    Some(cmd) => handle_cmd(&client, &state, &ctx, cmd, &mut requested).await,
                    None => break,
                }
            }
            ask = asks.recv() => {
                if let Ok(ask) = ask {
                    // Did the user ask for this drop a moment ago? Then they
                    // have already consented, and we must not fetch it twice by
                    // also downloading it ourselves — answering the question is
                    // the *only* thing that starts the transfer.
                    let handle = str_of(&ask.p, "drop");
                    let solicited = requested
                        .get(&handle)
                        .is_some_and(|at| at.elapsed() < RECEIVE_GRACE);

                    if solicited {
                        client.answer(ask.id, Some(json!({"accept": true}))).await;
                    } else {
                        // Trust the daemon's own deadline rather than assuming
                        // one, minus a slice for the round trip.
                        let ttl = ask.p["expires_in_ms"]
                            .as_u64()
                            .map(Duration::from_millis)
                            .unwrap_or(Duration::from_secs(60))
                            .saturating_sub(Duration::from_secs(2));
                        let mut state = state.lock().expect("state");
                        state.incoming.push(Incoming {
                            id: ask.id,
                            // Untrusted display metadata: shown quoted, never
                            // interpreted as a path.
                            name: str_of(&ask.p, "name"),
                            size: str_of(&ask.p, "human_size"),
                            from: str_of(&ask.p, "from").chars().take(10).collect(),
                            expires_at: Instant::now() + ttl,
                        });
                        drop(state);
                        ctx.request_repaint();
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(env) => {
                        apply_event(&state, &mut names, &env);
                        ctx.request_repaint();
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let mut state = state.lock().expect("state");
                        state.connected = false;
                        state.error = Some("the daemon went away".into());
                        ctx.request_repaint();
                        break;
                    }
                }
            }
        }
    }
}

async fn attach(socket: &Option<PathBuf>) -> Result<Client, iroh_drop_daemon::ApiError> {
    let path = socket.clone().unwrap_or_else(default_socket_path);
    // A UI, so the daemon routes consent questions here. No blocking handler:
    // we answer through `Client::answer` when the human clicks.
    connect(&path, Hello::ui("iroh-drop-app"), None).await
}

/// Launch `iroh-dropd` from beside our own executable and wait for its socket.
///
/// Inside an `.app` the helper sits in the same `Contents/MacOS` directory, so
/// "next to the executable" is both where it is and the only place we are
/// willing to look — searching `PATH` would let anything named `iroh-dropd`
/// inherit our user's files.
///
/// The child is spawned with no stdio and is not waited on. A GUI launched from
/// Finder has no controlling terminal, so nothing sends it `SIGHUP` when this
/// process exits: that is the point, since the helper is what keeps shared files
/// reachable after the window closes.
async fn start_helper(socket: &Option<PathBuf>, lan_only: bool) -> bool {
    let Ok(own) = std::env::current_exe() else {
        return false;
    };
    let helper = own.with_file_name(if cfg!(windows) {
        "iroh-dropd.exe"
    } else {
        "iroh-dropd"
    });
    if !helper.is_file() {
        tracing::debug!("no bundled helper at {}", helper.display());
        return false;
    }

    let mut command = std::process::Command::new(&helper);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Spawning a console binary from a windowed app would otherwise pop a
    // console window next to it. The helper stays a console exe so it is
    // usable standalone; only the GUI's spawn hides the window.
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    if lan_only {
        command.arg("--lan-only");
    }
    if let Some(socket) = socket {
        command.arg("--socket").arg(socket);
    }
    if command.spawn().is_err() {
        return false;
    }
    tracing::info!("started background helper {}", helper.display());

    // It has to bind a socket and open an endpoint; poll rather than guess.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if attach(socket).await.is_ok() {
            return true;
        }
    }
    false
}

async fn embedded(lan_only: bool) -> Result<(Arc<Service>, Client), iroh_drop_daemon::ApiError> {
    let downloads = dirs_download();
    std::fs::create_dir_all(&downloads).ok();
    let service = Service::new(ServiceOptions {
        store_path: None,
        identity_path: None,
        offline: lan_only,
        mdns: true,
        download_dir: downloads,
        auto_accept: false,
        link_base: None,
    })
    .await?;
    let client = Client::connect_memory(&service, Hello::ui("iroh-drop-app"), None).await?;
    Ok((service, client))
}

fn dirs_download() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let downloads = PathBuf::from(&home).join("Downloads");
        if downloads.is_dir() {
            return downloads.join("iroh-drop");
        }
    }
    PathBuf::from("iroh-drop-downloads")
}

async fn handle_cmd(
    client: &Arc<Client>,
    state: &Arc<Mutex<UiState>>,
    ctx: &egui::Context,
    cmd: Cmd,
    requested: &mut HashMap<String, Instant>,
) {
    match cmd {
        Cmd::Answer { id, accept } => {
            client
                .answer(id, accept.then(|| json!({"accept": true})))
                .await;
            let mut state = state.lock().expect("state");
            state.incoming.retain(|i| i.id != id);
            if !accept {
                state.note("Declined.");
            }
        }
        Cmd::Send(paths) => {
            {
                let mut state = state.lock().expect("state");
                state.busy = Some("Preparing…".into());
                state.error = None;
                state.share_link = None;
            }
            ctx.request_repaint();
            let result = do_send(client, &paths).await;
            let mut state = state.lock().expect("state");
            state.busy = None;
            match result {
                Ok(link) => {
                    state.note(format!("Ready to share {} item(s).", paths.len()));
                    state.share_link = Some(link);
                }
                Err(e) => state.error = Some(e),
            }
        }
        Cmd::Receive(input) => {
            {
                let mut state = state.lock().expect("state");
                state.busy = Some("Connecting…".into());
                state.error = None;
            }
            ctx.request_repaint();
            let result = do_receive(client, &input).await;
            let mut state = state.lock().expect("state");
            state.busy = None;
            match result {
                Ok(handle) => {
                    requested.insert(handle, Instant::now());
                }
                Err(e) => state.error = Some(e),
            }
        }
        Cmd::Forget(handle) => {
            let _ = client.call("drop.leave", json!({"drop": handle})).await;
        }
        Cmd::Fetch { drop, pick, name } => {
            state
                .lock()
                .expect("state")
                .note(format!("getting {name}"));
            if let Err(e) = client
                .call("offer.fetch", json!({"drop": drop, "pick": pick}))
                .await
            {
                state.lock().expect("state").error = Some(e.msg);
            }
        }
    }
    refresh_status(client, state).await;
    ctx.request_repaint();
}

async fn do_send(client: &Arc<Client>, paths: &[PathBuf]) -> Result<String, String> {
    let created = client
        .call("drop.create", json!({"name": send_label(paths)}))
        .await
        .map_err(|e| e.msg)?;
    let handle = created["drop"].clone();

    for path in paths {
        let path = path.to_str().ok_or("that path is not valid text")?;
        client
            .call("offer.publish", json!({"drop": handle, "path": path}))
            .await
            .map_err(|e| e.msg)?;
    }

    let ticket = client
        .call("drop.ticket", json!({"drop": handle}))
        .await
        .map_err(|e| e.msg)?;
    // The link, never the raw ticket: nobody should have to know that word.
    Ok(str_of(&ticket, "link"))
}

fn send_label(paths: &[PathBuf]) -> String {
    match paths.first().and_then(|p| p.file_name()).map(|n| n.to_string_lossy().to_string()) {
        Some(first) if paths.len() > 1 => format!("{first} +{}", paths.len() - 1),
        Some(first) => first,
        None => "files".into(),
    }
}

/// Join a drop and return its handle.
///
/// Note what this does **not** do: fetch. Every offer already produces a consent
/// question, and the answer to that question is what starts the transfer. An
/// explicit fetch here would download everything a second time and, far worse,
/// would ignore a user who answered "no".
async fn do_receive(client: &Arc<Client>, input: &str) -> Result<String, String> {
    let ticket = extract_ticket(input).ok_or("that does not look like an iroh-drop link")?;
    let joined = client
        .call("drop.join", json!({"ticket": ticket}))
        .await
        .map_err(|e| e.msg)?;
    let handle = str_of(&joined, "drop");

    // Wait only so we can say "there is nothing there" instead of nothing at
    // all. Contents arrive by catch-up sync or a live announcement.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let listed = client
            .call("offer.list", json!({"drop": handle}))
            .await
            .map_err(|e| e.msg)?;
        if !listed["items"]
            .as_array()
            .map(|items| items.is_empty())
            .unwrap_or(true)
        {
            return Ok(handle);
        }
        if Instant::now() > deadline {
            return Err("nobody offered anything in that drop".into());
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

/// Accept a bare ticket, a `drop1…` inside other text, or the fragment link.
pub fn extract_ticket(input: &str) -> Option<String> {
    let input = input.trim();
    let start = input.find("drop1")?;
    let ticket: String = input[start..]
        .chars()
        .take_while(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        .collect();
    // Base32 with no padding; anything this short is a typo, not a ticket.
    (ticket.len() > 32).then_some(ticket)
}

async fn refresh_status(client: &Arc<Client>, state: &Arc<Mutex<UiState>>) {
    let status = client.call("daemon.status", json!({})).await.ok();
    let listed = client.call("drop.list", json!({})).await.ok();
    // What is on offer in our groups that we do not have yet. The durable
    // half of "you see anything offered until you leave": a consent card
    // that timed out is still here with a Get button. The guard ends with
    // this block — never held across the offer.list round-trips below.
    let handles: Vec<(String, String)> = {
        let mut guard = state.lock().expect("state");
        if let Some(status) = status {
            guard.lan_only = status["offline"].as_bool().unwrap_or(false);
            guard.download_dir = str_of(&status, "download_dir");
        }
        if let Some(listed) = listed {
            guard.drops = listed["drops"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|d| DropRow {
                    handle: str_of(d, "drop"),
                    name: d["name"].as_str().unwrap_or("files").to_string(),
                    files: d["offers"].as_u64().unwrap_or(0),
                    peers: d["peers"].as_u64().unwrap_or(0),
                    mine: d["mine"].as_bool().unwrap_or(true),
                })
                .collect::<Vec<_>>();
        }
        guard
            .drops
            .iter()
            .map(|d| (d.handle.clone(), d.name.clone()))
            .collect()
    };
    let mut available = Vec::new();
    for (handle, group) in handles {
        if let Ok(listed) = client.call("offer.list", json!({"drop": handle})).await {
            for item in listed["items"].as_array().cloned().unwrap_or_default() {
                if item["status"].as_str() == Some("missing") {
                    available.push(AvailableRow {
                        drop: handle.clone(),
                        group: group.clone(),
                        pick: item["n"].as_u64().unwrap_or(0).to_string(),
                        name: str_of(&item, "name"),
                        size: str_of(&item, "human_size"),
                    });
                }
            }
        }
    }
    state.lock().expect("state").available = available;
}

fn apply_event(state: &Arc<Mutex<UiState>>, names: &mut HashMap<String, String>, env: &Envelope) {
    let mut state = state.lock().expect("state");
    let hash = str_of(&env.p, "hash");
    match env.e.as_str() {
        "offer.received" => {
            names.insert(hash, str_of(&env.p, "name"));
        }
        "fetch.progress" => {
            let name = names.get(&hash).cloned().unwrap_or_else(|| "file".into());
            let done = env.p["downloaded"].as_u64().unwrap_or(0);
            let total = env.p["total"].as_u64();
            match state.transfers.iter_mut().find(|t| t.name == name && !t.finished) {
                Some(existing) => {
                    existing.done = done;
                    existing.total = total;
                }
                None => state.transfers.push(Transfer {
                    name,
                    done,
                    total,
                    finished: false,
                    failed: None,
                    saved_to: Vec::new(),
                }),
            }
        }
        "fetch.materialized" => {
            let paths: Vec<String> = env.p["paths"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .iter()
                .filter_map(|p| p.as_str().map(str::to_string))
                .collect();
            let name = names.get(&hash).cloned().unwrap_or_else(|| "file".into());
            match state.transfers.iter_mut().find(|t| t.name == name && !t.finished) {
                Some(existing) => {
                    existing.finished = true;
                    existing.saved_to = paths;
                }
                None => state.transfers.push(Transfer {
                    name,
                    done: 0,
                    total: None,
                    finished: true,
                    failed: None,
                    saved_to: paths,
                }),
            }
            state.note("Saved.");
        }
        "fetch.failed" => {
            let name = names.get(&hash).cloned().unwrap_or_else(|| "file".into());
            let error = str_of(&env.p, "error");
            match state.transfers.iter_mut().find(|t| t.name == name && !t.finished) {
                Some(existing) => {
                    existing.finished = true;
                    existing.failed = Some(error);
                }
                None => state.note(format!("Could not get {name}: {error}")),
            }
        }
        "offer.declined" => state.note("Declined an offer."),
        "peer.joined" => state.note("Someone connected."),
        _ => {}
    }
}

fn str_of(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::extract_ticket;

    const TICKET: &str = "drop1aimfofis3yfxv6oqyama7hct5hgil7ozmrwh4u7ryd6ihbtadeesuaitapjhbfsnoh";

    #[test]
    fn accepts_a_bare_ticket() {
        assert_eq!(extract_ticket(TICKET).as_deref(), Some(TICKET));
    }

    #[test]
    fn accepts_the_fragment_link() {
        let link = format!("https://drop.example/#{TICKET}");
        assert_eq!(extract_ticket(&link).as_deref(), Some(TICKET));
    }

    #[test]
    fn survives_being_pasted_out_of_a_chat_app() {
        // What a paste actually looks like: quotes, chatter, a trailing newline.
        let pasted = format!("  Chad: here you go — {TICKET}\n");
        assert_eq!(extract_ticket(&pasted).as_deref(), Some(TICKET));
    }

    #[test]
    fn stops_at_punctuation_so_a_trailing_period_is_not_swallowed() {
        let sentence = format!("{TICKET}. thanks!");
        assert_eq!(extract_ticket(&sentence).as_deref(), Some(TICKET));
    }

    #[test]
    fn rejects_things_that_are_not_tickets() {
        assert!(extract_ticket("").is_none());
        assert!(extract_ticket("hello").is_none());
        // A prefix alone is a typo, not a capability.
        assert!(extract_ticket("drop1").is_none());
        assert!(extract_ticket("drop1abc").is_none());
    }
}
