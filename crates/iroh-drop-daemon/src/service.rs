//! The daemon: one endpoint, N sessions, many clients.
//!
//! The service owns lifetime. Clients come and go; transfers do not stop when a
//! window closes, and content you received keeps being served — which is the
//! only way the replication design does anything in practice.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh_drop::{
    DropBuilder, DropEvent, DropPolicy, DropProtocol, DropSession, DropTicket, LocalBlobStatus,
    StackOptions,
};
use iroh_drop_sdk::collections::publish_path;
use iroh_drop_sdk::inventory::{human_bytes, inventory, resolve_pick};
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, warn};

use crate::frame::{Envelope, Frame, Hello, Role, API_VERSION};

/// How long a consent question waits before defaulting to deny.
///
/// Long enough that stepping away from the desk does not lose a transfer, short
/// enough that a forgotten prompt cannot be clicked days later by somebody else
/// at the same machine. The deadline is published to UIs as `expires_in_ms` so
/// a card can disappear exactly when it stops being answerable — a button that
/// silently does nothing is worse than no button.
pub const CONSENT_TIMEOUT: Duration = Duration::from_secs(300);

/// How many events the replay ring holds.
pub const EVENT_RING: usize = 4096;

/// Minimum gap between `fetch.progress` events for the same blob.
///
/// A large transfer produces progress callbacks per chunk; a UI needs about ten
/// a second. Without this a 40 000-chunk fetch emits 40 000 events, which costs
/// more than the transfer.
pub const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// A method or question failure.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{code}: {msg}")]
pub struct ApiError {
    /// Stable machine-readable code.
    pub code: String,
    /// Human-readable detail.
    pub msg: String,
}

impl ApiError {
    /// Build an error with an explicit code.
    pub fn new(code: &str, msg: impl std::fmt::Display) -> Self {
        Self {
            code: code.into(),
            msg: msg.to_string(),
        }
    }
    fn unsupported(what: impl std::fmt::Display) -> Self {
        Self::new("unsupported", what)
    }
    fn bad_params(msg: impl std::fmt::Display) -> Self {
        Self::new("bad_params", msg)
    }
    fn not_found(msg: impl std::fmt::Display) -> Self {
        Self::new("not_found", msg)
    }
    fn internal(msg: impl std::fmt::Display) -> Self {
        Self::new("internal", msg)
    }
}

type ApiResult = Result<Value, ApiError>;

/// How to bring up the daemon.
#[derive(Clone, Debug)]
pub struct ServiceOptions {
    /// Persistent blob store. `None` means in-memory (nothing survives).
    pub store_path: Option<PathBuf>,
    /// Persistent identity, so peers can recognise you across restarts.
    pub identity_path: Option<PathBuf>,
    /// No relays, no DNS, no pkarr — the most decentralized posture.
    pub offline: bool,
    /// Announce and resolve peers on the local network.
    pub mdns: bool,
    /// Where accepted transfers land by default.
    pub download_dir: PathBuf,
    /// Skip the consent question entirely. Off by default, and a bad idea
    /// anywhere but a test.
    pub auto_accept: bool,
    /// Base URL of a static page that can hand a ticket to the app, e.g.
    /// `https://drop.example`. When set, `drop.ticket` also returns a plain
    /// `https` link with the ticket in the **fragment**, which browsers never
    /// send to the server — so the page can be a dumb static file that learns
    /// nothing and holds no database.
    ///
    /// `None` means the only link is the `iroh-drop://` scheme, which works
    /// without any infrastructure but only for people who have the app.
    pub link_base: Option<String>,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            store_path: None,
            identity_path: None,
            offline: false,
            mdns: true,
            download_dir: PathBuf::from("."),
            auto_accept: false,
            link_base: None,
        }
    }
}

/// URL scheme the desktop app registers, so a link opens it directly.
pub const LINK_SCHEME: &str = "iroh-drop";

/// Turn a ticket into something a person can send in a chat app.
///
/// Nobody should ever have to see, retype, or know the word for a ticket. This
/// is the only form the UI shows.
pub fn app_link(ticket: &str) -> String {
    format!("{LINK_SCHEME}://receive/{ticket}")
}

/// The `https` form, when a base URL is configured. The ticket goes in the
/// fragment so it never reaches the server.
pub fn web_link(base: &str, ticket: &str) -> String {
    format!("{}/#{ticket}", base.trim_end_matches('/'))
}

struct DropEntry {
    session: Arc<DropSession>,
    name: Option<String>,
}

struct TaskEntry {
    kind: String,
    drop: String,
    hash: Option<String>,
    state: String,
    abort: Option<tokio::task::AbortHandle>,
}

struct EventBus {
    tx: broadcast::Sender<Envelope>,
    seq: AtomicU64,
    ring: Mutex<VecDeque<Envelope>>,
}

impl EventBus {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            tx,
            seq: AtomicU64::new(1),
            ring: Mutex::new(VecDeque::with_capacity(EVENT_RING)),
        }
    }

    fn emit(&self, e: &str, p: Value) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let env = Envelope {
            seq,
            e: e.to_string(),
            p,
        };
        {
            let mut ring = self.ring.lock().expect("event ring");
            if ring.len() == EVENT_RING {
                ring.pop_front();
            }
            ring.push_back(env.clone());
        }
        // No subscribers is normal: the daemon runs headless.
        let _ = self.tx.send(env);
        seq
    }

    fn current(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Events from `from` onward, plus whether we had already evicted some of
    /// what was asked for. Sequence numbers start at 1, so `from` of 0 or 1
    /// both mean "everything you still have".
    fn replay(&self, from: u64) -> (Vec<Envelope>, bool) {
        let from = from.max(1);
        let ring = self.ring.lock().expect("event ring");
        let oldest = ring.front().map(|e| e.seq).unwrap_or(from);
        let truncated = oldest > from;
        let events = ring.iter().filter(|e| e.seq >= from).cloned().collect();
        (events, truncated)
    }
}

/// Routes `Ask` frames to UI clients and matches their answers.
struct AskRouter {
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, ApiError>>>>,
    /// Registered UIs, oldest first, each with a token so a disconnecting
    /// connection can remove *itself*.
    ///
    /// Identity matters here. `mpsc::Sender::is_closed` only reports a dropped
    /// receiver, and a dead client's writer task may still be parked on `recv`
    /// with the socket already gone — so a channel can look perfectly healthy
    /// while nothing is listening. Handing a question to that ghost loses it
    /// silently, and the offer sits unanswered until it times out.
    uis: Mutex<Vec<(u64, mpsc::Sender<Frame>)>>,
    next_ui: AtomicU64,
}

impl AskRouter {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
            uis: Mutex::new(Vec::new()),
            next_ui: AtomicU64::new(1),
        }
    }

    fn register_ui(&self, tx: mpsc::Sender<Frame>) -> u64 {
        let token = self.next_ui.fetch_add(1, Ordering::SeqCst);
        self.uis.lock().expect("uis").push((token, tx));
        token
    }

    fn unregister_ui(&self, token: u64) {
        self.uis
            .lock()
            .expect("uis")
            .retain(|(candidate, _)| *candidate != token);
    }

    fn forget_closed(&self) {
        self.uis.lock().expect("uis").retain(|(_, tx)| !tx.is_closed());
    }

    /// Is there a UI we could actually ask right now?
    fn has_live_ui(&self) -> bool {
        self.uis.lock().expect("uis").iter().any(|(_, tx)| !tx.is_closed())
    }

    fn answer(&self, id: u64, result: Result<Value, ApiError>) {
        if let Some(tx) = self.pending.lock().expect("pending").remove(&id) {
            let _ = tx.send(result);
        }
    }

    /// Ask the first live UI client a question.
    ///
    /// Returns `None` when nobody is listening, nobody answers in time, or the
    /// client reports an error. Every one of those means **deny**: silence is
    /// never consent.
    async fn ask(&self, q: &str, p: Value, timeout: Duration) -> Option<Value> {
        self.forget_closed();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();

        // The most recently attached UI wins: that is the window the person is
        // actually looking at, and it means restarting the app takes over asking
        // rather than competing with whatever came before.
        let target = {
            let uis = self.uis.lock().expect("uis");
            uis.iter().rev().find(|(_, tx)| !tx.is_closed()).map(|(_, tx)| tx.clone())
        };
        let target = match target {
            Some(t) => t,
            None => {
                debug!("no ui client attached; denying {q}");
                return None;
            }
        };

        self.pending.lock().expect("pending").insert(id, tx);
        let frame = Frame::Ask {
            id,
            q: q.to_string(),
            p,
        };
        if target.send(frame).await.is_err() {
            self.pending.lock().expect("pending").remove(&id);
            return None;
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(value))) => Some(value),
            _ => {
                self.pending.lock().expect("pending").remove(&id);
                debug!("{q} was declined, errored, or timed out");
                None
            }
        }
    }
}

/// The daemon.
pub struct Service {
    protocol: DropProtocol,
    options: ServiceOptions,
    drops: Mutex<HashMap<String, DropEntry>>,
    next_drop: AtomicU64,
    tasks: Mutex<HashMap<String, TaskEntry>>,
    next_task: AtomicU64,
    bus: EventBus,
    asks: AskRouter,
    /// Last time we emitted progress for a blob, for coalescing.
    progress_seen: Mutex<HashMap<String, Instant>>,
}

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("endpoint_id", &self.endpoint_id())
            .field("drops", &self.drops.lock().map(|d| d.len()).unwrap_or(0))
            .finish_non_exhaustive()
    }
}

impl Service {
    /// Bring up the endpoint, the store, and an empty session table.
    pub async fn new(options: ServiceOptions) -> Result<Arc<Self>, ApiError> {
        // Auto-fetch stays off: an offer must never consume disk before a
        // human (or an explicit policy) says yes. Consent happens above.
        let policy = DropPolicy {
            auto_fetch: false,
            output_directory: options.download_dir.clone(),
            ..Default::default()
        };
        let protocol = DropBuilder::from_options(StackOptions {
            store_path: options.store_path.clone(),
            identity_path: options.identity_path.clone(),
            offline: options.offline,
            mdns: options.mdns,
        })
        .await
        .map_err(ApiError::internal)?
        .policy(policy)
        .build()
        .await
        .map_err(ApiError::internal)?;

        Ok(Arc::new(Self {
            protocol,
            options,
            drops: Mutex::new(HashMap::new()),
            next_drop: AtomicU64::new(1),
            tasks: Mutex::new(HashMap::new()),
            next_task: AtomicU64::new(1),
            bus: EventBus::new(),
            asks: AskRouter::new(),
            progress_seen: Mutex::new(HashMap::new()),
        }))
    }

    /// This daemon's stable endpoint id.
    pub fn endpoint_id(&self) -> String {
        self.protocol.stack().addr().id.to_string()
    }

    /// Every method this build supports, for capability discovery.
    pub fn methods() -> &'static [&'static str] {
        &[
            "hello",
            "daemon.status",
            "drop.create",
            "drop.join",
            "drop.list",
            "drop.ticket",
            "drop.leave",
            "offer.list",
            "offer.publish",
            "offer.fetch",
            "task.list",
            "task.cancel",
            "events.replay",
        ]
    }

    // ── client connections ────────────────────────────────────────────────

    /// Serve one client connection. The channels carry frames; swapping them
    /// for JSONL over a Unix socket changes nothing above this line.
    pub fn attach(
        self: &Arc<Self>,
        mut inbound: mpsc::Receiver<Frame>,
        outbound: mpsc::Sender<Frame>,
    ) -> tokio::task::JoinHandle<()> {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            // A connection is not a client until it says hello.
            let hello = match inbound.recv().await {
                Some(Frame::Req { id, m, p }) if m == "hello" => {
                    match serde_json::from_value::<Hello>(p) {
                        Ok(hello) if hello.api == API_VERSION => {
                            let res = json!({
                                "api": API_VERSION,
                                "daemon": env!("CARGO_PKG_VERSION"),
                                "wire": iroh_drop::WIRE_VERSION,
                                "endpoint_id": service.endpoint_id(),
                                "methods": Self::methods(),
                                "events_from": service.bus.current(),
                            });
                            let _ = outbound.send(Frame::Res { id, p: res }).await;
                            hello
                        }
                        Ok(hello) => {
                            let _ = outbound
                                .send(Frame::Err {
                                    id,
                                    code: "api_version".into(),
                                    msg: format!(
                                        "daemon speaks api {API_VERSION}, client speaks {}",
                                        hello.api
                                    ),
                                })
                                .await;
                            return;
                        }
                        Err(e) => {
                            let _ = outbound
                                .send(Frame::Err {
                                    id,
                                    code: "bad_params".into(),
                                    msg: e.to_string(),
                                })
                                .await;
                            return;
                        }
                    }
                }
                _ => {
                    warn!("client did not say hello first; closing");
                    return;
                }
            };

            let ui_token = if hello.roles.contains(&Role::Ui) {
                Some(service.asks.register_ui(outbound.clone()))
            } else {
                None
            };

            // Fan events out to this client until it goes away.
            let mut events = service.bus.tx.subscribe();
            let ev_out = outbound.clone();
            let forwarder = tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(env) => {
                            if ev_out.send(env.to_frame()).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            // Tell the client it missed things rather than
                            // applying backpressure to the session loops.
                            let frame = Frame::Ev {
                                seq: 0,
                                e: "events.truncated".into(),
                                p: json!({"missed": n}),
                            };
                            if ev_out.send(frame).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });

            while let Some(frame) = inbound.recv().await {
                match frame {
                    Frame::Req { id, m, p } => {
                        let reply = match service.dispatch(&m, p).await {
                            Ok(p) => Frame::Res { id, p },
                            Err(e) => Frame::Err {
                                id,
                                code: e.code,
                                msg: e.msg,
                            },
                        };
                        if outbound.send(reply).await.is_err() {
                            break;
                        }
                    }
                    // Answers to our own questions.
                    Frame::Res { id, p } => service.asks.answer(id, Ok(p)),
                    Frame::Err { id, code, msg } => {
                        service.asks.answer(id, Err(ApiError { code, msg }))
                    }
                    other => debug!("ignoring unexpected frame from client: {other:?}"),
                }
            }

            forwarder.abort();
            // Deregister by identity, not by guessing from channel state.
            if let Some(token) = ui_token {
                service.asks.unregister_ui(token);
            }
            service.asks.forget_closed();
        })
    }

    // ── method dispatch ───────────────────────────────────────────────────

    /// Run one method. Unknown names get a clean `unsupported`, never a
    /// dropped connection — the same courtesy the wire protocol's control
    /// channel extends to unknown ops.
    pub async fn dispatch(self: &Arc<Self>, method: &str, params: Value) -> ApiResult {
        match method {
            "hello" => Err(ApiError::new("already_hello", "hello was already sent")),
            "daemon.status" => self.daemon_status(),
            "drop.create" => self.drop_create(params).await,
            "drop.join" => self.drop_join(params).await,
            "drop.list" => self.drop_list(),
            "drop.ticket" => self.drop_ticket(params),
            "drop.leave" => self.drop_leave(params).await,
            "offer.list" => self.offer_list(params),
            "offer.publish" => self.offer_publish(params),
            "offer.fetch" => self.offer_fetch(params),
            "task.list" => self.task_list(),
            "task.cancel" => self.task_cancel(params),
            "events.replay" => self.events_replay(params),
            other => Err(ApiError::unsupported(format!("no method {other}"))),
        }
    }

    fn daemon_status(&self) -> ApiResult {
        let drops = self.drops.lock().expect("drops");
        Ok(json!({
            "endpoint_id": self.endpoint_id(),
            "offline": self.options.offline,
            "mdns": self.options.mdns,
            "persistent_store": self.options.store_path.is_some(),
            "download_dir": self.options.download_dir,
            "drops": drops.len(),
            "seq": self.bus.current(),
        }))
    }

    async fn drop_create(self: &Arc<Self>, params: Value) -> ApiResult {
        let name = params.get("name").and_then(Value::as_str).map(str::to_string);
        let session = self
            .protocol
            .create(iroh_drop::CreateOptions {
                display_name: name.clone(),
                auto_fetch_recommended: false,
            })
            .await
            .map_err(ApiError::internal)?;
        let handle = self.register(session, name);
        let entry = self.entry(&handle)?;
        Ok(json!({
            "drop": handle,
            "topic": entry.session.topic_id().to_string(),
            "ticket": entry.session.short_ticket().to_string(),
        }))
    }

    async fn drop_join(self: &Arc<Self>, params: Value) -> ApiResult {
        let ticket = params
            .get("ticket")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_params("ticket is required"))?;
        let ticket: DropTicket = ticket
            .parse()
            .map_err(|e| ApiError::bad_params(format!("bad ticket: {e}")))?;
        let session = self
            .protocol
            .join(ticket)
            .await
            .map_err(ApiError::internal)?;
        let handle = self.register(session, None);
        let entry = self.entry(&handle)?;
        Ok(json!({
            "drop": handle,
            "topic": entry.session.topic_id().to_string(),
        }))
    }

    fn drop_list(&self) -> ApiResult {
        let drops = self.drops.lock().expect("drops");
        let mut list: Vec<Value> = drops
            .iter()
            .map(|(handle, entry)| {
                let items = inventory(&entry.session);
                // `offers` is how many things were announced; `files` is how many
                // a person would count. A folder is one offer and many files, so
                // reporting only the former makes a 400-photo album read as "1
                // file".
                let files: usize = items.iter().map(|item| item.members.unwrap_or(1)).sum();
                let bytes: u64 = items.iter().map(|item| item.content_size).sum();
                json!({
                    "drop": handle,
                    "name": entry.name,
                    "topic": entry.session.topic_id().to_string(),
                    "peers": entry.session.peers().len(),
                    "offers": items.len(),
                    "files": files,
                    "bytes": bytes,
                    "human_size": human_bytes(bytes),
                })
            })
            .collect();
        list.sort_by(|a, b| a["drop"].as_str().cmp(&b["drop"].as_str()));
        Ok(json!({"drops": list}))
    }

    fn drop_ticket(&self, params: Value) -> ApiResult {
        let entry = self.entry(self.handle_param(&params)?)?;
        let full = params
            .get("full")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let ticket = if full {
            entry.session.ticket()
        } else {
            entry.session.short_ticket()
        };
        let ticket = ticket.to_string();
        Ok(json!({
            // `link` is what a UI shows and a person sends. `ticket` is the
            // underlying capability, for tooling that needs it.
            "link": crate::service::app_link(&ticket),
            "web_link": self.options.link_base.as_deref().map(|base| web_link(base, &ticket)),
            "ticket": ticket,
        }))
    }

    async fn drop_leave(self: &Arc<Self>, params: Value) -> ApiResult {
        let handle = self.handle_param(&params)?.to_string();
        let entry = {
            let mut drops = self.drops.lock().expect("drops");
            drops
                .remove(&handle)
                .ok_or_else(|| ApiError::not_found(format!("no drop {handle}")))?
        };
        // Withdraw politely; a crash cannot, which is the case
        // `publisher_exit.rs` already covers.
        entry.session.shutdown_no_announce().await;
        self.bus.emit("drop.left", json!({"drop": handle}));
        Ok(json!({}))
    }

    fn offer_list(&self, params: Value) -> ApiResult {
        let entry = self.entry(self.handle_param(&params)?)?;
        let items: Vec<Value> = inventory(&entry.session)
            .into_iter()
            .map(|item| {
                json!({
                    "n": item.index,
                    "name": item.name,
                    "hash": item.hash.to_hex(),
                    "size": item.content_size,
                    "human_size": item.human_size(),
                    "kind": item.kind(),
                    "is_collection": item.is_collection,
                    "members": item.members,
                    "status": status_str(&item.status),
                })
            })
            .collect();
        Ok(json!({"items": items}))
    }

    fn offer_publish(self: &Arc<Self>, params: Value) -> ApiResult {
        let handle = self.handle_param(&params)?.to_string();
        let entry = self.entry(&handle)?;
        let path = params
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_params("path is required"))?
            .to_string();
        let name = params.get("name").and_then(Value::as_str).map(str::to_string);

        let session = Arc::clone(&entry.session);
        let service = Arc::clone(self);
        Ok(self.spawn_task("publish", &handle, None, move |task| async move {
            match publish_path(&session, &path, name).await {
                Ok(published) => {
                    service.bus.emit(
                        "publish.completed",
                        json!({
                            "task": task,
                            "hash": published.blob.hash.to_hex(),
                            "name": published.blob.name,
                            "size": published.total_size,
                            "human_size": human_bytes(published.total_size),
                            "members": published.members,
                            "is_collection": published.is_collection,
                        }),
                    );
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            }
        }))
    }

    fn offer_fetch(self: &Arc<Self>, params: Value) -> ApiResult {
        let handle = self.handle_param(&params)?.to_string();
        let entry = self.entry(&handle)?;
        let pick = params
            .get("pick")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_params("pick is required"))?;
        let hash = resolve_pick(&entry.session, pick)
            .map_err(|e| ApiError::not_found(e.to_string()))?;
        let out = params
            .get("out")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| self.options.download_dir.clone());

        Ok(self.spawn_fetch(&handle, Arc::clone(&entry.session), hash, out))
    }

    fn task_list(&self) -> ApiResult {
        let tasks = self.tasks.lock().expect("tasks");
        let mut list: Vec<Value> = tasks
            .iter()
            .map(|(id, t)| {
                json!({
                    "task": id, "kind": t.kind, "drop": t.drop,
                    "hash": t.hash, "state": t.state,
                })
            })
            .collect();
        list.sort_by(|a, b| a["task"].as_str().cmp(&b["task"].as_str()));
        Ok(json!({"tasks": list}))
    }

    fn task_cancel(&self, params: Value) -> ApiResult {
        let id = params
            .get("task")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_params("task is required"))?;
        let mut tasks = self.tasks.lock().expect("tasks");
        let entry = tasks
            .get_mut(id)
            .ok_or_else(|| ApiError::not_found(format!("no task {id}")))?;
        if let Some(abort) = entry.abort.take() {
            abort.abort();
        }
        entry.state = "cancelled".into();
        drop(tasks);
        self.bus
            .emit("task.state", json!({"task": id, "state": "cancelled"}));
        Ok(json!({}))
    }

    fn events_replay(&self, params: Value) -> ApiResult {
        let from = params.get("from").and_then(Value::as_u64).unwrap_or(0);
        let (events, truncated) = self.bus.replay(from);
        Ok(json!({"events": events, "truncated": truncated}))
    }

    // ── internals ─────────────────────────────────────────────────────────

    /// Whether enough time has passed to emit progress for this blob again.
    fn progress_due(&self, hash: &str) -> bool {
        let now = Instant::now();
        let mut seen = self.progress_seen.lock().expect("progress");
        match seen.get(hash) {
            Some(last) if now.duration_since(*last) < PROGRESS_INTERVAL => false,
            _ => {
                seen.insert(hash.to_string(), now);
                true
            }
        }
    }

    fn handle_param<'a>(&self, params: &'a Value) -> Result<&'a str, ApiError> {
        params
            .get("drop")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_params("drop is required"))
    }

    fn entry(&self, handle: &str) -> Result<DropRef, ApiError> {
        let drops = self.drops.lock().expect("drops");
        let entry = drops
            .get(handle)
            .ok_or_else(|| ApiError::not_found(format!("no drop {handle}")))?;
        Ok(DropRef {
            session: Arc::clone(&entry.session),
            name: entry.name.clone(),
        })
    }

    /// Take ownership of a session: give it a short handle and start pumping
    /// its events onto the bus.
    fn register(self: &Arc<Self>, session: DropSession, name: Option<String>) -> String {
        let handle = format!("d{}", self.next_drop.fetch_add(1, Ordering::SeqCst));
        let session = Arc::new(session);
        self.drops.lock().expect("drops").insert(
            handle.clone(),
            DropEntry {
                session: Arc::clone(&session),
                name,
            },
        );
        self.bus.emit(
            "drop.joined",
            json!({"drop": handle, "topic": session.topic_id().to_string()}),
        );
        self.spawn_pump(handle.clone(), session);
        handle
    }

    /// Translate one session's `DropEvent`s into API events, and run the
    /// consent flow for incoming offers.
    ///
    /// A `Weak` reference matters here: the service holds the session, the
    /// session's pump must not hold the service, or nothing is ever dropped.
    fn spawn_pump(self: &Arc<Self>, handle: String, session: Arc<DropSession>) {
        let weak = Arc::downgrade(self);
        let mut events = session.subscribe();
        let self_id = session.self_id();
        tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(e) => e,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let Some(service) = weak.upgrade() else { break };
                service.on_session_event(&handle, &session, self_id, event);
            }
        });
    }

    fn on_session_event(
        self: &Arc<Self>,
        handle: &str,
        session: &Arc<DropSession>,
        self_id: iroh::EndpointId,
        event: DropEvent,
    ) {
        let d = handle;
        match event {
            DropEvent::PeerJoined { peer } => {
                self.bus
                    .emit("peer.joined", json!({"drop": d, "peer": peer.to_string()}));
            }
            DropEvent::PeerLeft { peer } => {
                self.bus
                    .emit("peer.left", json!({"drop": d, "peer": peer.to_string()}));
            }
            DropEvent::OfferReceived { from, offer } => {
                self.bus.emit(
                    "offer.received",
                    json!({
                        "drop": d,
                        "from": from.to_string(),
                        "hash": offer.blob_hash.to_hex(),
                        "name": offer.name,
                        "size": offer.size,
                        "human_size": human_bytes(offer.size),
                        "media_type": offer.media_type,
                    }),
                );
                if from != self_id {
                    self.spawn_consent(handle.to_string(), Arc::clone(session), from, offer);
                }
            }
            DropEvent::OfferRejected { from, reason } => {
                self.bus.emit(
                    "offer.rejected",
                    json!({"drop": d, "from": from.to_string(), "reason": reason.to_string()}),
                );
            }
            DropEvent::FetchStarted { hash, provider } => {
                self.bus.emit(
                    "fetch.started",
                    json!({"drop": d, "hash": hash.to_hex(), "provider": provider.to_string()}),
                );
            }
            DropEvent::FetchProgress {
                hash,
                downloaded,
                total,
            } => {
                // Always let the last one through, so a UI never sticks at 97%.
                let is_final = total.is_some_and(|t| downloaded >= t);
                if is_final || self.progress_due(&hash.to_hex()) {
                    self.bus.emit(
                        "fetch.progress",
                        json!({"drop": d, "hash": hash.to_hex(),
                               "downloaded": downloaded, "total": total}),
                    );
                }
            }
            DropEvent::FetchCompleted { hash, provider } => {
                self.progress_seen.lock().expect("progress").remove(&hash.to_hex());
                self.bus.emit(
                    "fetch.completed",
                    json!({"drop": d, "hash": hash.to_hex(), "provider": provider.to_string()}),
                );
            }
            DropEvent::FetchFailed { hash, error } => {
                self.bus.emit(
                    "fetch.failed",
                    json!({"drop": d, "hash": hash.to_hex(), "error": error.to_string()}),
                );
            }
            DropEvent::ProviderAvailable { hash, peer } => {
                self.bus.emit(
                    "provider.available",
                    json!({"drop": d, "hash": hash.to_hex(), "peer": peer.to_string()}),
                );
            }
            DropEvent::ProviderUnavailable { hash, peer } => {
                self.bus.emit(
                    "provider.unavailable",
                    json!({"drop": d, "hash": hash.to_hex(), "peer": peer.to_string()}),
                );
            }
            DropEvent::ProtocolWarning { from, warning } => {
                self.bus.emit(
                    "protocol.warning",
                    json!({"drop": d, "from": from.map(|f| f.to_string()),
                           "warning": warning.to_string()}),
                );
            }
            other => debug!("unmapped session event: {other:?}"),
        }
    }

    /// The consent flow that replaces a blocking decider.
    ///
    /// `OfferDecider::decide` is synchronous and runs inside the gossip receive
    /// loop, so it cannot wait for a person. Instead the offer is merely
    /// *recorded* (auto-fetch is off), we ask a UI out here, and only an
    /// explicit yes turns into an ordinary manual fetch.
    fn spawn_consent(
        self: &Arc<Self>,
        handle: String,
        session: Arc<DropSession>,
        from: iroh::EndpointId,
        offer: iroh_drop::OfferV1,
    ) {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let question = json!({
                "drop": handle,
                "from": from.to_string(),
                // Everything below is untrusted display metadata. A UI must
                // render it as such — filenames especially.
                "name": offer.name,
                "size": offer.size,
                "human_size": human_bytes(offer.size),
                "media_type": offer.media_type,
                "members": offer.metadata.get("collection.members"),
                "known": false,
                "hash": offer.blob_hash.to_hex(),
                "expires_in_ms": CONSENT_TIMEOUT.as_millis() as u64,
            });

            // auto_accept is the windowless-helper posture: it answers consent
            // only when there is no live UI to ask. Asking always wins — a person
            // looking at a card is a better judge than a flag, and auto-accepting
            // while a UI is attached would silently bypass consent the user
            // expects to be asked for.
            let answer = if service.options.auto_accept && !service.asks.has_live_ui() {
                Some(json!({"accept": true}))
            } else {
                service
                    .asks
                    .ask("offer.accept", question, CONSENT_TIMEOUT)
                    .await
            };

            let Some(answer) = answer else {
                service.bus.emit(
                    "offer.declined",
                    json!({"drop": handle, "hash": offer.blob_hash.to_hex(),
                           "reason": "no consent"}),
                );
                return;
            };
            if !answer.get("accept").and_then(Value::as_bool).unwrap_or(false) {
                service.bus.emit(
                    "offer.declined",
                    json!({"drop": handle, "hash": offer.blob_hash.to_hex(),
                           "reason": "declined"}),
                );
                return;
            }

            let out = answer
                .get("out")
                .and_then(Value::as_str)
                .map(PathBuf::from)
                .unwrap_or_else(|| service.options.download_dir.clone());
            service.spawn_fetch(&handle, session, offer.blob_hash, out);
        });
    }

    fn spawn_fetch(
        self: &Arc<Self>,
        handle: &str,
        session: Arc<DropSession>,
        hash: iroh_drop::BlobHash,
        out: PathBuf,
    ) -> Value {
        let service = Arc::clone(self);
        self.spawn_task("fetch", handle, Some(hash.to_hex()), move |task| async move {
            match iroh_drop_sdk::collections::fetch_any(&session, hash, &out).await {
                Ok(paths) => {
                    service.bus.emit(
                        "fetch.materialized",
                        json!({"task": task, "hash": hash.to_hex(), "paths": paths}),
                    );
                    Ok(())
                }
                Err(e) => Err(e.to_string()),
            }
        })
    }

    /// Long work never blocks a connection: allocate a task, return its id,
    /// and report the outcome as events. Closing a UI must not abort a
    /// 10 GiB receive, so the task belongs to the daemon, not the client.
    fn spawn_task<F, Fut>(
        self: &Arc<Self>,
        kind: &str,
        drop_handle: &str,
        hash: Option<String>,
        work: F,
    ) -> Value
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let id = format!("t{}", self.next_task.fetch_add(1, Ordering::SeqCst));
        let service = Arc::clone(self);
        let task_id = id.clone();
        let handle = tokio::spawn(async move {
            let result = work(task_id.clone()).await;
            let (state, error) = match result {
                Ok(()) => ("done", None),
                Err(e) => ("failed", Some(e)),
            };
            if let Some(entry) = service.tasks.lock().expect("tasks").get_mut(&task_id) {
                entry.state = state.into();
                entry.abort = None;
            }
            service.bus.emit(
                "task.state",
                json!({"task": task_id, "state": state, "error": error}),
            );
        });

        self.tasks.lock().expect("tasks").insert(
            id.clone(),
            TaskEntry {
                kind: kind.into(),
                drop: drop_handle.into(),
                hash,
                state: "running".into(),
                abort: Some(handle.abort_handle()),
            },
        );
        self.bus.emit(
            "task.state",
            json!({"task": id, "kind": kind, "drop": drop_handle, "state": "running"}),
        );
        json!({"task": id})
    }

    /// Withdraw from every drop and close the endpoint.
    ///
    /// Closing the endpoint is not optional. Stopping the sessions only ends
    /// gossip participation — the blobs protocol answers on the endpoint, so a
    /// "stopped" daemon that still has an open endpoint keeps serving every
    /// byte it holds. `tests/socket_transport.rs` pins this: it asserts that a
    /// shut-down publisher cannot be the provider of a later fetch.
    pub async fn shutdown(self: Arc<Self>) {
        let entries: Vec<DropEntry> = {
            let mut drops = self.drops.lock().expect("drops");
            drops.drain().map(|(_, v)| v).collect()
        };
        for entry in entries {
            entry.session.shutdown_no_announce().await;
        }
        self.bus.emit("daemon.stopping", json!({}));
        self.protocol.stack().endpoint.close().await;
    }
}

/// A cheap clone of what a caller needs from a registered drop.
struct DropRef {
    session: Arc<DropSession>,
    #[allow(dead_code)]
    name: Option<String>,
}

impl std::ops::Deref for DropRef {
    type Target = Arc<DropSession>;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

fn status_str(status: &LocalBlobStatus) -> &'static str {
    match status {
        LocalBlobStatus::Missing => "missing",
        LocalBlobStatus::Fetching { .. } => "fetching",
        _ => "available",
    }
}
