//! The drop session: one gossip topic and its local state.
//!
//! A session subscribes to the drop's gossip topic, processes offer /
//! provider / request messages, drives manual and automatic fetches through
//! the blobs downloader, and re-announces availability for every blob it has
//! completely downloaded.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// parking_lot locks cannot be poisoned: one panic inside a critical section
// must not turn every later lock acquisition into a panic as well.
use parking_lot::{Mutex, RwLock};
use std::time::Duration;

use n0_future::time::{Instant, SystemTime};

use crate::seal::DropKey;
use crate::transport::{DropTransport, TransportEvent, TransportEventStream};
use bytes::Bytes;
use futures::StreamExt;
use iroh::{EndpointAddr, EndpointId, SecretKey};
use iroh_blobs::api::downloader::DownloadProgressItem;
use iroh_blobs::api::proto::BlobStatus;
use iroh_blobs::api::{Store, TempTag};
use iroh_blobs::protocol::GetRequest;
use iroh_blobs::HashAndFormat;
use iroh_gossip::proto::TopicId;
use n0_future::task::JoinSet;
use tokio::sync::{broadcast, watch, Semaphore};
use tracing::{debug, info, instrument, trace, warn};

use crate::builder::DropStack;
use crate::error::{
    DropError, IntegrityError, NetworkError, ProtocolError, ProtocolWarningKind, RejectReason,
    StorageError,
};
use crate::hash::BlobHash;
use crate::limits::{self, PeerRateLimiter};
use crate::message::{
    collision_safe_path, validate_name, ExtensionV1, MessageBodyV1, MessageV1, OfferV1,
    ProviderState, ProviderV1, RequestV1, MAX_BODY_SIZE,
};
use crate::policy::{DropPolicy, OfferContext, OfferDecider, OfferDecision};
use crate::state::{DropState, LocalBlobStatus, OfferRecord};
use crate::ticket::DropTicket;

/// How long to wait for provider announcements after broadcasting a request.
pub const DEFAULT_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for the topic to gain a first neighbor before sending a
/// message that must not be silently dropped (e.g. requests).
const JOIN_WAIT: Duration = Duration::from_secs(3);

/// Minimum interval between availability replies for the same requested hash.
const REQUEST_REPLY_INTERVAL: Duration = Duration::from_secs(10);

/// How soon after a catch-up from a peer we would pull from that same peer
/// again. Anti-entropy fires on every neighbor-up, and a flapping connection
/// must not become a sync loop; an up-to-date pull is one cheap round trip,
/// so a minute is plenty fresh for files humans are waiting on.
const ANTI_ENTROPY_COOLDOWN: Duration = Duration::from_secs(60);

/// Upper bound on the anti-entropy bookkeeping map. Cleared wholesale when
/// hit: the worst case of a cleared entry is one extra cheap sync.
const ANTI_ENTROPY_PEERS_CAP: usize = 512;

/// Capacity of the event broadcast channel.
const EVENT_CHANNEL_CAPACITY: usize = 512;

/// Capacity of each extension-kind channel. A lagging extension consumer
/// loses frames rather than slowing the session; a fresh [`DropSession::on_extension`]
/// replays what the retained log still holds.
const EXTENSION_CHANNEL_CAPACITY: usize = 256;

/// Events emitted by a [`DropSession`], tagged by subsystem in the CLI.
///
/// Non-exhaustive: new event kinds are additive, so match with a wildcard arm.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum DropEvent {
    /// A new direct neighbor appeared in the gossip swarm.
    PeerJoined {
        /// The neighbor's endpoint identity.
        peer: EndpointId,
    },

    /// A direct neighbor left the gossip swarm.
    PeerLeft {
        /// The neighbor's endpoint identity.
        peer: EndpointId,
    },

    /// A protocol-valid offer was received and recorded.
    OfferReceived {
        /// The verified author of the offer.
        from: EndpointId,
        /// The offered content.
        offer: OfferV1,
    },

    /// An offer was rejected. If the offer was protocol-valid but
    /// policy-forbidden, it is still recorded and can be fetched manually.
    OfferRejected {
        /// The verified author (or delivering peer for undecodable frames).
        from: EndpointId,
        /// Why it was rejected.
        reason: RejectReason,
    },

    /// A fetch attempt from a specific provider started.
    FetchStarted {
        /// The blob being fetched.
        hash: BlobHash,
        /// The provider being tried.
        provider: EndpointId,
    },

    /// Fetch progress. `downloaded` is cumulative for the current fetch.
    FetchProgress {
        /// The blob being fetched.
        hash: BlobHash,
        /// Bytes downloaded so far.
        downloaded: u64,
        /// Advertised total size, if known.
        total: Option<u64>,
    },

    /// A fetch completed and the content verified against its hash.
    FetchCompleted {
        /// The fetched blob.
        hash: BlobHash,
        /// The provider that served the winning transfer.
        provider: EndpointId,
    },

    /// A fetch failed after all known providers were tried.
    FetchFailed {
        /// The blob.
        hash: BlobHash,
        /// The failure.
        error: DropError,
    },

    /// A peer announced it serves a blob.
    ProviderAvailable {
        /// The blob.
        hash: BlobHash,
        /// The serving peer.
        peer: EndpointId,
    },

    /// A peer announced it stopped serving a blob, or left.
    ProviderUnavailable {
        /// The blob.
        hash: BlobHash,
        /// The peer.
        peer: EndpointId,
    },

    /// A non-fatal protocol anomaly was observed.
    ProtocolWarning {
        /// The peer involved, if known.
        from: Option<EndpointId>,
        /// What happened.
        warning: ProtocolWarningKind,
    },
}

/// A signature-verified gossip frame carrying a message kind this crate
/// does not implement — the raw material of extension protocols.
///
/// The author is cryptographically attributable (the frame's signature was
/// verified on receipt), but the payload is untrusted, opaque, and
/// meaningful only to the extension that defined the kind.
#[derive(Clone, Debug)]
pub struct ExtensionFrame {
    /// The verified author of the frame.
    pub author: EndpointId,
    /// The message's dedup id. Extensions that republish or reply should
    /// track these to stay idempotent.
    pub id: [u8; 16],
    /// The extension protocol's namespace (matches the subscription).
    pub namespace: [u8; 16],
    /// The extension protocol's own message number.
    pub local_kind: u32,
    /// The extension protocol's payload schema version.
    pub schema_version: u16,
    /// The extension's opaque payload.
    pub payload: Bytes,
}

/// Where a completed fetch should put its bytes.
#[derive(Clone, Debug)]
pub enum FetchOutput {
    /// Export to a collision-safe file inside this directory.
    Directory(PathBuf),
    /// Export to this exact path (parent directories must exist).
    Exact(PathBuf),
    /// Keep the blob in the blob store only; do not export.
    Store,
}

/// The result of a successful fetch.
#[derive(Clone, Debug)]
pub struct FetchResult {
    /// The fetched blob.
    pub hash: BlobHash,
    /// Actual size in bytes, confirmed by the blob protocol.
    pub size: u64,
    /// The export path, if an export was requested.
    pub path: Option<PathBuf>,
    /// The provider that served the winning transfer, if a transfer happened.
    pub provider: Option<EndpointId>,
    /// Whether the bytes were already locally complete before the fetch.
    pub already_local: bool,
}

/// A blob that was published into a drop.
#[derive(Clone, Debug)]
pub struct PublishedBlob {
    /// Canonical content identity.
    pub hash: BlobHash,
    /// Validated display name.
    pub name: String,
    /// Size in bytes.
    pub size: u64,
    /// Advisory media type, guessed from the file extension.
    pub media_type: Option<String>,
}

/// Owns the session's shutdown signal.
///
/// Held by user-facing [`DropSession`] handles (and, via an `Arc` clone, by
/// in-flight internal tasks). When the *last* guard is dropped the event
/// loop is signalled to stop, which in turn releases the session's gossip
/// subscription. Internal machinery keeps only a `Weak` reference so the
/// event loop never keeps the session alive by itself.
#[derive(Debug)]
struct SessionGuard(watch::Sender<bool>);

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Live sessions by topic, for the sync accept handler. Sessions register
/// themselves on creation; the `Weak` keeps the registry from extending
/// session lifetimes.
pub(crate) type SessionRegistry =
    Arc<parking_lot::RwLock<HashMap<TopicId, std::sync::Weak<SessionInner>>>>;

/// Shared session internals.
pub(crate) struct SessionInner {
    pub(crate) stack: Arc<DropStack>,
    /// Application judgement on incoming offers, after policy limits.
    decider: Arc<dyn OfferDecider>,
    policy: DropPolicy,
    pub(crate) topic_id: TopicId,
    ticket: RwLock<DropTicket>,
    transport: Arc<dyn DropTransport>,
    /// Present iff this is a private drop (sealed wire family 4).
    pub(crate) drop_key: Option<DropKey>,
    /// Participation mode from the ticket: which family this session speaks.
    /// `Sealed` without a key is a blind relay — retains and relays sealed
    /// frames, never decrypts, publishes, or serves history.
    pub(crate) mode: crate::ticket::DropMode,
    pub(crate) state: RwLock<DropState>,
    events: broadcast::Sender<DropEvent>,
    /// Weak handle to the shutdown guard, used to spawn internal tasks that
    /// legitimately extend the session's lifetime (e.g. auto-fetch).
    guard: std::sync::Weak<SessionGuard>,
    tasks: Mutex<JoinSet<()>>,
    fetch_sem: Semaphore,
    active_fetches: AtomicUsize,
    auto_fetch_bytes: AtomicU64,
    /// Number of currently connected gossip neighbors (tracked from events).
    neighbors: AtomicUsize,
    /// Notifies waiters when `neighbors` becomes non-zero.
    neighbor_notify: tokio::sync::Notify,
    /// Last anti-entropy pull per peer, for the cooldown.
    synced_from: Mutex<HashMap<EndpointId, Instant>>,
    request_replies: Mutex<HashMap<BlobHash, Instant>>,
    /// Per-peer flood control for inbound messages and answered requests.
    message_limiter: Mutex<PeerRateLimiter>,
    request_limiter: Mutex<PeerRateLimiter>,
    provider_timeout: Duration,
    secret: SecretKey,
    temp_tags: Mutex<Vec<TempTag>>,
    /// Live extension subscriptions by namespace, created lazily by
    /// `on_extension`.
    extension_sinks: Mutex<HashMap<[u8; 16], broadcast::Sender<ExtensionFrame>>>,
}

impl SessionInner {
    /// Spawn a task owned by this session, so shutdown joins it.
    pub(crate) fn spawn_task<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.tasks.lock().spawn(future);
    }
}

/// One drop session: a gossip topic, its local state, and its fetch engine.
///
/// Cloning is cheap; all clones share the same session. The session stops
/// when the last handle is dropped (after in-flight internal tasks finish)
/// or when [`DropSession::shutdown`] is called.
#[derive(Clone)]
pub struct DropSession {
    inner: Arc<SessionInner>,
    guard: Arc<SessionGuard>,
}

impl DropSession {
    pub(crate) fn new(
        stack: Arc<DropStack>,
        policy: DropPolicy,
        decider: Arc<dyn OfferDecider>,
        topic_id: TopicId,
        ticket: DropTicket,
        transport: Arc<dyn DropTransport>,
        drop_key: Option<DropKey>,
    ) -> Self {
        let self_id = stack.endpoint.id();
        let secret = stack.endpoint.secret_key().clone();
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let guard = Arc::new(SessionGuard(shutdown));
        let event_stream = transport.take_events();
        let provider_timeout = policy.provider_timeout;
        let inner = Arc::new(SessionInner {
            stack,
            policy: policy.clone(),
            decider,
            topic_id,
            mode: ticket.mode(),
            ticket: RwLock::new(ticket),
            transport,
            drop_key,
            state: RwLock::new(DropState::new(topic_id, self_id, policy)),
            events,
            guard: Arc::downgrade(&guard),
            tasks: Mutex::new(JoinSet::new()),
            fetch_sem: Semaphore::new(64),
            active_fetches: AtomicUsize::new(0),
            auto_fetch_bytes: AtomicU64::new(0),
            neighbors: AtomicUsize::new(0),
            neighbor_notify: tokio::sync::Notify::new(),
            synced_from: Mutex::new(HashMap::new()),
            request_replies: Mutex::new(HashMap::new()),
            message_limiter: Mutex::new(PeerRateLimiter::new(limits::MESSAGES)),
            request_limiter: Mutex::new(PeerRateLimiter::new(limits::REQUESTS)),
            provider_timeout,
            secret,
            temp_tags: Mutex::new(Vec::new()),
            extension_sinks: Mutex::new(HashMap::new()),
        });
        // Register for the catch-up sync accept handler.
        inner
            .stack
            .session_registry()
            .write()
            .insert(topic_id, Arc::downgrade(&inner));
        let task_inner = Arc::clone(&inner);
        inner
            .tasks
            .lock()
            .spawn(run_event_loop(task_inner, event_stream, shutdown_rx));
        DropSession { inner, guard }
    }

    /// The current ticket for this drop.
    ///
    /// The returned ticket keeps the original bootstrap addresses and adds
    /// currently known peers (by identity; their addresses resolve through
    /// the endpoint's address lookup services while they are online). Share
    /// this refreshed ticket so new peers can bootstrap even after the
    /// original creator has left.
    pub fn ticket(&self) -> DropTicket {
        let mut ticket = self.inner.ticket.read().clone();
        let state = self.inner.state.read();
        let mut nodes: Vec<iroh::EndpointAddr> = ticket.bootstrap_nodes().to_vec();
        // A ticket handed out by a live peer must point at that peer: whoever
        // receives it can then reach us even if the original publisher is
        // gone. Our full address is included, which also works offline.
        //
        // Any existing entry for our own id is stale and must be replaced,
        // not just deduped against: after a restart we come back with the
        // same identity but a freshly bound port, and a ticket that keeps the
        // old entry points every joiner at our own ghost.
        let self_addr = self.inner.stack.addr();
        nodes.retain(|a| a.id != self_addr.id);
        nodes.insert(0, self_addr.clone());
        let mut have: std::collections::HashSet<EndpointId> = nodes.iter().map(|a| a.id).collect();
        for peer in &state.known_peers {
            if nodes.len() >= crate::ticket::MAX_BOOTSTRAP_NODES {
                break;
            }
            if *peer != state.self_endpoint_id && have.insert(*peer) {
                nodes.push(iroh::EndpointAddr::from(*peer));
            }
        }
        nodes.truncate(crate::ticket::MAX_BOOTSTRAP_NODES);
        ticket.set_bootstrap_nodes(nodes);
        ticket
    }

    /// A ticket that names peers by id only, without socket addresses.
    ///
    /// Much shorter than [`Self::ticket`], and it never goes stale, because
    /// addresses are looked up at join time instead of being frozen into the
    /// string. It only works where the joiner can *resolve* an id: online
    /// (pkarr/DNS) or on a local network with mDNS enabled
    /// ([`crate::builder::StackOptions::mdns`]). With neither, use
    /// [`Self::ticket`].
    pub fn short_ticket(&self) -> DropTicket {
        let mut ticket = self.ticket();
        let nodes: Vec<iroh::EndpointAddr> = ticket
            .bootstrap_nodes()
            .iter()
            .map(|addr| iroh::EndpointAddr::from(addr.id))
            .collect();
        ticket.set_bootstrap_nodes(nodes);
        ticket
    }

    /// The gossip topic of this drop.
    pub fn topic_id(&self) -> TopicId {
        self.inner.topic_id
    }

    /// Peers seen in this drop during the session (past and present
    /// neighbors plus anyone who delivered us a message).
    pub fn peers(&self) -> Vec<EndpointId> {
        let state = self.inner.state.read();
        let mut peers: Vec<EndpointId> = state.known_peers.iter().copied().collect();
        peers.sort();
        peers
    }

    /// Pull additional peers into the gossip swarm, e.g. the bootstrap set
    /// of a fresher ticket for this same drop. Discovery still has to
    /// resolve them — pair with [`crate::DropStack::add_known_addr`] when
    /// the addresses are new. Duplicate or stale peers are harmless.
    pub async fn join_peers(&self, peers: Vec<EndpointId>) -> Result<(), DropError> {
        self.inner.transport.join_peers(peers).await
    }

    /// Our endpoint identity.
    pub fn self_id(&self) -> EndpointId {
        self.inner.stack.endpoint.id()
    }

    /// Subscribe to the session event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<DropEvent> {
        self.inner.events.subscribe()
    }

    /// All known offers.
    pub fn offers(&self) -> Vec<OfferRecord> {
        let state = self.inner.state.read();
        let mut offers: Vec<OfferRecord> = state.offers.values().cloned().collect();
        offers.sort_by_key(|r| r.first_seen_at);
        offers
    }

    /// Known providers for a blob, in fetch order.
    pub fn providers(&self, hash: &BlobHash) -> Vec<EndpointId> {
        let state = self.inner.state.read();
        state
            .providers
            .get(hash)
            .map(|set| set.ordered())
            .unwrap_or_default()
    }

    /// Resolve a hash, hash prefix, name, or alias to a content hash.
    ///
    /// Names are display strings, not identities: an ambiguous name is an
    /// error listing the candidates, never an arbitrary pick.
    pub fn resolve(&self, hash_or_name: &str) -> Result<BlobHash, crate::state::ResolveOfferError> {
        self.inner.state.read().find_offer(hash_or_name)
    }

    /// Import a local file into the blob store and announce it to the drop.
    #[instrument(skip_all, fields(path = %path.as_ref().display()))]
    pub async fn publish_path(&self, path: impl AsRef<Path>) -> Result<PublishedBlob, DropError> {
        self.publish_path_as(path, None).await
    }

    /// Import a file like [`publish_path`](Self::publish_path), but override
    /// the advertised name (otherwise the file name is used).
    pub async fn publish_path_as(
        &self,
        path: impl AsRef<Path>,
        name: Option<String>,
    ) -> Result<PublishedBlob, DropError> {
        let path = path.as_ref();
        let name = match name {
            Some(name) => name,
            None => path
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or_else(|| {
                    StorageError::InvalidPath(format!("no file name: {}", path.display()))
                })?
                .to_string(),
        };
        validate_name(&name).map_err(|e| StorageError::InvalidPath(e.to_string()))?;
        let meta = std::fs::metadata(path)
            .map_err(|e| StorageError::InvalidPath(format!("{}: {e}", path.display())))?;
        if meta.is_dir() {
            return Err(StorageError::InvalidPath(format!(
                "{} is a directory; directories are not supported by the MVP",
                path.display()
            ))
            .into());
        }
        // iroh-blobs requires absolute import paths.
        let abs_path = std::path::absolute(path)
            .map_err(|e| StorageError::InvalidPath(format!("{}: {e}", path.display())))?;
        let tag = self
            .inner
            .stack
            .store()
            .blobs()
            .add_path(&abs_path)
            .temp_tag()
            .await
            .map_err(|e| StorageError::Import(e.to_string()))?;
        let hash: iroh_blobs::Hash = *tag.as_ref();
        let haf = HashAndFormat::from(&tag);
        self.inner.temp_tags.lock().push(tag);
        let media_type = mime_guess::from_path(path).first().map(|m| m.to_string());
        self.finish_import(hash.into(), haf, name, meta.len(), media_type)
            .await
    }

    /// The shared blob store.
    ///
    /// Exposed so higher layers (collections, custom exporters, verifiers)
    /// can read and write bytes without the protocol crate having to know
    /// their conventions.
    pub fn store(&self) -> Store {
        Store::clone(self.inner.stack.store())
    }

    /// Read a locally complete blob's bytes, refusing anything larger than
    /// `max_len`.
    ///
    /// Higher layers use this for small, structured blobs (collection
    /// manifests, indexes) without depending on the blob store's API.
    pub async fn read_bytes(&self, hash: BlobHash, max_len: u64) -> Result<Bytes, DropError> {
        let blob_hash = iroh_blobs::Hash::from_bytes(*hash.as_bytes());
        let size = match self
            .inner
            .stack
            .store()
            .blobs()
            .status(blob_hash)
            .await
            .map_err(|e| StorageError::Store(e.to_string()))?
        {
            BlobStatus::Complete { size } => size,
            BlobStatus::Partial { .. } | BlobStatus::NotFound => {
                return Err(StorageError::Store(format!(
                    "blob {} is not locally complete",
                    hash.fmt_short()
                ))
                .into())
            }
        };
        if size > max_len {
            return Err(StorageError::Store(format!(
                "blob {} is {size} bytes, over the {max_len} byte limit",
                hash.fmt_short()
            ))
            .into());
        }
        self.inner
            .stack
            .store()
            .blobs()
            .get_bytes(blob_hash)
            .await
            .map_err(|e| StorageError::Store(e.to_string()).into())
    }

    /// Import a file into the blob store *without* announcing it.
    ///
    /// The bytes become servable to anyone who learns the hash, which is how
    /// collection members are made available without flooding the drop with
    /// one offer per file. Returns the content hash and size.
    pub async fn import_path(&self, path: impl AsRef<Path>) -> Result<(BlobHash, u64), DropError> {
        let path = path.as_ref();
        let meta = std::fs::metadata(path)
            .map_err(|e| StorageError::InvalidPath(format!("{}: {e}", path.display())))?;
        if !meta.is_file() {
            return Err(
                StorageError::InvalidPath(format!("{} is not a file", path.display())).into(),
            );
        }
        // iroh-blobs requires absolute import paths.
        let abs_path = std::path::absolute(path)
            .map_err(|e| StorageError::InvalidPath(format!("{}: {e}", path.display())))?;
        let tag = self
            .inner
            .stack
            .store()
            .blobs()
            .add_path(abs_path)
            .temp_tag()
            .await
            .map_err(|e| StorageError::Import(e.to_string()))?;
        let hash: iroh_blobs::Hash = *tag.as_ref();
        let haf = HashAndFormat::from(&tag);
        self.inner.temp_tags.lock().push(tag);
        // Named tag so the blob survives a store restart.
        self.inner
            .stack
            .store()
            .tags()
            .set(format!("iroh-drop/{}", BlobHash::from(hash)), haf)
            .await
            .map_err(|e| StorageError::Store(e.to_string()))?;
        Ok((hash.into(), meta.len()))
    }

    /// Import bytes into the blob store and announce them to the drop.
    pub async fn publish_bytes(
        &self,
        name: String,
        bytes: Bytes,
    ) -> Result<PublishedBlob, DropError> {
        self.publish_bytes_as(name, bytes, None).await
    }

    /// Import bytes and announce them with an explicit media type.
    ///
    /// The media type is an advisory hint on the wire; higher layers use it
    /// for conventions such as collection manifests.
    pub async fn publish_bytes_as(
        &self,
        name: String,
        bytes: Bytes,
        media_type: Option<String>,
    ) -> Result<PublishedBlob, DropError> {
        self.publish_bytes_with(name, bytes, media_type, Default::default())
            .await
    }

    /// Import bytes and announce them with a media type and metadata.
    ///
    /// Metadata is bounded, untrusted, and meaningless to the protocol; it
    /// exists so higher layers can carry hints (for example how many files a
    /// collection contains) without new wire versions. Entries beyond the
    /// documented limits are rejected before broadcast.
    pub async fn publish_bytes_with(
        &self,
        name: String,
        bytes: Bytes,
        media_type: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> Result<PublishedBlob, DropError> {
        validate_name(&name).map_err(|e| StorageError::InvalidPath(e.to_string()))?;
        let size = bytes.len() as u64;
        let tag = self
            .inner
            .stack
            .store()
            .blobs()
            .add_bytes(bytes)
            .temp_tag()
            .await
            .map_err(|e| StorageError::Import(e.to_string()))?;
        let hash: iroh_blobs::Hash = *tag.as_ref();
        let haf = HashAndFormat::from(&tag);
        self.inner.temp_tags.lock().push(tag);
        let media_type =
            media_type.or_else(|| mime_guess::from_path(&name).first().map(|m| m.to_string()));
        self.finish_import_with(hash.into(), haf, name, size, media_type, metadata)
            .await
    }

    async fn finish_import(
        &self,
        hash: BlobHash,
        haf: HashAndFormat,
        name: String,
        size: u64,
        media_type: Option<String>,
    ) -> Result<PublishedBlob, DropError> {
        self.finish_import_with(hash, haf, name, size, media_type, Default::default())
            .await
    }

    async fn finish_import_with(
        &self,
        hash: BlobHash,
        haf: HashAndFormat,
        name: String,
        size: u64,
        media_type: Option<String>,
        metadata: std::collections::BTreeMap<String, String>,
    ) -> Result<PublishedBlob, DropError> {
        // Named tag so the blob survives a store restart.
        self.inner
            .stack
            .store()
            .tags()
            .set(format!("iroh-drop/{hash}"), haf)
            .await
            .map_err(|e| StorageError::Store(e.to_string()))?;
        let offer = OfferV1 {
            blob_hash: hash,
            name: name.clone(),
            size,
            media_type: media_type.clone(),
            created_at_ms: Some(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            ),
            metadata,
        };
        {
            let mut state = self.inner.state.write();
            state.record_offer(self.self_id(), offer.clone());
            if let Some(record) = state.offers.get_mut(&hash) {
                record.local_status = LocalBlobStatus::Complete;
            }
        }
        self.broadcast(MessageBodyV1::Offer(offer)).await?;
        self.broadcast(MessageBodyV1::Provider(ProviderV1 {
            blob_hash: hash,
            state: ProviderState::Available,
            announced_at_ms: Some(now_ms()),
        }))
        .await?;
        info!(%hash, %name, size, "offer sent");
        Ok(PublishedBlob {
            hash,
            name,
            size,
            media_type,
        })
    }

    /// Fetch a blob by hash (or via [`DropSession::resolve`] for names) and
    /// export it according to `output`.
    ///
    /// If no providers are known, a [`RequestV1`] is broadcast and the call
    /// waits up to the provider timeout for availability announcements.
    /// Manual fetches ignore the auto-fetch policy; the blob hash guarantees
    /// the bytes.
    pub async fn fetch(
        &self,
        hash: BlobHash,
        output: FetchOutput,
    ) -> Result<FetchResult, DropError> {
        self.fetch_inner(hash, output).await
    }

    async fn fetch_inner(
        &self,
        hash: BlobHash,
        output: FetchOutput,
    ) -> Result<FetchResult, DropError> {
        match self.fetch_inner_impl(hash, output).await {
            Ok(result) => Ok(result),
            Err(err) => {
                // Uniform terminal-failure handling: status + event, exactly once.
                self.set_status(
                    &hash,
                    LocalBlobStatus::Failed {
                        retryable: true,
                        message: err.to_string(),
                    },
                );
                self.emit(DropEvent::FetchFailed {
                    hash,
                    error: err.clone(),
                });
                Err(err)
            }
        }
    }

    async fn fetch_inner_impl(
        &self,
        hash: BlobHash,
        output: FetchOutput,
    ) -> Result<FetchResult, DropError> {
        let store = self.inner.stack.store().clone();
        let blob_hash: iroh_blobs::Hash = hash.into();

        // Already complete locally?
        if matches!(
            store.blobs().status(blob_hash).await,
            Ok(BlobStatus::Complete { .. })
        ) {
            let size = self.mark_complete(&hash, None).await?;
            let path = self.export(&hash, &output).await?;
            return Ok(FetchResult {
                hash,
                size,
                path,
                provider: None,
                already_local: true,
            });
        }

        // Up to three rounds of: request providers (if needed) -> download.
        // Requests are re-broadcast each round because the provider index
        // may be stale and because a broadcast that raced the gossip join
        // can be dropped by the swarm.
        let mut current_provider: Option<EndpointId> = None;
        let mut last_error: Option<DropError> = None;
        for attempt in 0..3 {
            let mut providers = self.known_remote_providers(&hash);
            if providers.is_empty() {
                debug!(%hash, attempt, "no known providers, broadcasting request");
                self.broadcast_when_joined(MessageBodyV1::Request(RequestV1 { blob_hash: hash }))
                    .await?;
                providers = self.wait_for_providers(&hash).await;
                if providers.is_empty() {
                    last_error = Some(DropError::Network(NetworkError::NoProviders(
                        hash.to_string(),
                    )));
                    continue;
                }
            }
            match self
                .download_attempt(&hash, &providers, &mut current_provider)
                .await
            {
                Ok(()) => {
                    last_error = None;
                    break;
                }
                Err(e) => {
                    last_error = Some(e);
                    debug!(%hash, attempt, "fetch attempt failed, re-requesting providers");
                    self.broadcast_when_joined(MessageBodyV1::Request(RequestV1 {
                        blob_hash: hash,
                    }))
                    .await?;
                    self.wait_for_providers(&hash).await;
                }
            }
        }
        if let Some(err) = last_error {
            return Err(err);
        }

        // The downloader only completes when the content is fully verified
        // against the requested hash (bao verification is inherent to the
        // blobs protocol). Confirm the local status before announcing.
        let size = match store.blobs().status(blob_hash).await {
            Ok(BlobStatus::Complete { size }) => size,
            other => {
                return Err(DropError::Integrity(IntegrityError::HashMismatch {
                    expected: hash.to_string(),
                    actual: format!("incomplete local state: {other:?}"),
                }));
            }
        };
        let advertised = self
            .inner
            .state
            .read()
            .offers
            .get(&hash)
            .map(|r| r.offer.size);
        if let Some(advertised) = advertised {
            if advertised != size {
                warn!(%hash, advertised, actual = size, "advertised size differs from actual size");
            }
        }

        let provider = current_provider;
        if let Some(p) = provider {
            let mut state = self.inner.state.write();
            if let Some(set) = state.providers.get_mut(&hash) {
                set.mark_success(p);
            }
        }
        debug!(%hash, "download stream finished, marking complete");
        self.mark_complete(&hash, Some(size)).await?;
        debug!(%hash, "marked complete, exporting");
        let path = self.export(&hash, &output).await?;
        debug!(%hash, "exported");

        self.emit(DropEvent::FetchCompleted {
            hash,
            provider: provider.unwrap_or_else(|| self.self_id()),
        });
        Ok(FetchResult {
            hash,
            size,
            path,
            provider,
            already_local: false,
        })
    }

    /// One download attempt against an ordered provider list.
    async fn download_attempt(
        &self,
        hash: &BlobHash,
        providers: &[EndpointId],
        current_provider: &mut Option<EndpointId>,
    ) -> Result<(), DropError> {
        let total = self
            .inner
            .state
            .read()
            .offers
            .get(hash)
            .map(|r| r.offer.size);
        self.set_status(
            hash,
            LocalBlobStatus::Fetching {
                downloaded: 0,
                total,
            },
        );
        let blob_hash: iroh_blobs::Hash = (*hash).into();
        let downloader = self.inner.stack.downloader();
        let progress = downloader.download(GetRequest::blob(blob_hash), providers.to_vec());
        let mut stream = progress
            .stream()
            .await
            .map_err(|e| DropError::Network(NetworkError::Transfer(e.to_string())))?;
        let mut outcome: Result<(), DropError> = Ok(());
        while let Some(item) = stream.next().await {
            match item {
                DownloadProgressItem::TryProvider { id, .. } => {
                    *current_provider = Some(id);
                    self.emit(DropEvent::FetchStarted {
                        hash: *hash,
                        provider: id,
                    });
                }
                DownloadProgressItem::ProviderFailed { id, .. } => {
                    {
                        let mut state = self.inner.state.write();
                        if let Some(set) = state.providers.get_mut(hash) {
                            set.mark_failure(id);
                        }
                    }
                    self.emit(DropEvent::ProtocolWarning {
                        from: Some(id),
                        warning: ProtocolWarningKind::ProviderFailed {
                            provider: id.fmt_short().to_string(),
                            hash: hash.fmt_short(),
                            reason: "transfer failed".into(),
                        },
                    });
                }
                DownloadProgressItem::Progress(downloaded) => {
                    self.set_status(hash, LocalBlobStatus::Fetching { downloaded, total });
                    self.emit(DropEvent::FetchProgress {
                        hash: *hash,
                        downloaded,
                        total,
                    });
                }
                DownloadProgressItem::PartComplete { .. } => {}
                DownloadProgressItem::DownloadError => {
                    outcome = Err(DropError::Network(NetworkError::Transfer(
                        "all providers failed".into(),
                    )));
                }
                DownloadProgressItem::Error(e) => {
                    outcome = Err(DropError::Network(NetworkError::Transfer(e.to_string())));
                }
            }
        }
        outcome
    }

    /// Mark a blob locally complete, remember us as provider, and announce
    /// availability to the group. Returns the confirmed size.
    async fn mark_complete(
        &self,
        hash: &BlobHash,
        known_size: Option<u64>,
    ) -> Result<u64, DropError> {
        let store = self.inner.stack.store().clone();
        let blob_hash: iroh_blobs::Hash = (*hash).into();
        let size = match known_size {
            Some(size) => size,
            None => match store.blobs().status(blob_hash).await {
                Ok(BlobStatus::Complete { size }) => size,
                other => {
                    return Err(DropError::Integrity(IntegrityError::HashMismatch {
                        expected: "complete local blob".into(),
                        actual: format!("{other:?}"),
                    }))
                }
            },
        };
        {
            let mut state = self.inner.state.write();
            let self_id = state.self_endpoint_id;
            let set = state.providers.entry(*hash).or_default();
            set.add(self_id, false);
            if let Some(record) = state.offers.get_mut(hash) {
                record.local_status = LocalBlobStatus::Complete;
            } else {
                // Synthesize a record so this blob shows up in `offers()`.
                state.record_offer(
                    self_id,
                    OfferV1 {
                        blob_hash: *hash,
                        name: hash.to_string(),
                        size,
                        media_type: None,
                        created_at_ms: None,
                        metadata: Default::default(),
                    },
                );
                if let Some(record) = state.offers.get_mut(hash) {
                    record.local_status = LocalBlobStatus::Complete;
                }
            }
        }
        // Keep a named tag so the blob survives restarts of a persistent store.
        self.inner
            .stack
            .store()
            .tags()
            .set(format!("iroh-drop/{hash}"), HashAndFormat::raw(blob_hash))
            .await
            .map_err(|e| DropError::Storage(StorageError::Store(e.to_string())))?;
        self.broadcast(MessageBodyV1::Provider(ProviderV1 {
            blob_hash: *hash,
            state: ProviderState::Available,
            announced_at_ms: Some(now_ms()),
        }))
        .await?;
        info!(%hash, "now serving");
        Ok(size)
    }

    async fn export(
        &self,
        hash: &BlobHash,
        output: &FetchOutput,
    ) -> Result<Option<PathBuf>, DropError> {
        let path = match output {
            FetchOutput::Store => return Ok(None),
            FetchOutput::Exact(path) => path.clone(),
            FetchOutput::Directory(dir) => {
                let name = {
                    let state = self.inner.state.read();
                    state
                        .offers
                        .get(hash)
                        .map(|r| r.display_name().to_string())
                        .filter(|n| validate_name(n).is_ok())
                        .unwrap_or_else(|| hash.to_string())
                };
                std::fs::create_dir_all(dir).map_err(|e| StorageError::Export(e.to_string()))?;
                collision_safe_path(dir, &name, hash)
                    .map_err(|e| StorageError::Export(e.to_string()))?
            }
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::Export(e.to_string()))?;
        }
        // iroh-blobs requires absolute export targets.
        let path = std::path::absolute(&path).map_err(|e| StorageError::Export(e.to_string()))?;
        let blob_hash: iroh_blobs::Hash = (*hash).into();
        self.inner
            .stack
            .store()
            .blobs()
            .export(blob_hash, &path)
            .await
            .map_err(|e| DropError::Storage(StorageError::Export(e.to_string())))?;
        Ok(Some(path))
    }

    fn known_remote_providers(&self, hash: &BlobHash) -> Vec<EndpointId> {
        let state = self.inner.state.read();
        state
            .providers
            .get(hash)
            .map(|set| set.ordered_excluding(state.self_endpoint_id))
            .unwrap_or_default()
    }

    async fn wait_for_providers(&self, hash: &BlobHash) -> Vec<EndpointId> {
        let start = Instant::now();
        loop {
            let providers = self.known_remote_providers(hash);
            if !providers.is_empty() {
                return providers;
            }
            if start.elapsed() > self.inner.provider_timeout {
                return Vec::new();
            }
            n0_future::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Broadcast after waiting briefly for the topic to have at least one
    /// neighbor. Used for requests, where a dropped message means silence.
    async fn broadcast_when_joined(&self, body: MessageBodyV1) -> Result<(), DropError> {
        if self.inner.neighbors.load(Ordering::SeqCst) == 0 {
            let notified = self.inner.neighbor_notify.notified();
            let _ = n0_future::time::timeout(JOIN_WAIT, notified).await;
        }
        self.broadcast(body).await
    }

    async fn broadcast(&self, body: MessageBodyV1) -> Result<(), DropError> {
        let retain = match body {
            MessageBodyV1::Offer(_) | MessageBodyV1::Provider(_) => Retain::Yes,
            MessageBodyV1::Request(_) => Retain::No,
            // Extensions get relay duty, not history: bounded by the
            // unknown-frame budget, replayable to late subscribers while
            // retained, never served as catch-up history for core state.
            MessageBodyV1::Extension(_) => Retain::Unknown,
        };
        self.broadcast_signed(MessageV1::new(body), retain).await
    }

    /// Sign, pre-dedupe our own echo, retain per `retain`, and broadcast.
    async fn broadcast_signed(&self, msg: MessageV1, retain: Retain) -> Result<(), DropError> {
        // The session's family is fixed by its ticket's mode: sealed drops
        // seal every frame (family 4), public drops speak family 3. A blind
        // relay (sealed, no key) has nothing it may say.
        let bytes = match (self.inner.mode, &self.inner.drop_key) {
            (crate::ticket::DropMode::Sealed, Some(key)) => {
                msg.encode_sealed(&self.inner.secret, key, &self.inner.topic_id)?
            }
            (crate::ticket::DropMode::Sealed, None) => {
                return Err(DropError::Protocol(ProtocolError::NoDropKey));
            }
            (crate::ticket::DropMode::Public, _) => {
                msg.encode(&self.inner.secret, &self.inner.topic_id)?
            }
        };
        {
            let mut state = self.inner.state.write();
            // Our own echo is already applied locally; skip it on receipt.
            state
                .seen_messages
                .check_and_insert((self.self_id(), msg.id));
            match retain {
                Retain::Yes => state.retain_frame(Bytes::copy_from_slice(&bytes)),
                Retain::Unknown => state.retain_unknown_frame(Bytes::copy_from_slice(&bytes)),
                Retain::No => {}
            }
        }
        self.inner.transport.broadcast(Bytes::from(bytes)).await?;
        Ok(())
    }

    /// Broadcast a namespaced extension frame. The frame is signed, and
    /// peers that do not know `namespace` verify, relay, and retain it
    /// anyway — an extension propagates through peers that predate it, and
    /// late subscribers replay it from retained history.
    ///
    /// The payload is opaque to the protocol: never interpreted, never
    /// validated beyond the [`MAX_BODY_SIZE`] cap, and untrusted on
    /// receipt. Our own frames are not delivered back to our
    /// [`Self::on_extension`] subscribers.
    ///
    /// Kind numbers are the core spec's; applications only ever choose a
    /// namespace, their own `local_kind`, and their `schema_version`.
    pub async fn send_extension(
        &self,
        namespace: [u8; 16],
        local_kind: u32,
        schema_version: u16,
        payload: Bytes,
    ) -> Result<(), DropError> {
        if payload.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(payload.len()).into());
        }
        self.broadcast(MessageBodyV1::Extension(ExtensionV1 {
            namespace,
            local_kind,
            schema_version,
            payload: payload.to_vec(),
        }))
        .await
    }

    /// Subscribe to verified frames of an extension namespace: everything
    /// already in the retained history (including what catch-up sync pulled
    /// before this session joined), followed by everything that arrives
    /// live.
    ///
    /// Frames arrive exactly once in practice (the session's dedup cache
    /// separates replay from live delivery), but extensions should treat
    /// delivery as at-least-once and stay idempotent — [`ExtensionFrame::id`]
    /// exists for that. A lagging receiver loses frames rather than slowing
    /// the session; subscribing again replays what the bounded retained log
    /// still holds.
    ///
    /// Cost note: replay decodes the retained log (bounded by the history
    /// cap, worst case a few thousand frames) to filter by namespace.
    pub fn on_extension(&self, namespace: [u8; 16]) -> broadcast::Receiver<ExtensionFrame> {
        let tx = {
            let mut sinks = self.inner.extension_sinks.lock();
            sinks
                .entry(namespace)
                .or_insert_with(|| broadcast::channel(EXTENSION_CHANNEL_CAPACITY).0)
                .clone()
        };
        // Subscribe before replaying: a live frame can only be delivered
        // once, because anything already retained is also already deduped
        // against a live re-delivery.
        let rx = tx.subscribe();
        let retained = self.inner.state.read().retained_frames();
        for frame in retained {
            let Ok(verified) = decode_for_session(&self.inner, &frame) else {
                continue;
            };
            if let Some(MessageBodyV1::Extension(ext)) = verified.body {
                if ext.namespace == namespace {
                    let _ = tx.send(ExtensionFrame {
                        author: verified.author,
                        id: verified.message.id,
                        namespace: ext.namespace,
                        local_kind: ext.local_kind,
                        schema_version: ext.schema_version,
                        payload: Bytes::from(ext.payload),
                    });
                }
            }
        }
        rx
    }

    /// The shared internals, for crate-internal helpers (catch-up sync).
    pub(crate) fn inner_handle(&self) -> Arc<SessionInner> {
        Arc::clone(&self.inner)
    }

    fn emit(&self, event: DropEvent) {
        // Ignore lag: slow CLI consumers must not backpressure the session.
        let _ = self.inner.events.send(event);
    }

    fn set_status(&self, hash: &BlobHash, status: LocalBlobStatus) {
        let mut state = self.inner.state.write();
        if let Some(record) = state.offers.get_mut(hash) {
            record.local_status = status;
        }
    }

    /// Broadcast an arbitrary payload on the drop's gossip topic.
    ///
    /// Hidden from the docs: intended for integration tests that inject
    /// malformed, oversized, or forged messages to prove the session
    /// survives them.
    #[doc(hidden)]
    pub async fn inject_raw_message(&self, bytes: Bytes) -> Result<(), DropError> {
        self.inner.transport.broadcast(bytes).await
    }

    /// Announce withdrawal for everything we serve, without stopping. A
    /// deliberate, polite leave uses this before [`Self::shutdown_no_announce`];
    /// a crash cannot, which is the case `publisher_exit.rs` covers.
    pub async fn announce_withdrawal(&self) {
        let complete: Vec<BlobHash> = {
            let state = self.inner.state.read();
            state
                .offers
                .iter()
                .filter(|(_, r)| r.local_status == LocalBlobStatus::Complete)
                .map(|(h, _)| *h)
                .collect()
        };
        for hash in complete {
            let _ = n0_future::time::timeout(
                Duration::from_secs(1),
                self.broadcast(MessageBodyV1::Provider(ProviderV1 {
                    blob_hash: hash,
                    state: ProviderState::Withdrawing,
                    announced_at_ms: Some(now_ms()),
                })),
            )
            .await;
        }
    }

    /// Shut the session down: announce withdrawal for everything we serve,
    /// then stop the gossip loop and all fetch tasks.
    pub async fn shutdown(self) -> Result<(), DropError> {
        self.announce_withdrawal().await;
        self.shutdown_no_announce().await;
        Ok(())
    }

    /// Every retained signed frame — the replayable history of this drop,
    /// suitable for persisting so a restart can reconstruct the drop rather
    /// than merely its blob cache. Bounded by the retained-history cap.
    pub fn export_history(&self) -> Vec<Bytes> {
        let mut frames = Vec::new();
        let mut cursor = 0u64;
        loop {
            let page = self.inner.state.read().sync_frames(cursor, 256);
            if page.frames.is_empty() {
                break;
            }
            cursor = page.end_cursor;
            frames.extend(page.frames);
            if page.caught_up {
                break;
            }
        }
        frames
    }

    /// Replay persisted frames into state after a restart. Frames are verified
    /// by the same `MessageV1::decode` as live traffic and applied through the
    /// same `DropState` transitions — but as *history*, not live traffic: no
    /// peer credit, no decider, no auto-fetch, no events, no rebroadcast. Live
    /// gossip re-propagates whatever is still current.
    ///
    /// Returns how many frames applied.
    pub async fn restore_history(&self, frames: Vec<Bytes>) -> usize {
        let mut applied = 0;
        for frame in frames {
            let Ok(verified) = MessageV1::decode(&frame, &self.inner.topic_id) else {
                continue;
            };
            let author = verified.author;
            let message = verified.message;
            let mut state = self.inner.state.write();
            if !state.seen_messages.check_and_insert((author, message.id)) {
                continue;
            }
            match message.body.decode() {
                Ok(Some(MessageBodyV1::Offer(offer))) => {
                    state.retain_frame(Bytes::copy_from_slice(&frame));
                    state.record_restored(author, offer);
                    applied += 1;
                }
                Ok(Some(MessageBodyV1::Provider(provider))) => {
                    state.retain_frame(Bytes::copy_from_slice(&frame));
                    let announced_at = provider.announced_at_ms.unwrap_or(0);
                    match provider.state {
                        ProviderState::Available => {
                            state.record_provider(
                                provider.blob_hash,
                                author,
                                announced_at,
                                message.id,
                            );
                        }
                        ProviderState::Withdrawing => {
                            state.withdraw_provider(
                                provider.blob_hash,
                                author,
                                announced_at,
                                message.id,
                            );
                        }
                    }
                    applied += 1;
                }
                Ok(Some(_)) | Ok(None) => {
                    // Requests and unknown kinds are not retained history; skip.
                }
                Err(_) => {}
            }
        }
        applied
    }

    /// Re-announce that we serve `hash` — used after a restart restores state,
    /// so the group learns our availability again without a new fetch. Only
    /// announces when the blob is actually complete in the local store.
    pub async fn reannounce(&self, hash: &BlobHash) -> Result<(), DropError> {
        self.mark_complete(hash, None).await?;
        self.broadcast(MessageBodyV1::Provider(ProviderV1 {
            blob_hash: *hash,
            state: ProviderState::Available,
            announced_at_ms: Some(now_ms()),
        }))
        .await?;
        Ok(())
    }

    /// Stop without announcing withdrawal (e.g. when we are not serving).
    pub async fn shutdown_no_announce(&self) {
        let _ = self.guard.0.send(true);
        // Swap the task set out so the event loop (which may concurrently be
        // spawning a fetch task) never deadlocks against this join, and so
        // late auto-fetch spawns simply run detached.
        let mut tasks = {
            let mut guard = self.inner.tasks.lock();
            std::mem::take(&mut *guard)
        };
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                debug!("session task ended with error: {e}");
            }
        }
    }
}

/// The gossip receive loop. Runs until shutdown is signalled or the gossip
/// stream ends.
async fn run_event_loop(
    inner: Arc<SessionInner>,
    mut events: TransportEventStream,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                trace!("transport event loop: shutdown signalled");
                break;
            }
            item = events.next() => {
                match item {
                    Some(event) => handle_transport_event(&inner, event).await,
                    None => {
                        debug!("transport event stream ended");
                        break;
                    }
                }
            }
        }
    }
}

/// Decode an incoming frame in the session's wire family: sealed (3) for
/// private drops, plain (2) for public ones. A frame from the *other*
/// family fails cleanly — `UnsupportedVersion` — which the error arm of
/// the caller reports as an observable warning.
pub(crate) fn decode_for_session(
    inner: &Arc<SessionInner>,
    content: &Bytes,
) -> Result<crate::message::VerifiedMessage, ProtocolError> {
    match (inner.mode, &inner.drop_key) {
        (crate::ticket::DropMode::Sealed, Some(key)) => {
            MessageV1::decode_sealed(content, key, &inner.topic_id)
        }
        // Blind relay: verify the outer signature so dedup, retention, and
        // relaying work, but the body stays sealed.
        (crate::ticket::DropMode::Sealed, None) => {
            MessageV1::verify_sealed_outer(content, &inner.topic_id)
        }
        (crate::ticket::DropMode::Public, _) => MessageV1::decode(content, &inner.topic_id),
    }
}

async fn handle_transport_event(inner: &Arc<SessionInner>, event: TransportEvent) {
    match event {
        TransportEvent::NeighborUp(peer) => {
            {
                let mut state = inner.state.write();
                state.known_peers.insert(peer);
            }
            inner.neighbors.fetch_add(1, Ordering::SeqCst);
            inner.neighbor_notify.notify_waiters();
            let _ = inner.events.send(DropEvent::PeerJoined { peer });
            maybe_sync_from_neighbor(inner, peer);
        }
        TransportEvent::NeighborDown(peer) => {
            // One atomic step, never load-then-store: the gossip event loop is
            // the only writer today, but a saturating fetch_update keeps that
            // assumption from becoming a race the day a second writer appears.
            inner
                .neighbors
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    Some(n.saturating_sub(1))
                })
                .ok();
            let _ = inner.events.send(DropEvent::PeerLeft { peer });
        }
        TransportEvent::Received {
            content,
            delivered_from,
        } => match decode_for_session(inner, &content) {
            Ok(verified) => {
                handle_message(
                    inner,
                    verified.author,
                    verified.message,
                    verified.body,
                    delivered_from,
                    content.clone(),
                )
                .await
            }
            Err(e) => {
                let (reason, warning) = match &e {
                    ProtocolError::UnsupportedVersion(v) => (
                        RejectReason::UnsupportedVersion(*v),
                        ProtocolWarningKind::UnsupportedVersion { version: *v },
                    ),
                    ProtocolError::InvalidSignature => (
                        RejectReason::InvalidSignature,
                        ProtocolWarningKind::InvalidSignature,
                    ),
                    ProtocolError::MessageTooLarge(size) => (
                        RejectReason::Malformed(e.to_string()),
                        ProtocolWarningKind::Oversized { size: *size },
                    ),
                    ProtocolError::InvalidName(m) => (
                        RejectReason::InvalidName(m.clone()),
                        ProtocolWarningKind::Malformed {
                            reason: e.to_string(),
                        },
                    ),
                    other => (
                        RejectReason::Malformed(other.to_string()),
                        ProtocolWarningKind::Malformed {
                            reason: other.to_string(),
                        },
                    ),
                };
                let _ = inner.events.send(DropEvent::OfferRejected {
                    from: delivered_from,
                    reason,
                });
                let _ = inner.events.send(DropEvent::ProtocolWarning {
                    from: Some(delivered_from),
                    warning,
                });
            }
        },
        TransportEvent::Lagged => {
            let _ = inner.events.send(DropEvent::ProtocolWarning {
                from: None,
                warning: ProtocolWarningKind::Lagged,
            });
        }
    }
}

/// Verify-level message dispatch, shared by the gossip receive path and
/// catch-up sync replays. `raw` is the original signed frame, retained for
/// sync serving when it carries an offer or provider announcement.
/// Anti-entropy: a neighbor that just appeared may hold history we missed
/// while they were gone (join-time catch-up only covers the ticket's
/// bootstrap set, only at join). Pull from them; they see the same
/// neighbor-up and pull from us, so both directions converge. Replay goes
/// through the standard verify + dedupe path, so an up-to-date pull costs
/// one round trip and applies nothing.
fn maybe_sync_from_neighbor(inner: &Arc<SessionInner>, peer: EndpointId) {
    {
        let mut recent = inner.synced_from.lock();
        if recent.len() >= ANTI_ENTROPY_PEERS_CAP {
            recent.clear();
        }
        let now = Instant::now();
        if let Some(last) = recent.get(&peer) {
            if now.duration_since(*last) < ANTI_ENTROPY_COOLDOWN {
                return;
            }
        }
        recent.insert(peer, now);
    }
    // Id-only address on purpose: we are already gossiping with this peer,
    // so discovery has just demonstrated it can resolve them.
    inner.clone().spawn_task(crate::sync::sync_catchup(
        inner.clone(),
        vec![EndpointAddr::from(peer)],
    ));
}

pub(crate) async fn handle_message(
    inner: &Arc<SessionInner>,
    author: EndpointId,
    message: MessageV1,
    body: Option<MessageBodyV1>,
    delivered_from: EndpointId,
    raw: Bytes,
) {
    let self_id = inner.stack.endpoint.id();
    let kind = message.body.kind;
    // Offers and provider announcements are the shared history worth keeping.
    // Unknown kinds are retained too, within a small budget, so this peer can
    // still relay extensions it does not implement.
    let retainable = match &body {
        Some(MessageBodyV1::Offer(_)) | Some(MessageBodyV1::Provider(_)) => Retain::Yes,
        // Extensions get relay duty, not history: the unknown-frame budget
        // bounds them, and late subscribers can replay what is retained.
        Some(MessageBodyV1::Extension(_)) => Retain::Unknown,
        Some(_) => Retain::No,
        None => Retain::Unknown,
    };
    // Flood control comes before anything expensive we can avoid.
    if !inner.message_limiter.lock().allow(delivered_from) {
        trace!(peer = %delivered_from.fmt_short(), "rate limiting peer");
        let _ = inner.events.send(DropEvent::ProtocolWarning {
            from: Some(delivered_from),
            warning: ProtocolWarningKind::RateLimited,
        });
        return;
    }
    {
        let mut state = inner.state.write();
        // Only the delivering neighbor is a verified *connected* peer. The
        // message author is attributed by signature but may be several
        // gossip hops away; we deliberately do not require the author to be
        // a direct neighbor (that would break multi-hop propagation), and
        // we do not record authors as known peers.
        state.note_peer(delivered_from);
        if !state.seen_messages.check_and_insert((author, message.id)) {
            trace!(%author, "deduplicated message");
            return;
        }
        match retainable {
            Retain::Yes => state.retain_frame(raw),
            Retain::Unknown => state.retain_unknown_frame(raw),
            Retain::No => {}
        }
    }
    let Some(body) = body else {
        // A kind from the future (or from an application extension). We
        // verified it, we will relay it, and we do not pretend to understand
        // it. On a blind relay this is the *normal* case — every sealed
        // frame arrives verified-but-undecrypted — so don't warn there.
        trace!(%author, kind, "ignoring unknown message kind");
        if kind != crate::KIND_SEALED {
            let _ = inner.events.send(DropEvent::ProtocolWarning {
                from: Some(delivered_from),
                warning: ProtocolWarningKind::UnknownKind { kind },
            });
        }
        // Unknown numeric kinds are relayed, never delivered: application
        // protocols ride the namespaced extension envelope, so a bare kind
        // has no subscriber to notify. A future core kind will teach new
        // builds to decode it.
        return;
    };
    match body {
        MessageBodyV1::Offer(offer) => {
            handle_offer(inner, author, offer, self_id, delivered_from).await;
        }
        MessageBodyV1::Provider(provider) => {
            let hash = provider.blob_hash;
            let announced_at = provider.announced_at_ms.unwrap_or(0);
            let applied = {
                let mut state = inner.state.write();
                match provider.state {
                    ProviderState::Available => {
                        state.record_provider(hash, author, announced_at, message.id)
                    }
                    ProviderState::Withdrawing => {
                        state.withdraw_provider(hash, author, announced_at, message.id)
                    }
                }
            };
            // A stale relay (an older assertion arriving late) changes
            // nothing and is not worth an event.
            if applied {
                let event = match provider.state {
                    ProviderState::Available => DropEvent::ProviderAvailable { hash, peer: author },
                    ProviderState::Withdrawing => {
                        DropEvent::ProviderUnavailable { hash, peer: author }
                    }
                };
                let _ = inner.events.send(event);
            }
        }
        MessageBodyV1::Request(request) => {
            handle_request(inner, request.blob_hash, delivered_from).await;
        }
        MessageBodyV1::Extension(ext) => {
            // Deliver to whoever subscribed this namespace. The frame was
            // already retained above, so a missed delivery can still be
            // replayed by a fresh `on_extension`.
            if let Some(tx) = inner.extension_sinks.lock().get(&ext.namespace) {
                let _ = tx.send(ExtensionFrame {
                    author,
                    id: message.id,
                    namespace: ext.namespace,
                    local_kind: ext.local_kind,
                    schema_version: ext.schema_version,
                    payload: Bytes::from(ext.payload),
                });
            }
        }
    }
}

/// Milliseconds since the Unix epoch, for self-asserted ordering.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Whether a frame belongs in the retained history we serve to late joiners.
enum Retain {
    Yes,
    No,
    Unknown,
}

async fn handle_offer(
    inner: &Arc<SessionInner>,
    author: EndpointId,
    offer: OfferV1,
    self_id: EndpointId,
    delivered_from: EndpointId,
) {
    let hash = offer.blob_hash;
    // Ask the application before spending memory on this offer. Policy limits
    // are computed first so the decider can see what the protocol would do.
    let policy_allows_auto_fetch = inner.policy.auto_fetch
        && author != self_id
        && inner
            .policy
            .check_auto_fetch(
                &offer,
                inner.active_fetches.load(Ordering::SeqCst),
                inner.auto_fetch_bytes.load(Ordering::SeqCst),
            )
            .is_ok();
    let is_known = inner.state.read().offers.contains_key(&hash);
    let decision = inner.decider.decide(
        &offer,
        &OfferContext {
            author,
            delivered_from,
            is_new: !is_known,
            policy_allows_auto_fetch,
        },
    );
    if let OfferDecision::Reject(reason) = &decision {
        debug!(%author, %hash, "offer refused by decider: {reason}");
        let _ = inner.events.send(DropEvent::OfferRejected {
            from: author,
            reason: RejectReason::Policy(reason.clone()),
        });
        return;
    }
    let outcome = {
        let mut state = inner.state.write();
        state.record_offer_bounded(author, offer.clone())
    };
    if outcome.is_rejected() {
        debug!(%author, %hash, "offer refused: author is over quota");
        let _ = inner.events.send(DropEvent::OfferRejected {
            from: author,
            reason: RejectReason::QuotaExceeded,
        });
        return;
    }
    if let crate::state::OfferOutcome::New { evicted } = &outcome {
        if !evicted.is_empty() {
            let _ = inner.events.send(DropEvent::ProtocolWarning {
                from: None,
                warning: ProtocolWarningKind::InventoryEvicted {
                    count: evicted.len(),
                },
            });
        }
    }
    let is_new = outcome.is_new();
    if !is_new {
        debug!(%hash, "duplicate offer merged as alias");
        return;
    }
    let _ = inner.events.send(DropEvent::OfferReceived {
        from: author,
        offer: offer.clone(),
    });

    // Auto-fetch evaluation.
    if !inner.policy.auto_fetch || author == self_id {
        return;
    }
    let already = {
        let state = inner.state.read();
        matches!(
            state.offers.get(&hash).map(|r| &r.local_status),
            Some(LocalBlobStatus::Complete) | Some(LocalBlobStatus::Fetching { .. })
        )
    };
    if already {
        return;
    }
    let active = inner.active_fetches.load(Ordering::SeqCst);
    let spent = inner.auto_fetch_bytes.load(Ordering::SeqCst);
    if let Err(e) = inner.policy.check_auto_fetch(&offer, active, spent) {
        // The offer stays in the inventory for a manual fetch; only the
        // automatic pull is refused, and the application is told why.
        let _ = inner.events.send(DropEvent::OfferRejected {
            from: author,
            reason: RejectReason::Policy(e.to_string()),
        });
        return;
    }
    // `RecordOnly` means "remember it, do not pull it".
    if decision != OfferDecision::Accept {
        debug!(%hash, "decider allows the offer but not an automatic fetch");
        return;
    }
    // Extend the session lifetime for the duration of the auto-fetch, but
    // only if a user-facing handle still exists (otherwise we are shutting
    // down and should not start new work).
    let Some(guard) = inner.guard.upgrade() else {
        return;
    };
    // Reserve quota up-front; refunded on failure.
    inner
        .auto_fetch_bytes
        .fetch_add(offer.size, Ordering::SeqCst);
    inner.active_fetches.fetch_add(1, Ordering::SeqCst);
    let session = DropSession {
        inner: Arc::clone(inner),
        guard,
    };
    inner.tasks.lock().spawn(async move {
        let size = offer.size;
        let _permit = session
            .inner
            .fetch_sem
            .acquire()
            .await
            .expect("semaphore open");
        let output = FetchOutput::Directory(session.inner.policy.output_directory.clone());
        // fetch_inner emits FetchFailed itself on terminal failure.
        if session.fetch_inner(hash, output).await.is_err() {
            session
                .inner
                .auto_fetch_bytes
                .fetch_sub(size, Ordering::SeqCst);
        }
        session.inner.active_fetches.fetch_sub(1, Ordering::SeqCst);
    });
}

async fn handle_request(inner: &Arc<SessionInner>, hash: BlobHash, asker: EndpointId) {
    // One peer cannot make us answer endlessly, even for different blobs.
    if !inner.request_limiter.lock().allow(asker) {
        trace!(peer = %asker.fmt_short(), "rate limiting request");
        let _ = inner.events.send(DropEvent::ProtocolWarning {
            from: Some(asker),
            warning: ProtocolWarningKind::RateLimited,
        });
        return;
    }
    // Reply only if we actually have the complete blob.
    let blob_hash: iroh_blobs::Hash = hash.into();
    let complete = matches!(
        inner.stack.store().blobs().status(blob_hash).await,
        Ok(BlobStatus::Complete { .. })
    );
    if !complete {
        return;
    }
    // Rate-limit replies per hash.
    {
        let mut replies = inner.request_replies.lock();
        if let Some(last) = replies.get(&hash) {
            if last.elapsed() < REQUEST_REPLY_INTERVAL {
                return;
            }
        }
        replies.insert(hash, Instant::now());
    }
    let msg = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
        blob_hash: hash,
        state: ProviderState::Available,
        announced_at_ms: Some(now_ms()),
    }));
    match msg.encode(&inner.secret, &inner.topic_id) {
        Ok(bytes) => {
            if let Err(e) = inner.transport.broadcast(Bytes::from(bytes)).await {
                warn!("failed to answer provider request: {e}");
            }
        }
        Err(e) => warn!("failed to encode provider reply: {e}"),
    }
}
