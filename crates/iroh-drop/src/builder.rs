//! Dependency wiring: one shared endpoint, one blob store, explicit router
//! registration, explicit lifecycle ownership.
//!
//! [`DropStack`] constructs the shared infrastructure exactly once:
//!
//! ```text
//!                        iroh-drop
//!                    policy + coordination
//!                       /            \
//!              iroh-gossip        iroh-blobs
//!           membership + offers  storage + transfer
//!                      \            /
//!                 shared Iroh endpoint
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::presets;
use iroh::endpoint_info::UserData;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey};
use iroh_blobs::api::downloader::Downloader;
use iroh_blobs::api::Store;
#[cfg(not(target_arch = "wasm32"))]
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use iroh_blobs::BlobsProtocol;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
#[cfg(feature = "mdns")]
use iroh_mdns_address_lookup::MdnsAddressLookup;
use tracing::{debug, info, warn};

use crate::error::{DropError, NetworkError, PolicyError, StorageError};
use crate::message::MAX_MESSAGE_SIZE;
use crate::policy::{DropPolicy, OfferDecider, PolicyDecider};
use crate::seal::DropKey;
use crate::session::{DropSession, SessionRegistry};
use crate::ticket::{DropTicket, DropTicketOptionsV1};
use crate::transport::{DropTransport, GossipTransport};

/// How to construct the shared endpoint and blob store.
#[derive(Clone, Debug, Default)]
pub struct StackOptions {
    /// Path of a persistent filesystem blob store. `None` uses an in-memory
    /// store whose contents vanish with the process.
    pub store_path: Option<PathBuf>,
    /// Offline mode: no relay servers and no n0 address lookup. Useful for
    /// LANs, tests, and air-gapped demos. Bootstrap addresses from tickets
    /// are still honored through a local in-memory address lookup.
    pub offline: bool,
    /// Path of a persistent endpoint identity (raw 32-byte secret key).
    /// Created with owner-only permissions if missing. `None` generates a
    /// fresh identity per process, so the `EndpointId` changes on restart.
    pub identity_path: Option<PathBuf>,
    /// Endpoint identity as a raw key, for hosts that manage key storage
    /// themselves (a browser persisting to localStorage, a mobile app using
    /// a keychain). Takes precedence over [`Self::identity_path`].
    pub secret_key: Option<SecretKey>,
    /// Announce and resolve endpoint addresses on the local network with
    /// mDNS. This makes peers reachable by id alone on a LAN — including in
    /// [`Self::offline`] mode, where there is no relay or DNS lookup — and
    /// lets higher layers browse what is being shared nearby.
    pub mdns: bool,
}

/// Keeps the concrete blob store alive for the lifetime of the stack.
/// The fields are never read; dropping them closes the underlying store.
#[derive(Debug)]
#[allow(dead_code)]
enum StoreKeeper {
    #[cfg(not(target_arch = "wasm32"))]
    Fs(FsStore),
    Mem(MemStore),
    /// The store belongs to the host (see [`DropStack::from_parts`]).
    External,
}

/// The shared protocol stack: one endpoint, one store, gossip, blobs,
/// downloader, and the router that exposes both protocols.
#[derive(Debug)]
pub struct DropStack {
    /// The one shared Iroh endpoint.
    pub endpoint: Endpoint,
    /// The gossip protocol handle.
    pub gossip: Gossip,
    /// The blobs protocol handle (dereferences to the blob store).
    pub blobs: BlobsProtocol,
    /// `None` when the host owns the router (see [`DropStack::from_parts`]).
    router: Option<Router>,
    downloader: Downloader,
    /// `None` when the host manages address lookup itself.
    lookup: Option<MemoryLookup>,
    store: Store,
    _keeper: StoreKeeper,
    offline: bool,
    sessions: SessionRegistry,
    #[cfg(feature = "mdns")]
    mdns: Option<MdnsAddressLookup>,
}

impl DropStack {
    /// Build the full stack and spawn the protocol router.
    pub async fn new(options: StackOptions) -> Result<Self, DropError> {
        let lookup = MemoryLookup::new();
        let secret = match (&options.secret_key, &options.identity_path) {
            (Some(key), _) => key.clone(),
            (None, Some(path)) => load_or_create_identity(path)?,
            (None, None) => SecretKey::generate(),
        };
        // The mDNS service needs our endpoint id, which we know from the
        // secret key before the endpoint exists.
        #[cfg(not(feature = "mdns"))]
        if options.mdns {
            warn!("mdns requested but this build lacks the `mdns` feature; continuing without it");
        }
        #[cfg(feature = "mdns")]
        let mdns = if options.mdns {
            let endpoint_id = iroh::EndpointId::from(secret.public());
            match MdnsAddressLookup::builder().build(endpoint_id) {
                Ok(mdns) => Some(mdns),
                Err(e) => {
                    // A machine without usable multicast is not a fatal
                    // condition: carry on without local discovery.
                    warn!("local network discovery unavailable: {e}");
                    None
                }
            }
        } else {
            None
        };

        #[cfg(feature = "mdns")]
        let mut builder = if options.offline {
            Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Disabled)
                .address_lookup(lookup.clone())
                .secret_key(secret)
        } else {
            Endpoint::builder(presets::N0)
                .address_lookup(lookup.clone())
                .secret_key(secret)
        };
        #[cfg(not(feature = "mdns"))]
        let builder = if options.offline {
            Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Disabled)
                .address_lookup(lookup.clone())
                .secret_key(secret)
        } else {
            Endpoint::builder(presets::N0)
                .address_lookup(lookup.clone())
                .secret_key(secret)
        };
        #[cfg(feature = "mdns")]
        if let Some(mdns) = &mdns {
            builder = builder.address_lookup(mdns.clone());
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|e| DropError::Network(NetworkError::Endpoint(e.to_string())))?;

        #[cfg(not(target_arch = "wasm32"))]
        let (store, keeper) = match &options.store_path {
            Some(path) => {
                let fs = FsStore::load(path)
                    .await
                    .map_err(|e| DropError::Storage(StorageError::Store(e.to_string())))?;
                (Store::clone(&fs), StoreKeeper::Fs(fs))
            }
            None => {
                let mem = MemStore::new();
                (Store::clone(&mem), StoreKeeper::Mem(mem))
            }
        };
        // Browsers have no filesystem; blobs live in memory only.
        #[cfg(target_arch = "wasm32")]
        let (store, keeper) = {
            if options.store_path.is_some() {
                return Err(DropError::Storage(StorageError::Store(
                    "filesystem blob store is not supported on wasm32".into(),
                )));
            }
            let mem = MemStore::new();
            (Store::clone(&mem), StoreKeeper::Mem(mem))
        };

        let blobs = BlobsProtocol::new(&store, None);
        let gossip = Gossip::builder()
            .max_message_size(MAX_MESSAGE_SIZE)
            .spawn(endpoint.clone());
        let downloader = store.downloader(&endpoint);

        let sessions: SessionRegistry = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            // Live coordination rides gossip; `/iroh-drop/1` is the control
            // channel, currently serving catch-up sync for late joiners.
            .accept(
                crate::sync::SYNC_ALPN,
                crate::sync::protocol(sessions.clone()),
            )
            .spawn();

        Ok(Self {
            endpoint,
            gossip,
            blobs,
            router: Some(router),
            downloader,
            lookup: Some(lookup),
            store,
            _keeper: keeper,
            offline: options.offline,
            sessions,
            #[cfg(feature = "mdns")]
            mdns,
        })
    }

    /// The local-network discovery service, when enabled.
    ///
    /// Higher layers use this to see which peers are nearby and what they
    /// advertise; see [`Self::advertise`]. Requires the `mdns` feature.
    #[cfg(feature = "mdns")]
    pub fn mdns(&self) -> Option<&MdnsAddressLookup> {
        self.mdns.as_ref()
    }

    /// Attach a short, public, untrusted string to our address records.
    ///
    /// Whatever is passed here is visible to anyone who can see our discovery
    /// records (the local network for mDNS, the pkarr/DNS service when
    /// online), so callers must treat it as a broadcast. Pass `None` to stop
    /// advertising. Strings longer than [`UserData::MAX_LENGTH`] are
    /// rejected.
    pub fn advertise(&self, data: Option<&str>) -> Result<(), DropError> {
        let user_data = match data {
            None => None,
            Some(text) => Some(UserData::try_from(text.to_string()).map_err(|e| {
                DropError::Network(NetworkError::Endpoint(format!(
                    "cannot advertise {} bytes: {e}",
                    text.len()
                )))
            })?),
        };
        self.endpoint.set_user_data_for_address_lookup(user_data);
        Ok(())
    }

    /// Build a stack from components the caller already has.
    ///
    /// This is the embedding path: an application that already runs an iroh
    /// endpoint with gossip and blobs — its own router, its own protocols —
    /// can add drops without `iroh-drop` insisting on building a second
    /// network stack. The caller keeps ownership of the router and endpoint,
    /// and must register [`DropStack::sync_handler`] itself to serve catch-up
    /// sync.
    ///
    /// Address lookup: gossip bootstraps by endpoint *id*, so ticket
    /// addresses can only be used if something resolves them. Pass a
    /// [`MemoryLookup`] that is also registered on the endpoint to get the
    /// same behaviour as [`DropStack::new`]; pass `None` if the host's own
    /// discovery covers it.
    pub fn from_parts(
        endpoint: Endpoint,
        gossip: Gossip,
        blobs: BlobsProtocol,
        store: Store,
        lookup: Option<MemoryLookup>,
    ) -> Self {
        let downloader = store.downloader(&endpoint);
        Self {
            endpoint,
            gossip,
            blobs,
            router: None,
            downloader,
            lookup,
            store: Store::clone(&store),
            _keeper: StoreKeeper::External,
            offline: false,
            sessions: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            #[cfg(feature = "mdns")]
            mdns: None,
        }
    }

    /// The catch-up sync accept handler, for hosts that own their router.
    ///
    /// Register it under [`crate::DROP_ALPN`]:
    ///
    /// ```ignore
    /// let router = Router::builder(endpoint)
    ///     .accept(iroh_drop::DROP_ALPN, stack.sync_handler())
    ///     .spawn();
    /// ```
    ///
    /// Without it, this peer still publishes and fetches; it just cannot
    /// serve history to late joiners.
    pub fn sync_handler(&self) -> impl iroh::protocol::ProtocolHandler {
        crate::sync::protocol(Arc::clone(&self.sessions))
    }

    /// Live sessions by topic, for the catch-up sync accept handler.
    pub(crate) fn session_registry(&self) -> &SessionRegistry {
        &self.sessions
    }

    /// The shared blob store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// The shared downloader.
    pub fn downloader(&self) -> &Downloader {
        &self.downloader
    }

    /// Whether this stack runs without relays and n0 address lookup.
    pub fn is_offline(&self) -> bool {
        self.offline
    }

    /// Seed the local address lookup with a known peer address (e.g. from a
    /// ticket). This is how bootstrap peers become dialable.
    pub fn add_known_addr(&self, addr: EndpointAddr) {
        match &self.lookup {
            Some(lookup) => lookup.add_endpoint_info(addr),
            // The host owns discovery; a ticket's addresses are still used
            // for direct dials, but gossip bootstrap relies on the host's
            // own lookup services.
            None => debug!("no address lookup configured; ignoring ticket address"),
        }
    }

    /// Wait until the endpoint is online (connected to a relay and
    /// published). Returns immediately in offline mode.
    pub async fn wait_online(&self) {
        if !self.offline {
            self.endpoint.online().await;
        }
    }

    /// Our current endpoint address, for embedding into tickets.
    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    /// Shut down the router and the endpoint.
    ///
    /// A stack built with [`DropStack::from_parts`] does not own either, so
    /// this is a no-op there: the host shuts its own stack down.
    pub async fn shutdown(self) -> Result<(), DropError> {
        if let Some(router) = self.router {
            router
                .shutdown()
                .await
                .map_err(|e| DropError::Network(NetworkError::Endpoint(e.to_string())))?;
        }
        Ok(())
    }
}

/// Load a persistent endpoint identity, creating it if missing.
///
/// The file holds the raw 32-byte secret key and is created with owner-only
/// permissions on unix. A stable identity is what lets peers recognize each
/// other across restarts.
fn load_or_create_identity(path: &Path) -> Result<SecretKey, DropError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| {
                DropError::Storage(StorageError::InvalidPath(format!(
                    "identity file {} is not a 32-byte secret key",
                    path.display()
                )))
            })?;
            Ok(SecretKey::from_bytes(&bytes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let secret = SecretKey::generate();
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent)
                    .map_err(|e| DropError::Storage(StorageError::Io(e.to_string())))?;
            }
            std::fs::write(path, secret.to_bytes())
                .map_err(|e| DropError::Storage(StorageError::Io(e.to_string())))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
            }
            info!(path = %path.display(), "created persistent identity");
            Ok(secret)
        }
        Err(e) => Err(DropError::Storage(StorageError::Io(e.to_string()))),
    }
}

/// Options for creating a new drop.
#[derive(Clone, Debug, Default)]
pub struct CreateOptions {
    /// Untrusted display name embedded in the ticket.
    pub display_name: Option<String>,
    /// Whether the ticket recommends automatic fetching to joiners.
    pub auto_fetch_recommended: bool,
    /// Create a private drop: the ticket carries a fresh drop key and every
    /// frame is sealed (wire family 3 — see `docs/protocol.md`, "Private
    /// drops"). Rotation story: rotate = new drop.
    pub private: bool,
}

/// Builder for a [`DropProtocol`] service over an existing [`DropStack`].
/// Builds a session's gossip carrier. Receives the session's stack, the
/// drop's topic, and the bootstrap peer ids (empty on create).
pub type TransportFactory = Arc<
    dyn Fn(Arc<DropStack>, TopicId, Vec<EndpointId>) -> Result<Arc<dyn DropTransport>, DropError>
        + Send
        + Sync,
>;

pub struct DropBuilder {
    stack: Arc<DropStack>,
    policy: DropPolicy,
    decider: Arc<dyn OfferDecider>,
    transport_factory: Option<TransportFactory>,
}

impl DropBuilder {
    /// Wrap an existing, already-wired stack.
    ///
    /// The stack owns the shared endpoint, blob store, gossip and blobs
    /// protocol handles; `iroh-drop` never creates a hidden networking stack.
    pub fn new(stack: Arc<DropStack>) -> Self {
        Self {
            stack,
            policy: DropPolicy::default(),
            decider: Arc::new(PolicyDecider),
            transport_factory: None,
        }
    }

    /// Convenience: build a stack and the protocol service in one call.
    pub async fn from_options(options: StackOptions) -> Result<Self, DropError> {
        let stack = Arc::new(DropStack::new(options).await?);
        Ok(Self::new(stack))
    }

    /// Set the fetch policy.
    pub fn policy(mut self, policy: DropPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Install a decision hook for incoming offers.
    ///
    /// Runs after verification and policy limits, so it can only be more
    /// conservative than the policy — an allowlist, a user prompt, an
    /// application-specific quota.
    pub fn decider(mut self, decider: Arc<dyn OfferDecider>) -> Self {
        self.decider = decider;
        self
    }

    /// Override the gossip carrier for every session this builder starts.
    ///
    /// The protocol's semantics — signatures, dedup, retention, limits —
    /// are carrier-independent; sessions only need what [`DropTransport`]
    /// promises. Production uses [`GossipTransport`] (the default, no
    /// factory); tests substitute an in-memory transport, which makes the
    /// protocol-logic suite fast and deterministic. Note the carrier only
    /// replaces gossip: blob transfer and catch-up sync still use the
    /// stack's real endpoint.
    pub fn transport_factory(mut self, factory: TransportFactory) -> Self {
        self.transport_factory = Some(factory);
        self
    }

    /// Finish building the shared protocol service.
    pub async fn build(self) -> Result<DropProtocol, DropError> {
        Ok(DropProtocol {
            stack: self.stack,
            policy: self.policy,
            decider: self.decider,
            transport_factory: self.transport_factory,
        })
    }
}

/// The shared drop protocol service. Create or join any number of drops on
/// one stack; each gets its own [`DropSession`].
pub struct DropProtocol {
    stack: Arc<DropStack>,
    policy: DropPolicy,
    decider: Arc<dyn OfferDecider>,
    transport_factory: Option<TransportFactory>,
}

impl DropProtocol {
    /// The shared stack this service runs on.
    pub fn stack(&self) -> &Arc<DropStack> {
        &self.stack
    }

    /// The policy new sessions are created with.
    pub fn policy(&self) -> &DropPolicy {
        &self.policy
    }

    /// How many live sessions one protocol instance supports. Sessions hold
    /// retained history, dedup state, and gossip subscriptions; an
    /// application creating unbounded drops must be refused, not allowed to
    /// exhaust memory one topic at a time.
    pub const MAX_SESSIONS: usize = 64;

    /// Refuse a new session when the instance is already at capacity.
    fn check_session_capacity(&self) -> Result<(), DropError> {
        let registry = self.stack.session_registry().read();
        let active = registry
            .values()
            .filter(|weak| weak.upgrade().is_some())
            .count();
        if active >= Self::MAX_SESSIONS {
            return Err(DropError::Policy(PolicyError::TooManySessions {
                active,
                max: Self::MAX_SESSIONS,
            }));
        }
        Ok(())
    }

    /// Create a new drop: generate a random topic, subscribe, and return a
    /// session with a shareable ticket.
    pub async fn create(&self, options: CreateOptions) -> Result<DropSession, DropError> {
        self.check_session_capacity()?;
        let topic_id = TopicId::from_bytes(rand::random());
        let drop_key = options.private.then(DropKey::generate);
        let ticket_options = DropTicketOptionsV1 {
            auto_fetch_recommended: options.auto_fetch_recommended,
            display_name: options.display_name,
        };
        let ticket = match &drop_key {
            Some(key) => DropTicket::new_private(
                *topic_id.as_bytes(),
                vec![self.stack.addr()],
                ticket_options,
                key.clone(),
            ),
            None => DropTicket::new(
                *topic_id.as_bytes(),
                vec![self.stack.addr()],
                ticket_options,
            ),
        };
        let transport = self.session_transport(topic_id, vec![]).await?;
        info!(topic = %topic_id.fmt_short(), private = drop_key.is_some(), "drop created");
        Ok(DropSession::new(
            Arc::clone(&self.stack),
            self.policy.clone(),
            Arc::clone(&self.decider),
            topic_id,
            ticket,
            transport,
            drop_key,
        ))
    }

    /// The carrier for a new session: the transport factory if one was
    /// installed, otherwise iroh-gossip over the shared endpoint.
    async fn session_transport(
        &self,
        topic_id: TopicId,
        bootstrap: Vec<iroh::EndpointId>,
    ) -> Result<Arc<dyn DropTransport>, DropError> {
        if let Some(factory) = &self.transport_factory {
            return factory(Arc::clone(&self.stack), topic_id, bootstrap);
        }
        let topic = self
            .stack
            .gossip
            .subscribe(topic_id, bootstrap)
            .await
            .map_err(|e| DropError::Network(NetworkError::Gossip(e.to_string())))?;
        let (sender, receiver) = topic.split();
        Ok(Arc::new(GossipTransport::new(sender, receiver)))
    }

    /// Join an existing drop from a ticket.
    pub async fn join(&self, ticket: DropTicket) -> Result<DropSession, DropError> {
        self.check_session_capacity()?;
        // Seed the address lookup so bootstrap peers are dialable.
        for addr in ticket.bootstrap_nodes() {
            self.stack.add_known_addr(addr.clone());
        }
        let topic_id = TopicId::from_bytes(ticket.topic_id());
        let bootstrap: Vec<iroh::EndpointId> =
            ticket.bootstrap_nodes().iter().map(|a| a.id).collect();
        let transport = self.session_transport(topic_id, bootstrap).await?;
        let drop_key = ticket.drop_key();
        info!(topic = %topic_id.fmt_short(), private = drop_key.is_some(), "drop joined");
        let session = DropSession::new(
            Arc::clone(&self.stack),
            self.policy.clone(),
            Arc::clone(&self.decider),
            topic_id,
            ticket.clone(),
            transport,
            drop_key,
        );
        // Gossip has no history: ask the bootstrap peers for the offers and
        // provider announcements that predate us. Best-effort and bounded;
        // live gossip continues regardless.
        let inner = session.inner_handle();
        let peers = ticket.bootstrap_nodes().to_vec();
        if !peers.is_empty() {
            inner
                .clone()
                .spawn_task(crate::sync::sync_catchup(inner, peers));
        }
        Ok(session)
    }

    /// Shut the whole stack down (router, endpoint, store).
    pub async fn shutdown(self) -> Result<(), DropError> {
        match Arc::try_unwrap(self.stack) {
            Ok(stack) => stack.shutdown().await,
            Err(_) => {
                // Sessions still hold references; dropping the router handle
                // stops accepting, and the endpoint closes with the last
                // reference. Nothing more to do for the MVP.
                Ok(())
            }
        }
    }
}
