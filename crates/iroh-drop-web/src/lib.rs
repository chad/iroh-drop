//! wasm-bindgen wrapper around [`iroh_drop`] for browsers.
//!
//! The browser node is relay-only (no QUIC, no mDNS) and keeps blobs in an
//! in-memory store, so it is a *leaf*: it can share and receive, but it is
//! not a long-lived provider — desktop members keep content available.
//!
//! JS shape (names are preserved, snake_case):
//!
//! ```js
//! const drop = await WebDrop.start(identityOrNull);   // 32 bytes | null
//! localStorage.setItem(key, toB64(drop.identity()));  // persist identity
//! const session = await drop.join(ticketString);      // or drop.create(name)
//! session.on_event((ev) => { ... });                  // {kind: "offerReceived", ...}
//! const hash = await session.publish(name, u8, type); // share
//! const u8 = await session.fetch(hash);               // receive
//! ```

use std::str::FromStr;
use std::sync::Arc;

use iroh_drop::{
    BlobHash, CreateOptions, DropBuilder, DropEvent, DropProtocol, DropSession, DropStack,
    DropTicket, FetchOutput, LocalBlobStatus, SecretKey, StackOptions,
};
use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Upper bound on a single fetch/publish. The store is in-memory anyway;
/// this just fails fast on absurd sizes.
const MAX_BLOB_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn js_err(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

/// A running iroh-drop stack (endpoint, gossip, blobs). One per page.
#[wasm_bindgen]
pub struct WebDrop {
    protocol: DropProtocol,
}

#[wasm_bindgen]
impl WebDrop {
    /// Start the stack. `identity` is the 32-byte secret key returned by
    /// [`WebDrop::identity`] on a previous run, or `null` for a fresh one.
    ///
    /// `relay_url` selects a relay server (e.g. a self-hosted
    /// `https://relay.example.com`); `null`/`undefined` uses iroh's
    /// defaults (n0's public relays, which rate-limit large transfers —
    /// self-host for anything heavy).
    pub async fn start(
        identity: Option<Vec<u8>>,
        relay_url: Option<String>,
    ) -> Result<WebDrop, JsError> {
        console_error_panic_hook::set_once();
        let secret_key = match identity {
            Some(bytes) => {
                let bytes: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| JsError::new("identity must be 32 bytes"))?;
                Some(SecretKey::from_bytes(&bytes))
            }
            None => None,
        };
        let options = StackOptions {
            secret_key,
            relay_url,
            ..Default::default()
        };
        let protocol = DropBuilder::from_options(options)
            .await
            .map_err(js_err)?
            .build()
            .await
            .map_err(js_err)?;
        Ok(WebDrop { protocol })
    }

    /// The 32-byte secret key. Persist it (e.g. localStorage) to keep a
    /// stable identity across page loads.
    pub fn identity(&self) -> Vec<u8> {
        self.stack().endpoint.secret_key().to_bytes().to_vec()
    }

    /// Our endpoint id, hex. Handy for debugging; never shown to users.
    pub fn endpoint_id(&self) -> String {
        self.stack().endpoint.id().to_string()
    }

    /// Create a new drop; share `session.ticket()` (or a link containing it).
    pub async fn create(&self, display_name: Option<String>) -> Result<WebSession, JsError> {
        let session = self
            .protocol
            .create(CreateOptions {
                display_name,
                ..Default::default()
            })
            .await
            .map_err(js_err)?;
        Ok(WebSession { session })
    }

    /// Join an existing drop from a ticket string (`drop2…`).
    pub async fn join(&self, ticket: &str) -> Result<WebSession, JsError> {
        let ticket = DropTicket::from_string_prefixed(ticket).map_err(js_err)?;
        let session = self.protocol.join(ticket).await.map_err(js_err)?;
        Ok(WebSession { session })
    }
}

impl WebDrop {
    fn stack(&self) -> &Arc<DropStack> {
        self.protocol.stack()
    }
}

/// One drop session (created or joined).
#[wasm_bindgen]
pub struct WebSession {
    session: DropSession,
}

#[wasm_bindgen]
impl WebSession {
    /// The ticket string to share (`drop1…`).
    pub fn ticket(&self) -> String {
        self.session.ticket().to_string()
    }

    /// Our endpoint id within this session.
    pub fn self_id(&self) -> String {
        self.session.self_id().to_string()
    }

    /// Current offers as an array of
    /// `{hash, name, size, mediaType, from, have}`.
    pub fn offers(&self) -> Result<JsValue, JsError> {
        let offers: Vec<JsOffer> = self
            .session
            .offers()
            .into_iter()
            .map(|record| JsOffer {
                hash: record.offer.blob_hash.to_string(),
                name: record.display_name().to_string(),
                size: record.offer.size,
                media_type: record.offer.media_type.clone(),
                from: record.first_seen_from.to_string(),
                have: matches!(record.local_status, LocalBlobStatus::Complete),
            })
            .collect();
        serde_wasm_bindgen::to_value(&offers).map_err(js_err)
    }

    /// Publish bytes under a display name. Every member is offered them.
    /// Returns the content hash (hex) used with [`WebSession::fetch`].
    pub async fn publish(
        &self,
        name: String,
        data: Vec<u8>,
        media_type: Option<String>,
    ) -> Result<String, JsError> {
        if data.len() as u64 > MAX_BLOB_BYTES {
            return Err(JsError::new("blob exceeds 2 GiB in-memory limit"));
        }
        let published = self
            .session
            .publish_bytes_as(name, data.into(), media_type)
            .await
            .map_err(js_err)?;
        Ok(published.hash.to_string())
    }

    /// Fetch a blob (verified against its hash) and return its bytes.
    /// The browser also becomes a provider for as long as the page lives.
    pub async fn fetch(&self, hash: &str) -> Result<Vec<u8>, JsError> {
        let hash = BlobHash::from_str(hash).map_err(js_err)?;
        self.session
            .fetch(hash, FetchOutput::Store)
            .await
            .map_err(js_err)?;
        let bytes = self
            .session
            .read_bytes(hash, MAX_BLOB_BYTES)
            .await
            .map_err(js_err)?;
        Ok(bytes.to_vec())
    }

    /// Register a callback receiving events as `{kind, ...}` objects:
    /// `peerJoined`, `offerReceived`, `fetchProgress`, `fetchCompleted`, …
    /// May be called multiple times; each registration gets its own stream.
    pub fn on_event(&self, callback: js_sys::Function) {
        let mut rx = self.session.subscribe();
        wasm_bindgen_futures::spawn_local(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let value = serde_wasm_bindgen::to_value(&JsEvent::from(event))
                            .unwrap_or(JsValue::NULL);
                        let _ = callback.call1(&JsValue::NULL, &value);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    /// Leave the drop (announces withdrawal of anything we serve).
    pub async fn close(self) -> Result<(), JsError> {
        self.session.shutdown().await.map_err(js_err)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsOffer {
    hash: String,
    name: String,
    size: u64,
    media_type: Option<String>,
    from: String,
    have: bool,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum JsEvent {
    PeerJoined {
        peer: String,
    },
    PeerLeft {
        peer: String,
    },
    OfferReceived {
        from: String,
        hash: String,
        name: String,
        size: u64,
        media_type: Option<String>,
    },
    OfferRejected {
        from: String,
        reason: String,
    },
    FetchStarted {
        hash: String,
        provider: String,
    },
    FetchProgress {
        hash: String,
        downloaded: u64,
        total: Option<u64>,
    },
    FetchCompleted {
        hash: String,
        provider: String,
    },
    FetchFailed {
        hash: String,
        error: String,
    },
    ProviderAvailable {
        hash: String,
        peer: String,
    },
    ProviderUnavailable {
        hash: String,
        peer: String,
    },
    ProtocolWarning {
        from: Option<String>,
        warning: String,
    },
    /// `DropEvent` is non-exhaustive; newer kinds surface as debug text.
    Unknown {
        debug: String,
    },
}

impl From<DropEvent> for JsEvent {
    fn from(event: DropEvent) -> Self {
        match event {
            DropEvent::PeerJoined { peer } => JsEvent::PeerJoined {
                peer: peer.to_string(),
            },
            DropEvent::PeerLeft { peer } => JsEvent::PeerLeft {
                peer: peer.to_string(),
            },
            DropEvent::OfferReceived { from, offer } => JsEvent::OfferReceived {
                from: from.to_string(),
                hash: offer.blob_hash.to_string(),
                name: offer.name,
                size: offer.size,
                media_type: offer.media_type,
            },
            DropEvent::OfferRejected { from, reason } => JsEvent::OfferRejected {
                from: from.to_string(),
                reason: format!("{reason:?}"),
            },
            DropEvent::FetchStarted { hash, provider } => JsEvent::FetchStarted {
                hash: hash.to_string(),
                provider: provider.to_string(),
            },
            DropEvent::FetchProgress {
                hash,
                downloaded,
                total,
            } => JsEvent::FetchProgress {
                hash: hash.to_string(),
                downloaded,
                total,
            },
            DropEvent::FetchCompleted { hash, provider } => JsEvent::FetchCompleted {
                hash: hash.to_string(),
                provider: provider.to_string(),
            },
            DropEvent::FetchFailed { hash, error } => JsEvent::FetchFailed {
                hash: hash.to_string(),
                error: error.to_string(),
            },
            DropEvent::ProviderAvailable { hash, peer } => JsEvent::ProviderAvailable {
                hash: hash.to_string(),
                peer: peer.to_string(),
            },
            DropEvent::ProviderUnavailable { hash, peer } => JsEvent::ProviderUnavailable {
                hash: hash.to_string(),
                peer: peer.to_string(),
            },
            DropEvent::ProtocolWarning { from, warning } => JsEvent::ProtocolWarning {
                from: from.map(|p| p.to_string()),
                warning: format!("{warning:?}"),
            },
            other => JsEvent::Unknown {
                debug: format!("{other:?}"),
            },
        }
    }
}
