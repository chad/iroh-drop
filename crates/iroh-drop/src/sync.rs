//! Catch-up sync: how late joiners learn what was announced before they
//! arrived.
//!
//! Gossip has no history, so a peer that joins after offers were broadcast
//! would never see them. Sync fixes that over the reserved control ALPN
//! ([`SYNC_ALPN`]): the joiner asks any member for its retained *signed
//! frames* (offers and provider announcements, see
//! [`crate::state::SYNC_LOG_CAP`]) and replays them through the exact same
//! verify-and-dispatch path as live gossip. Frames are signed by their
//! original authors, so relaying them is safe regardless of who serves them.
//!
//! The protocol is deliberately tiny: one bi-stream per page, an
//! EOF-delimited postcard request, an EOF-delimited postcard response.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::EndpointAddr;
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::limits::{self, PeerRateLimiter};
use crate::message::{MessageV1, KIND_OFFER, KIND_PROVIDER, KIND_REQUEST, WIRE_VERSION};
use crate::session::{handle_message, SessionInner, SessionRegistry};

/// ALPN of the drop control channel. Sync is its first (and currently only)
/// user; the ALPN is shared so future control operations ride the same
/// connections.
pub(crate) const SYNC_ALPN: &[u8] = crate::DROP_ALPN;

/// Maximum size of a sync request.
const MAX_REQUEST_BYTES: usize = 4 * 1024;
/// Maximum size of a sync response page.
const MAX_RESPONSE_BYTES: usize = 512 * 1024;
/// Hard cap on frames per response page (a byte cap applies on top).
const MAX_FRAMES_PER_PAGE: usize = 256;
/// Safety cap on total frames a client will pull from one peer.
const MAX_TOTAL_FRAMES: usize = 8192;
/// Timeout for one connect / page round-trip.
const IO_TIMEOUT: Duration = Duration::from_secs(15);

/// How many peers may pull history from us at the same time.
const MAX_CONCURRENT_SERVING: usize = 8;

/// Control operation: ask what the peer supports.
pub(crate) const OP_HELLO: u16 = 1;

/// Control operation: ask for a page of retained frames.
pub(crate) const OP_SYNC_PAGE: u16 = 2;

/// The control-channel envelope.
///
/// Every request on `/iroh-drop/1` is a kind tag plus an opaque payload, for
/// the same reason message bodies are: a peer can reject or ignore an
/// operation it does not implement without the connection or the protocol
/// version having to change.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlRequestV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// Which operation. See [`OP_HELLO`], [`OP_SYNC_PAGE`].
    op: u16,
    /// postcard-encoded request for that operation.
    payload: Vec<u8>,
}

/// The matching response envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlResponseV1 {
    /// Our wire version, so a mismatched peer learns what we speak.
    version: u16,
    /// Echo of the request's operation, or [`OP_UNSUPPORTED`].
    op: u16,
    /// postcard-encoded response for that operation.
    payload: Vec<u8>,
}

/// Response `op` meaning "I do not implement that operation".
pub(crate) const OP_UNSUPPORTED: u16 = u16::MAX;

/// What a peer can do, so clients need not guess from a version number.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HelloV1 {
    /// Wire major versions this peer accepts.
    wire_versions: Vec<u16>,
    /// Control operations it implements.
    ops: Vec<u16>,
    /// Message kinds it understands (others it may still relay).
    message_kinds: Vec<u16>,
    /// Largest page it will serve, in frames.
    max_frames_per_page: u16,
}

/// A page of retained frames, please.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncRequestV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// The drop being synced.
    topic_id: [u8; 32],
    /// Absolute frame cursor; `0` asks from the beginning (clamped to the
    /// retained window).
    cursor: u64,
    /// Page size hint, clamped to [`MAX_FRAMES_PER_PAGE`].
    max_frames: u16,
}

/// One page of retained signed frames.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncResponseV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// Cursor for the next page; `None` when caught up.
    next_cursor: Option<u64>,
    /// Signed gossip frames (verifiable against their embedded authors).
    frames: Vec<Vec<u8>>,
}

/// The accept side of the sync protocol: serves retained frames for topics
/// this process has live sessions for.
#[derive(Debug)]
pub(crate) struct SyncProtocol {
    sessions: SessionRegistry,
    /// Caps concurrent history serving; sync amplifies small requests.
    serving: Arc<tokio::sync::Semaphore>,
    /// Per-peer page budget.
    limiter: parking_lot::Mutex<PeerRateLimiter>,
}

impl SyncProtocol {
    pub(crate) fn new(sessions: SessionRegistry) -> Self {
        Self {
            sessions,
            serving: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_SERVING)),
            limiter: parking_lot::Mutex::new(PeerRateLimiter::new(limits::SYNC_PAGES)),
        }
    }
}

impl ProtocolHandler for SyncProtocol {
    /// Serve operations until the peer closes the connection. One bi-stream
    /// per operation; the handler must outlive the last one, or the
    /// connection would close before the client finishes reading.
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        // Bound how much of this process one peer's history-pulling can
        // occupy: sync is small-request/large-response, so it amplifies.
        let _permit = match self.serving.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                debug!("sync: at capacity, refusing a connection");
                return Ok(());
            }
        };
        let peer = connection.remote_id();
        while let Ok((send, recv)) = connection.accept_bi().await {
            if !self.limiter.lock().allow(peer) {
                debug!(peer = %peer.fmt_short(), "sync: rate limited");
                return Ok(());
            }
            self.serve_op(send, recv).await?;
        }
        Ok(())
    }
}

impl SyncProtocol {
    /// Read one control request and answer it.
    async fn serve_op(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<(), AcceptError> {
        let request = read_capped(&mut recv, MAX_REQUEST_BYTES).await?;
        let Ok((envelope, _rest)) = postcard::take_from_bytes::<ControlRequestV1>(&request) else {
            debug!("control: undecodable request, closing");
            return Ok(());
        };
        if envelope.version != WIRE_VERSION {
            debug!(version = envelope.version, "control: version mismatch");
            // Answer anyway, so the peer learns which version we speak.
            self.reply(&mut send, OP_UNSUPPORTED, &()).await?;
            return Ok(());
        }
        match envelope.op {
            OP_HELLO => {
                let hello = HelloV1 {
                    wire_versions: vec![WIRE_VERSION],
                    ops: vec![OP_HELLO, OP_SYNC_PAGE],
                    message_kinds: vec![KIND_OFFER, KIND_PROVIDER, KIND_REQUEST],
                    max_frames_per_page: MAX_FRAMES_PER_PAGE as u16,
                };
                self.reply(&mut send, OP_HELLO, &hello).await
            }
            OP_SYNC_PAGE => {
                let Ok((request, _rest)) =
                    postcard::take_from_bytes::<SyncRequestV1>(&envelope.payload)
                else {
                    debug!("control: undecodable sync request");
                    return Ok(());
                };
                let response = self.sync_page(request);
                self.reply(&mut send, OP_SYNC_PAGE, &response).await
            }
            other => {
                debug!(op = other, "control: unsupported operation");
                self.reply(&mut send, OP_UNSUPPORTED, &()).await
            }
        }
    }

    /// Encode and send one response.
    async fn reply<T: Serialize>(
        &self,
        send: &mut SendStream,
        op: u16,
        payload: &T,
    ) -> Result<(), AcceptError> {
        let payload = postcard::to_allocvec(payload).map_err(AcceptError::from_err)?;
        let envelope = ControlResponseV1 {
            version: WIRE_VERSION,
            op,
            payload,
        };
        let bytes = postcard::to_allocvec(&envelope).map_err(AcceptError::from_err)?;
        write_all(send, &bytes).await?;
        send.finish().map_err(AcceptError::from_err)?;
        Ok(())
    }

    /// Build one page of retained frames.
    fn sync_page(&self, request: SyncRequestV1) -> SyncResponseV1 {
        let topic_id = TopicId::from_bytes(request.topic_id);
        let session = self
            .sessions
            .read()
            .get(&topic_id)
            .and_then(|weak| weak.upgrade());

        let max = (request.max_frames as usize).clamp(1, MAX_FRAMES_PER_PAGE);
        let (frames, next_cursor) = match &session {
            Some(inner) => {
                let page = {
                    let state = inner.state.read();
                    state.sync_frames(request.cursor, max)
                };
                // Bound the page by bytes as well as count. Frames are
                // consecutive, so dropping a tail adjusts the cursor by
                // exactly the number of dropped frames.
                let mut budget = MAX_RESPONSE_BYTES;
                let mut kept = page.frames.len();
                for (i, frame) in page.frames.iter().enumerate() {
                    if frame.len() > budget {
                        kept = i;
                        break;
                    }
                    budget -= frame.len();
                }
                let dropped = page.frames.len() - kept;
                let frames: Vec<Vec<u8>> = page.frames[..kept].iter().map(|f| f.to_vec()).collect();
                let next_cursor = if page.caught_up && dropped == 0 {
                    None
                } else {
                    Some(page.end_cursor - dropped as u64)
                };
                (frames, next_cursor)
            }
            None => (Vec::new(), None),
        };
        let response = SyncResponseV1 {
            version: WIRE_VERSION,
            next_cursor,
            frames,
        };
        trace!(
            frames = response.frames.len(),
            next = ?response.next_cursor,
            "sync: serving page"
        );
        response
    }
}

async fn read_capped(recv: &mut RecvStream, cap: usize) -> Result<Vec<u8>, AcceptError> {
    tokio::time::timeout(IO_TIMEOUT, recv.read_to_end(cap))
        .await
        .map_err(AcceptError::from_err)?
        .map_err(AcceptError::from_err)
}

async fn write_all(send: &mut SendStream, bytes: &[u8]) -> Result<(), AcceptError> {
    tokio::time::timeout(IO_TIMEOUT, send.write_all(bytes))
        .await
        .map_err(AcceptError::from_err)?
        .map_err(AcceptError::from_err)
}

/// Pull the retained offer/provider log from each reachable peer and replay
/// it locally. Best-effort: failures are logged and skipped, never fatal.
pub(crate) async fn sync_catchup(inner: Arc<SessionInner>, peers: Vec<EndpointAddr>) {
    for addr in peers {
        let peer_id = addr.id;
        let conn = match tokio::time::timeout(
            IO_TIMEOUT,
            inner.stack.endpoint.connect(addr.clone(), SYNC_ALPN),
        )
        .await
        {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                trace!(peer = %peer_id.fmt_short(), "sync: connect failed: {e}");
                continue;
            }
            Err(_) => {
                trace!(peer = %peer_id.fmt_short(), "sync: connect timed out");
                continue;
            }
        };
        debug!(peer = %peer_id.fmt_short(), "sync: catching up");

        // Ask what the peer supports before assuming. A peer that cannot
        // answer Hello may still serve pages (it may be older than this
        // operation), so a failure here is not fatal.
        let mut page_size = MAX_FRAMES_PER_PAGE as u16;
        match request_op::<HelloV1>(&conn, OP_HELLO, &()).await {
            Ok(Some(hello)) => {
                if !hello.wire_versions.contains(&WIRE_VERSION) {
                    debug!(
                        peer = %peer_id.fmt_short(),
                        versions = ?hello.wire_versions,
                        "sync: peer speaks a different wire version, skipping"
                    );
                    conn.close(0u32.into(), b"version mismatch");
                    continue;
                }
                if !hello.ops.contains(&OP_SYNC_PAGE) {
                    debug!(peer = %peer_id.fmt_short(), "sync: peer does not serve history");
                    conn.close(0u32.into(), b"no sync");
                    continue;
                }
                page_size = page_size.min(hello.max_frames_per_page.max(1));
                trace!(
                    peer = %peer_id.fmt_short(),
                    kinds = ?hello.message_kinds,
                    page_size,
                    "sync: negotiated"
                );
            }
            Ok(None) | Err(_) => {
                trace!(peer = %peer_id.fmt_short(), "sync: no hello, trying pages anyway");
            }
        }

        let mut cursor = 0u64;
        let mut total = 0usize;
        loop {
            let request = SyncRequestV1 {
                version: WIRE_VERSION,
                topic_id: *inner.topic_id.as_bytes(),
                cursor,
                max_frames: page_size,
            };
            let response = match request_op::<SyncResponseV1>(&conn, OP_SYNC_PAGE, &request).await {
                Ok(Some(response)) => response,
                Ok(None) => {
                    trace!(peer = %peer_id.fmt_short(), "sync: peer refused the page");
                    break;
                }
                Err(e) => {
                    trace!(peer = %peer_id.fmt_short(), "sync: page failed: {e}");
                    break;
                }
            };
            if response.version != WIRE_VERSION {
                break;
            }
            let mut applied = 0usize;
            for frame in &response.frames {
                total += 1;
                if total > MAX_TOTAL_FRAMES {
                    warn!(peer = %peer_id.fmt_short(), "sync: frame budget exceeded, stopping");
                    return;
                }
                // Replay through the standard verify + dispatch path.
                if let Ok(verified) = MessageV1::decode(frame) {
                    applied += 1;
                    handle_message(
                        &inner,
                        verified.author,
                        verified.message,
                        verified.body,
                        peer_id,
                        Bytes::copy_from_slice(frame),
                    )
                    .await;
                }
            }
            trace!(
                peer = %peer_id.fmt_short(),
                page_frames = response.frames.len(),
                applied,
                cursor,
                "sync: page applied"
            );
            match response.next_cursor {
                Some(next) if next > cursor => cursor = next,
                _ => break,
            }
        }
        conn.close(0u32.into(), b"sync done");
        if total > 0 {
            debug!(peer = %peer_id.fmt_short(), total, "sync: caught up");
        }
        // One responsive peer is enough for the shared log; extra peers are
        // only useful if the first had nothing.
        if total > 0 {
            return;
        }
    }
}

/// Send one control request and decode its response.
///
/// `Ok(None)` means the peer answered that it does not implement the
/// operation — a normal outcome worth handling, not an error.
async fn request_op<R: serde::de::DeserializeOwned>(
    conn: &Connection,
    op: u16,
    payload: &impl Serialize,
) -> Result<Option<R>, String> {
    let payload = postcard::to_allocvec(payload).map_err(|e| e.to_string())?;
    let request = postcard::to_allocvec(&ControlRequestV1 {
        version: WIRE_VERSION,
        op,
        payload,
    })
    .map_err(|e| e.to_string())?;

    let exchange = async {
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
        send.write_all(&request).await.map_err(|e| e.to_string())?;
        send.finish().map_err(|e| e.to_string())?;
        recv.read_to_end(MAX_RESPONSE_BYTES)
            .await
            .map_err(|e| e.to_string())
    };
    let bytes = tokio::time::timeout(IO_TIMEOUT, exchange)
        .await
        .map_err(|_| "timed out".to_string())??;

    let (envelope, _rest) = postcard::take_from_bytes::<ControlResponseV1>(&bytes)
        .map_err(|e| format!("response: {e}"))?;
    if envelope.version != WIRE_VERSION {
        return Err(format!("peer speaks wire version {}", envelope.version));
    }
    if envelope.op == OP_UNSUPPORTED {
        return Ok(None);
    }
    if envelope.op != op {
        return Err(format!("peer answered op {} for op {op}", envelope.op));
    }
    postcard::take_from_bytes::<R>(&envelope.payload)
        .map(|(value, _rest)| Some(value))
        .map_err(|e| format!("payload: {e}"))
}

/// Serve the sync protocol over the shared endpoint: convenience re-export
/// for the stack builder.
pub(crate) fn protocol(sessions: SessionRegistry) -> SyncProtocol {
    SyncProtocol::new(sessions)
}
