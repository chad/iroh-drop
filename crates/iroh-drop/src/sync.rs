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
use iroh::{EndpointAddr, EndpointId};
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::limits::{self, PeerRateLimiter};
use crate::message::{KIND_OFFER, KIND_PROVIDER, KIND_REQUEST, WIRE_VERSION};
use crate::seal::{KIND_SEALED, SEALED_WIRE_VERSION};
use crate::session::{decode_for_session, handle_message, SessionInner, SessionRegistry};

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

/// Hard cap on pages requested in one catch-up attempt, so a peer serving
/// trickle-sized pages cannot keep a requester talking forever.
const MAX_PAGES_PER_ATTEMPT: usize = 128;

/// Hard cap on frame bytes imported in one attempt. The frame count cap
/// alone allows 8192 x 64KiB worst-case frames — half a GiB.
const MAX_TOTAL_BYTES_PER_ATTEMPT: usize = 64 * 1024 * 1024;
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
pub struct ControlRequestV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// Which operation. See [`OP_HELLO`], [`OP_SYNC_PAGE`].
    op: u16,
    /// postcard-encoded request for that operation.
    payload: Vec<u8>,
}

/// The matching response envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlResponseV1 {
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
pub struct HelloV1 {
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
pub struct SyncRequestV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// The drop being synced.
    topic_id: [u8; 32],
    /// Absolute frame cursor; `0` asks from the beginning (clamped to the
    /// retained window).
    cursor: u64,
    /// Page size hint, clamped to [`MAX_FRAMES_PER_PAGE`].
    max_frames: u16,
    /// Proof of drop-key possession for sealed drops:
    /// `HMAC-SHA256(sync_key, requester || responder || encoded request)`,
    /// where `sync_key` is HKDF-separated from the frame key and
    /// `encoded request` is this struct encoded with `key_proof: None`.
    /// Bound to the connection and the exact request — not a portable
    /// token. Public drops omit it (`None`).
    key_proof: Option<[u8; 32]>,
}

/// The sync request schema as written before `key_proof` existed.
/// Decode-only, for requests from older builds and key-less actors.
#[derive(Serialize, Deserialize)]
struct SyncRequestV1Legacy {
    version: u16,
    topic_id: [u8; 32],
    cursor: u64,
    max_frames: u16,
}

impl SyncRequestV1Legacy {
    fn upgrade(self) -> SyncRequestV1 {
        SyncRequestV1 {
            version: self.version,
            topic_id: self.topic_id,
            cursor: self.cursor,
            max_frames: self.max_frames,
            key_proof: None,
        }
    }
}

/// Decode a sync request, tolerating the pre-`key_proof` schema.
fn decode_sync_request(bytes: &[u8]) -> Result<SyncRequestV1, postcard::Error> {
    if let Ok((request, _)) = postcard::take_from_bytes::<SyncRequestV1>(bytes) {
        return Ok(request);
    }
    postcard::take_from_bytes::<SyncRequestV1Legacy>(bytes).map(|(legacy, _)| legacy.upgrade())
}

/// One page of retained signed frames.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncResponseV1 {
    /// Must equal [`WIRE_VERSION`].
    version: u16,
    /// Cursor for the next page; `None` when caught up.
    next_cursor: Option<u64>,
    /// Signed gossip frames (verifiable against their embedded authors).
    frames: Vec<Vec<u8>>,
    /// Absolute cursor of the oldest frame the responder still retains.
    oldest_cursor: u64,
    /// True when the request cursor pointed at frames the responder has
    /// already evicted (`0 < request.cursor < oldest_cursor`): the requester
    /// has permanently missed history this peer once held, and should ask
    /// other peers if it cares. `cursor = 0` ("from the beginning of what
    /// you have") is never truncation.
    truncated: bool,
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
            self.serve_op(send, recv, &peer).await?;
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
        peer: &EndpointId,
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
                    // Family 3 (sealed) is a build capability; which family a
                    // given session speaks is the ticket's business.
                    wire_versions: vec![WIRE_VERSION, SEALED_WIRE_VERSION],
                    ops: vec![OP_HELLO, OP_SYNC_PAGE],
                    message_kinds: vec![KIND_OFFER, KIND_PROVIDER, KIND_REQUEST, KIND_SEALED],
                    max_frames_per_page: MAX_FRAMES_PER_PAGE as u16,
                };
                self.reply(&mut send, OP_HELLO, &hello).await
            }
            OP_SYNC_PAGE => {
                let Ok(request) = decode_sync_request(&envelope.payload) else {
                    debug!("control: undecodable sync request");
                    return Ok(());
                };
                let response = self.sync_page(request, peer);
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
    fn sync_page(&self, request: SyncRequestV1, peer: &EndpointId) -> SyncResponseV1 {
        let topic_id = TopicId::from_bytes(request.topic_id);
        let session = self
            .sessions
            .read()
            .get(&topic_id)
            .and_then(|weak| weak.upgrade());

        // Sealed drops serve history only to proven key holders — and a
        // blind relay serves no one, since it cannot verify a proof.
        // Refusal is an empty page: a proof-less requester cannot
        // distinguish it from "caught up", and gets nothing intelligible
        // either way.
        if let Some(inner) = &session {
            if inner.mode == crate::ticket::DropMode::Sealed {
                // Reconstruct the exact bytes the requester proved: the
                // request with the proof field cleared.
                let proven = SyncRequestV1 {
                    key_proof: None,
                    ..request.clone()
                };
                let proven_bytes =
                    postcard::to_allocvec(&proven).expect("request re-encoding is infallible");
                let self_id = inner.stack.endpoint.id();
                let ok = match (&inner.drop_key, &request.key_proof) {
                    (Some(drop_key), Some(proof)) => {
                        drop_key.verify_sync_proof(&topic_id, peer, &self_id, &proven_bytes, proof)
                    }
                    _ => false,
                };
                if !ok {
                    debug!("sync: refusing proof-less request for a sealed drop");
                    return SyncResponseV1 {
                        version: WIRE_VERSION,
                        next_cursor: None,
                        frames: Vec::new(),
                        oldest_cursor: 0,
                        truncated: false,
                    };
                }
            }
        }

        let max = (request.max_frames as usize).clamp(1, MAX_FRAMES_PER_PAGE);
        let (frames, next_cursor, oldest_cursor) = match &session {
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
                (frames, next_cursor, page.oldest_cursor)
            }
            None => (Vec::new(), None, 0),
        };
        let response = SyncResponseV1 {
            version: WIRE_VERSION,
            next_cursor,
            frames,
            oldest_cursor,
            truncated: request.cursor != 0 && request.cursor < oldest_cursor,
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
    n0_future::time::timeout(IO_TIMEOUT, recv.read_to_end(cap))
        .await
        .map_err(AcceptError::from_err)?
        .map_err(AcceptError::from_err)
}

async fn write_all(send: &mut SendStream, bytes: &[u8]) -> Result<(), AcceptError> {
    n0_future::time::timeout(IO_TIMEOUT, send.write_all(bytes))
        .await
        .map_err(AcceptError::from_err)?
        .map_err(AcceptError::from_err)
}

/// Pull the retained offer/provider log from each reachable peer and replay
/// it locally. Best-effort: failures are logged and skipped, never fatal.
pub(crate) async fn sync_catchup(inner: Arc<SessionInner>, peers: Vec<EndpointAddr>) {
    // A blind relay cannot prove key possession and could not read what it
    // received anyway; it lives on live gossip alone.
    if inner.mode == crate::ticket::DropMode::Sealed && inner.drop_key.is_none() {
        debug!("sync: blind relay does not request history");
        return;
    }
    for addr in peers {
        let peer_id = addr.id;
        let conn = match n0_future::time::timeout(
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

        let self_id = inner.stack.endpoint.id();
        let mut cursor = 0u64;
        let mut total = 0usize;
        let mut total_bytes = 0usize;
        let mut pages = 0usize;
        loop {
            let mut request = SyncRequestV1 {
                version: WIRE_VERSION,
                topic_id: *inner.topic_id.as_bytes(),
                cursor,
                max_frames: page_size,
                key_proof: None,
            };
            if let Some(key) = inner.drop_key.as_ref() {
                let proven =
                    postcard::to_allocvec(&request).expect("request encoding is infallible");
                request.key_proof =
                    Some(key.sync_proof(&inner.topic_id, &self_id, &peer_id, &proven));
            }
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
            pages += 1;
            if pages > MAX_PAGES_PER_ATTEMPT {
                debug!(peer = %peer_id.fmt_short(), "sync: page budget reached, stopping attempt");
                break;
            }
            if response.truncated {
                debug!(
                    peer = %peer_id.fmt_short(),
                    oldest = response.oldest_cursor,
                    cursor,
                    "sync: peer evicted history we missed; other peers may still have it"
                );
            }
            let mut applied = 0usize;
            for frame in &response.frames {
                total += 1;
                total_bytes += frame.len();
                if total > MAX_TOTAL_FRAMES {
                    warn!(peer = %peer_id.fmt_short(), "sync: frame budget exceeded, stopping");
                    return;
                }
                if total_bytes > MAX_TOTAL_BYTES_PER_ATTEMPT {
                    warn!(peer = %peer_id.fmt_short(), "sync: byte budget exceeded, stopping");
                    return;
                }
                // Replay through the standard verify + dispatch path.
                // Imported frames are decoded in the session's own wire
                // family: a private drop's retained log is ciphertext, and
                // only this session's key can read it.
                let frame_bytes = Bytes::copy_from_slice(frame);
                if let Ok(verified) = decode_for_session(&inner, &frame_bytes) {
                    applied += 1;
                    handle_message(
                        &inner,
                        verified.author,
                        verified.message,
                        verified.body,
                        peer_id,
                        frame_bytes,
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
    let bytes = n0_future::time::timeout(IO_TIMEOUT, exchange)
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

#[cfg(test)]
mod conformance {
    //! Golden control-channel vectors. These envelope types are deliberately
    //! crate-private, so their fixtures are checked here rather than in
    //! `tests/conformance.rs`. One command runs both suites:
    //! `cargo test -p iroh-drop conformance`.
    //!
    //! The gossip frames embedded in the sync response are rebuilt here with
    //! the same deterministic recipe as `tests/common/fixtures.rs` — keep
    //! the two in sync. The decode test below asserts the embedded frames
    //! are byte-identical to the committed gossip fixtures, so divergence
    //! fails the suite.

    use super::*;
    use crate::message::{
        BodyEnvelopeV1, MessageBodyV1, MessageV1, OfferV1, ProviderState, ProviderV1, RequestV1,
    };
    use iroh::SecretKey;
    use std::path::PathBuf;

    // --- deterministic recipe: keep in sync with tests/common/fixtures.rs ---

    // The gossip frames embedded in the sync response are signed with the
    // fixtures.rs topic ([0xC1; 32]) so the recipes stay byte-identical.
    const TOPIC_ID: [u8; 32] = [0x11; 32];

    fn author_key() -> SecretKey {
        SecretKey::from_bytes(&[0xA5; 32])
    }

    fn blob_hash() -> crate::hash::BlobHash {
        crate::hash::BlobHash::from_bytes([0x42; 32])
    }

    fn sign(id: u8, body: MessageBodyV1) -> Vec<u8> {
        MessageV1 {
            version: WIRE_VERSION,
            id: [id; 16],
            sent_at_ms: 1_752_700_000_000,
            body: BodyEnvelopeV1::encode(&body).unwrap(),
        }
        .encode(&author_key(), &TopicId::from_bytes([0xC1; 32]))
        .unwrap()
    }

    fn offer_frame() -> Vec<u8> {
        sign(
            0x0F,
            MessageBodyV1::Offer(OfferV1 {
                blob_hash: blob_hash(),
                name: "quarterly-report.pdf".into(),
                size: 4_194_304,
                media_type: Some("application/pdf".into()),
                created_at_ms: Some(1_752_600_000_000),
                metadata: std::collections::BTreeMap::from([
                    ("files".to_string(), "12".to_string()),
                    ("project".to_string(), "apollo".to_string()),
                ]),
            }),
        )
    }

    fn provider_frame() -> Vec<u8> {
        sign(
            0xA0,
            MessageBodyV1::Provider(ProviderV1 {
                blob_hash: blob_hash(),
                state: ProviderState::Available,
                announced_at_ms: Some(1_752_700_100_000),
            }),
        )
    }

    fn request_frame() -> Vec<u8> {
        sign(
            0xB0,
            MessageBodyV1::Request(RequestV1 {
                blob_hash: blob_hash(),
            }),
        )
    }

    // --- control envelopes under test ---

    fn hello_request() -> Vec<u8> {
        postcard::to_allocvec(&ControlRequestV1 {
            version: WIRE_VERSION,
            op: OP_HELLO,
            payload: postcard::to_allocvec(&()).unwrap(),
        })
        .unwrap()
    }

    fn hello_response() -> Vec<u8> {
        let hello = HelloV1 {
            wire_versions: vec![WIRE_VERSION, SEALED_WIRE_VERSION],
            ops: vec![OP_HELLO, OP_SYNC_PAGE],
            message_kinds: vec![KIND_OFFER, KIND_PROVIDER, KIND_REQUEST, KIND_SEALED],
            max_frames_per_page: MAX_FRAMES_PER_PAGE as u16,
        };
        postcard::to_allocvec(&ControlResponseV1 {
            version: WIRE_VERSION,
            op: OP_HELLO,
            payload: postcard::to_allocvec(&hello).unwrap(),
        })
        .unwrap()
    }

    fn sync_request() -> Vec<u8> {
        postcard::to_allocvec(&ControlRequestV1 {
            version: WIRE_VERSION,
            op: OP_SYNC_PAGE,
            payload: postcard::to_allocvec(&SyncRequestV1 {
                version: WIRE_VERSION,
                topic_id: TOPIC_ID,
                cursor: 0,
                max_frames: 16,
                key_proof: None,
            })
            .unwrap(),
        })
        .unwrap()
    }

    fn sync_response() -> Vec<u8> {
        postcard::to_allocvec(&ControlResponseV1 {
            version: WIRE_VERSION,
            op: OP_SYNC_PAGE,
            payload: postcard::to_allocvec(&SyncResponseV1 {
                version: WIRE_VERSION,
                next_cursor: None,
                frames: vec![offer_frame(), provider_frame(), request_frame()],
                oldest_cursor: 1,
                truncated: false,
            })
            .unwrap(),
        })
        .unwrap()
    }

    // --- fixture I/O ---

    fn fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
    }

    fn bless() -> bool {
        std::env::var_os("IROH_DROP_BLESS").is_some()
    }

    fn check_or_bless(name: &str, bytes: &[u8]) {
        if bless() {
            std::fs::create_dir_all(fixture_dir()).unwrap();
            std::fs::write(fixture_dir().join(name), bytes).unwrap();
            return;
        }
        let path = fixture_dir().join(name);
        let committed = std::fs::read(&path).unwrap_or_else(|e| {
            panic!("missing fixture {} ({e}) — run: IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance", path.display())
        });
        assert_eq!(
            &committed, bytes,
            "fixture {name} drifted from the wire format this build produces"
        );
    }

    #[test]
    fn conformance_control_frames_match_committed_bytes() {
        check_or_bless("control_hello_request.bin", &hello_request());
        check_or_bless("control_hello_response.bin", &hello_response());
        check_or_bless("control_sync_request.bin", &sync_request());
        check_or_bless("control_sync_response.bin", &sync_response());
    }

    #[test]
    fn legacy_sync_request_without_key_proof_still_decodes() {
        // Requests written before `key_proof` existed end after
        // `max_frames`; the tolerant decoder must map them to `None`.
        let legacy = SyncRequestV1Legacy {
            version: WIRE_VERSION,
            topic_id: TOPIC_ID,
            cursor: 7,
            max_frames: 16,
        };
        let bytes = postcard::to_allocvec(&legacy).unwrap();
        let request = decode_sync_request(&bytes).unwrap();
        assert_eq!(request.cursor, 7);
        assert!(request.key_proof.is_none());
        // And the current schema round-trips its proof.
        let keyed = SyncRequestV1 {
            version: WIRE_VERSION,
            topic_id: TOPIC_ID,
            cursor: 0,
            max_frames: 16,
            key_proof: Some([7u8; 32]),
        };
        let bytes = postcard::to_allocvec(&keyed).unwrap();
        let request = decode_sync_request(&bytes).unwrap();
        assert_eq!(request.key_proof, Some([7u8; 32]));
    }

    #[test]
    fn conformance_control_frames_decode_to_exact_values() {
        if bless() {
            return; // nothing committed yet on a first bless run
        }
        let read = |name: &str| std::fs::read(fixture_dir().join(name)).unwrap();

        // Hello request: version + op, empty payload.
        let (req, _): (ControlRequestV1, _) =
            postcard::take_from_bytes(&read("control_hello_request.bin")).unwrap();
        assert_eq!(req.version, WIRE_VERSION);
        assert_eq!(req.op, OP_HELLO);
        assert!(req.payload.is_empty());

        // Hello response: the full capability set, exactly.
        let (resp, _): (ControlResponseV1, _) =
            postcard::take_from_bytes(&read("control_hello_response.bin")).unwrap();
        assert_eq!(resp.version, WIRE_VERSION);
        assert_eq!(resp.op, OP_HELLO);
        let (hello, _): (HelloV1, _) = postcard::take_from_bytes(&resp.payload).unwrap();
        assert_eq!(hello.wire_versions, vec![WIRE_VERSION, SEALED_WIRE_VERSION]);
        assert_eq!(hello.ops, vec![OP_HELLO, OP_SYNC_PAGE]);
        assert_eq!(
            hello.message_kinds,
            vec![KIND_OFFER, KIND_PROVIDER, KIND_REQUEST, KIND_SEALED]
        );
        assert_eq!(hello.max_frames_per_page, MAX_FRAMES_PER_PAGE as u16);

        // Sync request: topic, cursor, page hint, no key proof.
        let (req, _): (ControlRequestV1, _) =
            postcard::take_from_bytes(&read("control_sync_request.bin")).unwrap();
        assert_eq!(req.op, OP_SYNC_PAGE);
        let page = decode_sync_request(&req.payload).unwrap();
        assert_eq!(page.version, WIRE_VERSION);
        assert_eq!(page.topic_id, TOPIC_ID);
        assert_eq!(page.cursor, 0);
        assert_eq!(page.max_frames, 16);
        assert!(page.key_proof.is_none());

        // Sync response: caught up, three frames — and each embedded frame
        // is byte-identical to the committed gossip fixture of the same
        // name, which is what pins the two recipes together.
        let (resp, _): (ControlResponseV1, _) =
            postcard::take_from_bytes(&read("control_sync_response.bin")).unwrap();
        assert_eq!(resp.op, OP_SYNC_PAGE);
        let (page, _): (SyncResponseV1, _) = postcard::take_from_bytes(&resp.payload).unwrap();
        assert_eq!(page.version, WIRE_VERSION);
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.frames.len(), 3);
        assert_eq!(page.frames[0], read("offer_full.bin"));
        assert_eq!(page.frames[1], read("provider_available.bin"));
        assert_eq!(page.frames[2], read("request.bin"));
        for frame in &page.frames {
            MessageV1::decode(frame, &TopicId::from_bytes([0xC1; 32]))
                .expect("served frames must verify");
        }
    }
}
