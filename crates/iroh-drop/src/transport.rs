//! The gossip carrier a session speaks its wire format over.
//!
//! Sessions never touch iroh-gossip directly; they go through
//! [`DropTransport`]. The production implementation is [`GossipTransport`]
//! (iroh-gossip over QUIC). Tests substitute an in-memory transport, which
//! is what makes the protocol-logic suite fast and deterministic — and the
//! substitution itself is the point: the protocol's semantics (signatures,
//! dedup, retention, limits) do not depend on the carrier's.
//!
//! ## The one contract that must survive any implementation
//!
//! `delivered_from` is the **relaying neighbor**, not the author. Frames
//! authenticate authorship only by their Ed25519 signature, which a
//! transport does not verify (and a relay cannot forge). A transport may
//! reorder, delay, duplicate, or drop; it may report neighbors that lie;
//! none of that can put words in an author's mouth.

use std::fmt;
use std::sync::Mutex;

use bytes::Bytes;
use futures::Stream;
use iroh::EndpointId;
use iroh_gossip::api::{Event as GossipEvent, GossipReceiver, GossipSender};
use tracing::warn;

use crate::error::{DropError, NetworkError};

/// A carrier event, distilled to exactly what the session logic consumes.
#[derive(Clone, Debug)]
pub enum TransportEvent {
    /// A frame arrived. `delivered_from` is the neighbor that handed it to
    /// us — topology information, *not* authorship. Authorship comes only
    /// from the frame's verified signature.
    Received {
        /// The raw signed frame.
        content: Bytes,
        /// The relaying neighbor.
        delivered_from: EndpointId,
    },
    /// A direct neighbor link formed.
    NeighborUp(EndpointId),
    /// A direct neighbor link went away.
    NeighborDown(EndpointId),
    /// The carrier fell behind and dropped frames. Sessions respond by
    /// pulling retained history via catch-up sync, so this is loss of
    /// timeliness, never of correctness.
    Lagged,
}

/// The boxed event stream a session consumes. One per session: taking it
/// starts the carrier's delivery for that session.
pub type TransportEventStream = std::pin::Pin<Box<dyn Stream<Item = TransportEvent> + Send>>;

/// The gossip carrier: how a session broadcasts signed frames and learns
/// about neighbors. Implementations must preserve the contract above;
/// everything else (reliability, ordering, topology) is deliberately
/// unspecified, because the protocol is designed to tolerate all of it.
pub trait DropTransport: Send + Sync + fmt::Debug + 'static {
    /// Broadcast an already-signed frame to all current neighbors.
    fn broadcast<'a>(
        &'a self,
        frame: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>;

    /// Ask the carrier to form direct neighbor links with these peers.
    /// Best-effort: peers may be unreachable, slow, or hostile.
    fn join_peers<'a>(
        &'a self,
        peers: Vec<EndpointId>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>;

    /// Take the session's event stream. Called exactly once, when the
    /// session starts its event loop.
    fn take_events(&self) -> TransportEventStream;
}

/// The production carrier: iroh-gossip over the shared endpoint.
#[derive(Debug)]
pub struct GossipTransport {
    sender: GossipSender,
    receiver: Mutex<Option<GossipReceiver>>,
}

impl GossipTransport {
    /// Wrap a subscribed gossip topic's halves.
    pub fn new(sender: GossipSender, receiver: GossipReceiver) -> Self {
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl DropTransport for GossipTransport {
    fn broadcast<'a>(
        &'a self,
        frame: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.sender
                .broadcast(frame)
                .await
                .map_err(|e| DropError::Network(NetworkError::Gossip(e.to_string())))
        })
    }

    fn join_peers<'a>(
        &'a self,
        peers: Vec<EndpointId>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.sender
                .join_peers(peers)
                .await
                .map_err(|e| DropError::Network(NetworkError::Gossip(e.to_string())))
        })
    }

    fn take_events(&self) -> TransportEventStream {
        let receiver = self
            .receiver
            .lock()
            .expect("transport events are taken once")
            .take()
            .expect("transport events are taken once");
        use futures::StreamExt;
        Box::pin(receiver.filter_map(|item| async move {
            match item {
                Ok(GossipEvent::Received(msg)) => Some(TransportEvent::Received {
                    content: msg.content,
                    delivered_from: msg.delivered_from,
                }),
                Ok(GossipEvent::NeighborUp(peer)) => Some(TransportEvent::NeighborUp(peer)),
                Ok(GossipEvent::NeighborDown(peer)) => Some(TransportEvent::NeighborDown(peer)),
                Ok(GossipEvent::Lagged) => Some(TransportEvent::Lagged),
                // Gossip receiver errors are terminal in practice (the
                // actor is gone; the stream yields None next). The session
                // event loop used to break on these; skipping here differs
                // only when the error is transient, which iroh-gossip's
                // are not.
                Err(e) => {
                    warn!("gossip receiver error: {e}");
                    None
                }
            }
        }))
    }
}
