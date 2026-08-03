//! An in-memory [`DropTransport`]: a process-local gossip bus with
//! controllable topology and deterministic fault injection.
//!
//! Topology is exactly what the test sets up: a joining session neighbors
//! the ticket's bootstrap peers (and vice versa), nothing else. Delivery is
//! synchronous and instant, which is what makes the protocol-logic tests
//! fast — no mesh formation, no propagation waits.
//!
//! Fault injection, all deterministic:
//!
//! * [`MemBus::mute`] / [`MemBus::unmute`] — a member's outgoing frames are
//!   dropped (message loss).
//! * [`MemBus::hold`] + [`MemBus::release_fifo`] /
//!   [`MemBus::release_reversed`] — queue a member's outgoing frames, then
//!   deliver them in order or reversed (delay / reordering).
//! * Dropping a session's protocol unregisters it and delivers
//!   `NeighborDown` to its neighbors (peer exit).
//!
//! Blob transfer and catch-up sync are unaffected: sessions still run on a
//! real loopback endpoint. Only the gossip carrier is simulated.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use iroh::{EndpointAddr, EndpointId};
use iroh_drop::transport::TransportEventStream;
use iroh_drop::{DropError, DropStack, DropTransport, TransportEvent};
use iroh_gossip::proto::TopicId;
use tokio::sync::mpsc;

/// One registered member's bus-side state.
#[derive(Debug)]
struct Member {
    tx: mpsc::UnboundedSender<TransportEvent>,
    neighbors: HashSet<EndpointId>,
    /// Weak so a dropped protocol really dies (its endpoint stops serving);
    /// the bus must never keep a peer's stack alive on its own.
    stack: Weak<DropStack>,
    /// The member's own address, taught to peers the way iroh-gossip
    /// disseminates `PeerData` through the swarm.
    addr: EndpointAddr,
    /// Outgoing frames are dropped while true (message loss).
    muted: bool,
    /// Outgoing frames are queued while `Some` (delay/reorder injection).
    held: Option<Vec<Bytes>>,
}

impl Member {
    fn learn_addr(&self, addr: EndpointAddr) {
        if let Some(stack) = self.stack.upgrade() {
            stack.add_known_addr(addr);
        }
    }
}

/// The shared bus all `MemTransport`s on a topic talk through.
#[derive(Debug, Default)]
pub struct MemBus {
    topics: Mutex<HashMap<TopicId, HashMap<EndpointId, Member>>>,
}

impl MemBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a member and wire its initial neighbor links. This is the
    /// [`iroh_drop::TransportFactory`] implementation: bootstrap peers that
    /// are present become mutual neighbors, everything else stays
    /// unreachable until `join_peers` or a later registration links them.
    ///
    /// Like real gossip's `PeerData` dissemination, addresses spread
    /// through the swarm: the new member learns every current member's
    /// address, and every current member learns theirs. That is what makes
    /// providers heard about through relayed frames dialable, the same
    /// invariant the real carrier provides once its shuffle converges —
    /// here it converges immediately and deterministically.
    pub fn register(
        self: &Arc<Self>,
        stack: &Arc<DropStack>,
        topic: TopicId,
        bootstrap: Vec<EndpointId>,
    ) -> Result<Arc<dyn DropTransport>, DropError> {
        let self_id = stack.endpoint.id();
        let self_addr = stack.addr();
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut topics = self.topics.lock().unwrap();
            let members = topics.entry(topic).or_default();
            for (peer, member) in members.iter_mut() {
                // Addresses cross-pollinate regardless of neighbor links…
                stack.add_known_addr(member.addr.clone());
                member.learn_addr(self_addr.clone());
                // …and bootstrap peers additionally become neighbors.
                if bootstrap.contains(peer) && member.neighbors.insert(self_id) {
                    let _ = member.tx.send(TransportEvent::NeighborUp(self_id));
                    let _ = tx.send(TransportEvent::NeighborUp(*peer));
                }
            }
            let neighbors: HashSet<EndpointId> = bootstrap
                .iter()
                .copied()
                .filter(|p| members.contains_key(p))
                .collect();
            members.insert(
                self_id,
                Member {
                    tx,
                    neighbors,
                    stack: Arc::downgrade(stack),
                    addr: self_addr,
                    muted: false,
                    held: None,
                },
            );
        }
        Ok(Arc::new(MemTransport {
            bus: Arc::clone(self),
            topic,
            self_id,
            rx: Mutex::new(Some(rx)),
        }))
    }

    /// Drop `id`'s outgoing frames (message loss) until [`Self::unmute`].
    pub fn mute(&self, topic: TopicId, id: EndpointId) {
        self.member_mut(topic, id, |m| m.muted = true);
    }

    /// Let `id`'s outgoing frames flow again.
    pub fn unmute(&self, topic: TopicId, id: EndpointId) {
        self.member_mut(topic, id, |m| m.muted = false);
    }

    /// Queue `id`'s outgoing frames instead of delivering them.
    pub fn hold(&self, topic: TopicId, id: EndpointId) {
        self.member_mut(topic, id, |m| m.held = Some(Vec::new()));
    }

    /// Deliver held frames in their original order, then resume live flow.
    pub fn release_fifo(&self, topic: TopicId, id: EndpointId) {
        self.release(topic, id, false);
    }

    /// Deliver held frames reversed (deterministic reordering), then resume.
    pub fn release_reversed(&self, topic: TopicId, id: EndpointId) {
        self.release(topic, id, true);
    }

    fn release(&self, topic: TopicId, id: EndpointId, reversed: bool) {
        let mut topics = self.topics.lock().unwrap();
        let Some(members) = topics.get_mut(&topic) else {
            return;
        };
        let Some(held) = members.get_mut(&id).and_then(|m| m.held.take()) else {
            return;
        };
        let frames: Vec<Bytes> = if reversed {
            held.into_iter().rev().collect()
        } else {
            held
        };
        for frame in frames {
            deliver(members, id, frame);
        }
    }

    fn member_mut(&self, topic: TopicId, id: EndpointId, f: impl FnOnce(&mut Member)) {
        let mut topics = self.topics.lock().unwrap();
        if let Some(member) = topics.get_mut(&topic).and_then(|m| m.get_mut(&id)) {
            f(member);
        }
    }

    fn broadcast(&self, topic: TopicId, from: EndpointId, frame: Bytes) {
        let mut topics = self.topics.lock().unwrap();
        let Some(members) = topics.get_mut(&topic) else {
            return;
        };
        let Some(sender) = members.get(&from) else {
            return;
        };
        if sender.muted {
            return; // loss
        }
        if sender.held.is_some() {
            members
                .get_mut(&from)
                .expect("sender present")
                .held
                .as_mut()
                .expect("held")
                .push(frame);
            return; // delayed
        }
        deliver(members, from, frame);
    }

    fn join_peers(&self, topic: TopicId, from: EndpointId, peers: &[EndpointId]) {
        let mut topics = self.topics.lock().unwrap();
        let Some(members) = topics.get_mut(&topic) else {
            return;
        };
        for peer in peers {
            let from_addr = {
                let Some(sender) = members.get_mut(&from) else {
                    return;
                };
                if !sender.neighbors.insert(*peer) {
                    continue; // already neighbors; gossip would not re-announce
                }
                let _ = sender.tx.send(TransportEvent::NeighborUp(*peer));
                sender.addr.clone()
            };
            // The new link also makes the two dialable, the way a real
            // gossip connection handshake exchanges addresses.
            let peer_addr = members.get_mut(peer).map(|member| {
                member.learn_addr(from_addr);
                let addr = member.addr.clone();
                if member.neighbors.insert(from) {
                    let _ = member.tx.send(TransportEvent::NeighborUp(from));
                }
                addr
            });
            if let (Some(peer_addr), Some(sender)) = (peer_addr, members.get(&from)) {
                sender.learn_addr(peer_addr);
            }
        }
    }

    fn leave(&self, topic: TopicId, id: EndpointId) {
        let mut topics = self.topics.lock().unwrap();
        let Some(members) = topics.get_mut(&topic) else {
            return;
        };
        let Some(leaving) = members.remove(&id) else {
            return;
        };
        for peer in leaving.neighbors {
            if let Some(member) = members.get_mut(&peer) {
                member.neighbors.remove(&id);
                let _ = member.tx.send(TransportEvent::NeighborDown(id));
            }
        }
    }
}

/// Deliver `frame` to every current neighbor of `from`.
fn deliver(members: &mut HashMap<EndpointId, Member>, from: EndpointId, frame: Bytes) {
    let Some(sender) = members.get(&from) else {
        return;
    };
    let neighbors: Vec<EndpointId> = sender.neighbors.iter().copied().collect();
    for peer in neighbors {
        if let Some(member) = members.get(&peer) {
            let _ = member.tx.send(TransportEvent::Received {
                content: frame.clone(),
                delivered_from: from,
            });
        }
    }
}

/// A session's handle to the bus. Unregisters (with `NeighborDown` to its
/// neighbors) when dropped.
#[derive(Debug)]
pub struct MemTransport {
    bus: Arc<MemBus>,
    topic: TopicId,
    self_id: EndpointId,
    rx: Mutex<Option<mpsc::UnboundedReceiver<TransportEvent>>>,
}

impl DropTransport for MemTransport {
    fn broadcast<'a>(
        &'a self,
        frame: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.bus.broadcast(self.topic, self.self_id, frame);
            Ok(())
        })
    }

    fn join_peers<'a>(
        &'a self,
        peers: Vec<EndpointId>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DropError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.bus.join_peers(self.topic, self.self_id, &peers);
            Ok(())
        })
    }

    fn take_events(&self) -> TransportEventStream {
        let mut rx = self
            .rx
            .lock()
            .expect("transport events are taken once")
            .take()
            .expect("transport events are taken once");
        Box::pin(futures::stream::poll_fn(move |cx| rx.poll_recv(cx)))
    }
}

impl Drop for MemTransport {
    fn drop(&mut self) {
        self.bus.leave(self.topic, self.self_id);
    }
}
