//! Local state model for a drop session.
//!
//! Blob bytes persist according to the configured blob store. Offer metadata,
//! provider information, and topic membership are in-memory for the MVP: after
//! a restart a process may retain the bytes but lose filenames, aliases,
//! provider history, and membership.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::proto::TopicId;

use crate::hash::BlobHash;
use crate::message::OfferV1;
use crate::policy::DropPolicy;
use crate::provider::ProviderSet;

/// A message deduplication key: the message id together with its verified
/// author, so different authors may legitimately reuse id sequences.
pub type DedupKey = (EndpointId, [u8; 16]);

/// Default maximum number of entries in the dedup cache.
pub const DEDUP_CAPACITY: usize = 10_000;

/// Default TTL for dedup entries.
pub const DEDUP_TTL: Duration = Duration::from_secs(10 * 60);

/// Largest number of offers one session will remember.
///
/// Offers are cheap to make and cost the receiver memory, so the table is
/// bounded: past this point the least recently seen offer is evicted. Bytes
/// already in the blob store are unaffected — only the index shrinks.
pub const MAX_OFFERS: usize = 4096;

/// Largest number of offers a single author may occupy in that table.
///
/// Stops one peer from evicting everyone else's offers by announcing
/// thousands of its own.
pub const MAX_OFFERS_PER_AUTHOR: usize = 512;

/// Largest number of display names remembered per blob.
pub const MAX_ALIASES_PER_OFFER: usize = 16;

/// Largest number of peers remembered per session.
pub const MAX_KNOWN_PEERS: usize = 1024;

/// Largest number of unknown-kind frames retained for relaying, as a share of
/// [`SYNC_LOG_CAP`]. Relaying extensions we do not understand is useful; being
/// filled with junk is not.
pub const MAX_UNKNOWN_FRAMES: usize = 256;

/// Maximum number of signed frames retained for catch-up sync.
///
/// Offers and provider announcements are retained as their original signed
/// wire frames so that late joiners can be caught up over the control ALPN:
/// frames verify against their embedded author key no matter who relays
/// them. Oldest-first eviction when full.
pub const SYNC_LOG_CAP: usize = 4096;

/// What happened when an offer was recorded.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OfferOutcome {
    /// A hash we had not seen before. Recording it may have evicted others.
    New {
        /// Offers forgotten to make room, oldest first.
        evicted: Vec<BlobHash>,
    },
    /// A hash we already knew; any new name was kept as an alias.
    Known,
    /// The author has too many offers in this session already.
    QuotaExceeded,
}

impl OfferOutcome {
    /// Whether this offer introduced a new hash.
    pub fn is_new(&self) -> bool {
        matches!(self, OfferOutcome::New { .. })
    }

    /// Whether the offer was refused.
    pub fn is_rejected(&self) -> bool {
        matches!(self, OfferOutcome::QuotaExceeded)
    }
}

/// One page of retained sync frames.
#[derive(Debug)]
pub struct SyncPage {
    /// Consecutive frames starting at the requested cursor (clamped to the
    /// retained window).
    pub frames: Vec<Bytes>,
    /// Absolute cursor one past the last returned frame.
    pub end_cursor: u64,
    /// Whether the page reaches the current end of the log.
    pub caught_up: bool,
}

/// Bounded dedup cache with TTL and oldest-first eviction.
#[derive(Debug)]
pub struct DedupCache {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<DedupKey, Instant>,
    order: VecDeque<DedupKey>,
}

impl DedupCache {
    /// Create a cache with the given capacity and TTL.
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity,
            ttl,
            entries: HashMap::with_capacity(capacity.min(1024)),
            order: VecDeque::with_capacity(capacity.min(1024)),
        }
    }

    /// Returns `true` if the key is new (and records it), `false` if it was
    /// seen recently.
    pub fn check_and_insert(&mut self, key: DedupKey) -> bool {
        self.evict_expired();
        if self.entries.contains_key(&key) {
            return false;
        }
        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        self.entries.insert(key, Instant::now());
        self.order.push_back(key);
        true
    }

    fn evict_expired(&mut self) {
        let now = Instant::now();
        while let Some(front) = self.order.front() {
            let expired = self
                .entries
                .get(front)
                .map(|t| now.duration_since(*t) > self.ttl)
                .unwrap_or(true);
            if expired {
                let key = self.order.pop_front().expect("front existed");
                self.entries.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for DedupCache {
    fn default() -> Self {
        Self::new(DEDUP_CAPACITY, DEDUP_TTL)
    }
}

/// Local status of a blob relative to this peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalBlobStatus {
    /// We do not have the blob.
    Missing,
    /// A fetch is in progress.
    Fetching {
        /// Bytes downloaded so far.
        downloaded: u64,
        /// Advertised total, if known.
        total: Option<u64>,
    },
    /// The blob is stored completely and verified; we can serve it.
    Complete,
    /// The last fetch failed.
    Failed {
        /// Whether retrying might succeed.
        retryable: bool,
        /// Human-readable failure description.
        message: String,
    },
}

impl std::fmt::Display for LocalBlobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "missing"),
            Self::Fetching { downloaded, total } => match total {
                Some(total) if *total > 0 => {
                    write!(f, "fetching {:>3}%", downloaded * 100 / total)
                }
                _ => write!(f, "fetching"),
            },
            Self::Complete => write!(f, "complete"),
            Self::Failed { retryable, .. } => {
                write!(f, "failed{}", if *retryable { " (retryable)" } else { "" })
            }
        }
    }
}

/// Everything we know about one offered blob.
#[derive(Clone, Debug)]
pub struct OfferRecord {
    /// The first valid offer we accepted for this hash.
    pub offer: OfferV1,
    /// The verified author of that offer.
    pub first_seen_from: EndpointId,
    /// When we first saw it (local monotonic clock).
    pub first_seen_at: Instant,
    /// All names this hash has been offered under, including `offer.name`.
    pub aliases: HashSet<String>,
    /// Local availability of the bytes.
    pub local_status: LocalBlobStatus,
}

impl OfferRecord {
    /// The preferred display name: the first name the hash was offered under.
    pub fn display_name(&self) -> &str {
        &self.offer.name
    }
}

/// The full local state of one drop session.
#[derive(Debug)]
pub struct DropState {
    /// The gossip topic of this drop.
    pub topic_id: TopicId,
    /// Our own endpoint identity.
    pub self_endpoint_id: EndpointId,
    /// All accepted offers by content hash.
    pub offers: HashMap<BlobHash, OfferRecord>,
    /// Known providers by content hash.
    pub providers: HashMap<BlobHash, ProviderSet>,
    /// Recently seen message ids.
    pub seen_messages: DedupCache,
    /// The local policy.
    pub policy: DropPolicy,
    /// Peers we currently know about (neighbors and message authors).
    pub known_peers: HashSet<EndpointId>,
    /// Offer hashes in least-recently-seen order, for eviction.
    offer_order: VecDeque<BlobHash>,
    /// How many offers each author currently occupies.
    offers_per_author: HashMap<EndpointId, usize>,
    /// Peers in insertion order, for bounded eviction.
    peer_order: VecDeque<EndpointId>,
    /// How many retained frames carry kinds we do not understand.
    unknown_frames: usize,
    /// Retained signed frames (offers and provider announcements) for
    /// catch-up sync, in the order they were accepted.
    sync_log: VecDeque<Bytes>,
    /// Absolute sequence number of `sync_log`'s front element (grows as the
    /// log evicts from the front); used as the sync cursor base.
    sync_log_start: u64,
}

impl DropState {
    /// Create empty state for a session.
    pub fn new(topic_id: TopicId, self_endpoint_id: EndpointId, policy: DropPolicy) -> Self {
        Self {
            topic_id,
            self_endpoint_id,
            offers: HashMap::new(),
            providers: HashMap::new(),
            seen_messages: DedupCache::default(),
            policy,
            known_peers: HashSet::new(),
            offer_order: VecDeque::new(),
            offers_per_author: HashMap::new(),
            peer_order: VecDeque::new(),
            unknown_frames: 0,
            sync_log: VecDeque::new(),
            sync_log_start: 0,
        }
    }

    /// Remember a peer, forgetting the oldest once [`MAX_KNOWN_PEERS`] is hit.
    pub fn note_peer(&mut self, peer: EndpointId) {
        if self.known_peers.insert(peer) {
            self.peer_order.push_back(peer);
            while self.peer_order.len() > MAX_KNOWN_PEERS {
                if let Some(old) = self.peer_order.pop_front() {
                    self.known_peers.remove(&old);
                }
            }
        }
    }

    /// Apply a provider assertion, newest-wins. Returns whether it changed
    /// anything worth telling the application about.
    pub fn record_provider(
        &mut self,
        hash: BlobHash,
        peer: EndpointId,
        announced_at_ms: u64,
    ) -> bool {
        self.providers
            .entry(hash)
            .or_default()
            .record(peer, announced_at_ms, false)
    }

    /// Apply a withdrawal, newest-wins.
    pub fn withdraw_provider(
        &mut self,
        hash: BlobHash,
        peer: EndpointId,
        announced_at_ms: u64,
    ) -> bool {
        self.providers
            .entry(hash)
            .or_default()
            .record(peer, announced_at_ms, true)
    }

    /// Note that this offer was just seen, moving it to the back of the
    /// eviction queue.
    fn touch_offer(&mut self, hash: BlobHash) {
        if let Some(pos) = self.offer_order.iter().position(|h| *h == hash) {
            self.offer_order.remove(pos);
        }
        self.offer_order.push_back(hash);
    }

    /// Whether `author` may add another offer.
    pub fn author_has_quota(&self, author: EndpointId) -> bool {
        self.offers_per_author.get(&author).copied().unwrap_or(0) < MAX_OFFERS_PER_AUTHOR
    }

    /// Evict least-recently-seen offers until the table fits.
    /// Returns the hashes that were forgotten.
    fn enforce_offer_cap(&mut self) -> Vec<BlobHash> {
        let mut evicted = Vec::new();
        while self.offers.len() > MAX_OFFERS {
            let Some(oldest) = self.offer_order.pop_front() else {
                break;
            };
            if let Some(record) = self.offers.remove(&oldest) {
                if let Some(count) = self.offers_per_author.get_mut(&record.first_seen_from) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.offers_per_author.remove(&record.first_seen_from);
                    }
                }
                self.providers.remove(&oldest);
                evicted.push(oldest);
            }
        }
        evicted
    }

    /// Retain a signed frame for catch-up sync.
    pub fn retain_frame(&mut self, frame: Bytes) {
        if self.sync_log.len() >= SYNC_LOG_CAP {
            self.sync_log.pop_front();
            self.sync_log_start += 1;
        }
        self.sync_log.push_back(frame);
    }

    /// Retain a frame whose kind this build does not understand, so that this
    /// peer still relays extensions to peers that do. Bounded separately and
    /// much more tightly than known kinds.
    pub fn retain_unknown_frame(&mut self, frame: Bytes) {
        if self.unknown_frames >= MAX_UNKNOWN_FRAMES {
            return;
        }
        self.unknown_frames += 1;
        self.retain_frame(frame);
    }

    /// A page of retained frames for catch-up sync.
    /// Frames are consecutive in the log, so truncating the page from the
    /// end adjusts the cursor by exactly the number of dropped frames.
    pub fn sync_frames(&self, cursor: u64, max: usize) -> SyncPage {
        let start = cursor.saturating_sub(self.sync_log_start) as usize;
        if start >= self.sync_log.len() {
            return SyncPage {
                frames: Vec::new(),
                end_cursor: self.sync_log_start + self.sync_log.len() as u64,
                caught_up: true,
            };
        }
        let end = (start + max).min(self.sync_log.len());
        let frames: Vec<Bytes> = self.sync_log.range(start..end).cloned().collect();
        SyncPage {
            frames,
            end_cursor: self.sync_log_start + end as u64,
            caught_up: end == self.sync_log.len(),
        }
    }

    /// Record an accepted offer, merging aliases for known hashes.
    ///
    /// Returns `true` if this hash was previously unknown.
    pub fn record_offer(&mut self, from: EndpointId, offer: OfferV1) -> bool {
        self.record_offer_bounded(from, offer).is_new()
    }

    /// Record an offer, enforcing per-author quotas and the table cap.
    pub fn record_offer_bounded(&mut self, from: EndpointId, offer: OfferV1) -> OfferOutcome {
        self.note_peer(from);
        self.record_offer_inner(from, offer)
    }

    /// Apply an offer from restored history. Identical bookkeeping to
    /// [`Self::record_offer_bounded`] except the author is not credited as a
    /// connected peer — a signature from the past is not a connection.
    pub fn record_restored(&mut self, from: EndpointId, offer: OfferV1) -> OfferOutcome {
        self.record_offer_inner(from, offer)
    }

    fn record_offer_inner(&mut self, from: EndpointId, offer: OfferV1) -> OfferOutcome {
        let hash = offer.blob_hash;
        let known = self.offers.contains_key(&hash);
        if !known && !self.author_has_quota(from) {
            return OfferOutcome::QuotaExceeded;
        }
        // The original offerer is assumed to serve the content.
        self.providers.entry(hash).or_default().add(from, true);
        self.touch_offer(hash);
        match self.offers.get_mut(&hash) {
            Some(record) => {
                if record.aliases.len() < MAX_ALIASES_PER_OFFER {
                    record.aliases.insert(offer.name.clone());
                }
                OfferOutcome::Known
            }
            None => {
                let mut aliases = HashSet::new();
                aliases.insert(offer.name.clone());
                self.offers.insert(
                    hash,
                    OfferRecord {
                        offer,
                        first_seen_from: from,
                        first_seen_at: Instant::now(),
                        aliases,
                        local_status: LocalBlobStatus::Missing,
                    },
                );
                *self.offers_per_author.entry(from).or_insert(0) += 1;
                let evicted = self.enforce_offer_cap();
                OfferOutcome::New { evicted }
            }
        }
    }

    /// Find an offer by hash, by exact hash-prefix, or by name/alias.
    pub fn find_offer(&self, hash_or_name: &str) -> Option<BlobHash> {
        // Exact hash.
        if let Ok(hash) = hash_or_name.parse::<BlobHash>() {
            return Some(hash);
        }
        // Unique hash prefix.
        let prefix_matches: Vec<BlobHash> = self
            .offers
            .keys()
            .filter(|h| h.matches_prefix(hash_or_name))
            .copied()
            .collect();
        if prefix_matches.len() == 1 {
            return Some(prefix_matches[0]);
        }
        // Name or alias (exact match).
        self.offers
            .iter()
            .find(|(_, r)| r.offer.name == hash_or_name || r.aliases.contains(hash_or_name))
            .map(|(h, _)| *h)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use iroh::SecretKey;

    fn id(seed: u8) -> EndpointId {
        EndpointId::from(SecretKey::from_bytes(&[seed; 32]).public())
    }

    fn offer(hash: BlobHash, name: &str) -> OfferV1 {
        OfferV1 {
            blob_hash: hash,
            name: name.into(),
            size: 1,
            media_type: None,
            created_at_ms: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn dedup_detects_replays() {
        let mut cache = DedupCache::new(4, Duration::from_secs(60));
        let key = (id(1), [7u8; 16]);
        assert!(cache.check_and_insert(key));
        assert!(!cache.check_and_insert(key));
        // Different author with same message id is a different key.
        assert!(cache.check_and_insert((id(2), [7u8; 16])));
    }

    #[test]
    fn dedup_evicts_oldest_at_capacity() {
        let mut cache = DedupCache::new(2, Duration::from_secs(3600));
        let a = (id(1), [1u8; 16]);
        let b = (id(1), [2u8; 16]);
        let c = (id(1), [3u8; 16]);
        assert!(cache.check_and_insert(a));
        assert!(cache.check_and_insert(b));
        assert!(cache.check_and_insert(c)); // evicts a
        assert_eq!(cache.len(), 2);
        assert!(cache.check_and_insert(a)); // a was evicted, so it's new again
    }

    #[test]
    fn same_hash_multiple_names_are_aliases() {
        let mut state =
            DropState::new(TopicId::from_bytes([0u8; 32]), id(0), DropPolicy::default());
        let hash = BlobHash::from_bytes([1u8; 32]);
        assert!(state.record_offer(id(1), offer(hash, "slides.pdf")));
        assert!(!state.record_offer(id(2), offer(hash, "deck.pdf")));
        let record = &state.offers[&hash];
        assert_eq!(record.display_name(), "slides.pdf");
        assert!(record.aliases.contains("deck.pdf"));
        assert_eq!(state.offers.len(), 1);
    }

    #[test]
    fn same_name_multiple_hashes_are_versions() {
        let mut state =
            DropState::new(TopicId::from_bytes([0u8; 32]), id(0), DropPolicy::default());
        let h1 = BlobHash::from_bytes([1u8; 32]);
        let h2 = BlobHash::from_bytes([2u8; 32]);
        assert!(state.record_offer(id(1), offer(h1, "slides.pdf")));
        assert!(state.record_offer(id(1), offer(h2, "slides.pdf")));
        assert_eq!(state.offers.len(), 2);
    }

    #[test]
    fn find_by_name_alias_and_prefix() {
        let mut state =
            DropState::new(TopicId::from_bytes([0u8; 32]), id(0), DropPolicy::default());
        let hash = BlobHash::from_bytes([0xabu8; 32]);
        state.record_offer(id(1), offer(hash, "slides.pdf"));
        state.record_offer(id(2), offer(hash, "deck.pdf"));
        assert_eq!(state.find_offer("slides.pdf"), Some(hash));
        assert_eq!(state.find_offer("deck.pdf"), Some(hash));
        assert_eq!(state.find_offer(&hash.to_hex()), Some(hash));
        assert_eq!(state.find_offer("ababab"), Some(hash)); // unique prefix
        assert_eq!(state.find_offer("cdcdcd"), None); // not a prefix
        let prefix = &hash.to_hex()[..12];
        assert_eq!(state.find_offer(prefix), Some(hash));
    }

    #[test]
    fn sync_log_pages_and_reports_catch_up() {
        let mut state =
            DropState::new(TopicId::from_bytes([9u8; 32]), id(7), DropPolicy::default());
        for i in 0..5u8 {
            state.retain_frame(Bytes::from(vec![i; 4]));
        }

        // First page stops short of the end.
        let page = state.sync_frames(0, 2);
        assert_eq!(page.frames.len(), 2);
        assert_eq!(page.end_cursor, 2);
        assert!(!page.caught_up);

        // Continuing from the returned cursor reaches the end.
        let page = state.sync_frames(page.end_cursor, 10);
        assert_eq!(page.frames.len(), 3);
        assert_eq!(page.end_cursor, 5);
        assert!(page.caught_up);

        // A cursor at (or past) the end is caught up with nothing to send.
        let page = state.sync_frames(5, 10);
        assert!(page.frames.is_empty());
        assert!(page.caught_up);
        assert_eq!(page.end_cursor, 5);
    }

    #[test]
    fn sync_log_evicts_oldest_and_keeps_cursors_absolute() {
        let mut state =
            DropState::new(TopicId::from_bytes([9u8; 32]), id(7), DropPolicy::default());
        for i in 0..(SYNC_LOG_CAP + 10) {
            state.retain_frame(Bytes::from(vec![(i % 251) as u8]));
        }

        // The window holds at most SYNC_LOG_CAP frames, ending at the
        // absolute count of everything ever retained.
        let page = state.sync_frames(0, usize::MAX);
        assert_eq!(page.frames.len(), SYNC_LOG_CAP);
        assert_eq!(page.end_cursor, (SYNC_LOG_CAP + 10) as u64);
        assert!(page.caught_up);

        // A cursor older than the window is clamped, not rejected: a joiner
        // that fell behind still gets everything still retained.
        let page = state.sync_frames(1, 8);
        assert_eq!(page.frames.len(), 8);
        assert_eq!(page.end_cursor, 18);
        assert!(!page.caught_up);
    }

    fn offer_n(i: usize) -> OfferV1 {
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&(i as u64).to_le_bytes());
        OfferV1 {
            blob_hash: BlobHash::from_bytes(seed),
            name: format!("f{i}.bin"),
            size: 1,
            media_type: None,
            created_at_ms: None,
            metadata: BTreeMap::new(),
        }
    }

    fn state() -> DropState {
        DropState::new(TopicId::from_bytes([1u8; 32]), id(1), DropPolicy::default())
    }

    #[test]
    fn one_author_cannot_exceed_its_offer_quota() {
        let mut state = state();
        let author = id(2);
        for i in 0..MAX_OFFERS_PER_AUTHOR {
            assert!(
                state.record_offer_bounded(author, offer_n(i)).is_new(),
                "offer {i} should be accepted"
            );
        }
        let over = state.record_offer_bounded(author, offer_n(MAX_OFFERS_PER_AUTHOR + 1));
        assert!(over.is_rejected(), "the quota must eventually say no");
        assert_eq!(state.offers.len(), MAX_OFFERS_PER_AUTHOR);

        // A different author still has its own quota: one peer cannot lock
        // everyone else out.
        assert!(state.record_offer_bounded(id(3), offer_n(99_999)).is_new());
    }

    #[test]
    fn repeat_offers_of_a_known_hash_do_not_consume_quota() {
        let mut state = state();
        let author = id(2);
        assert!(state.record_offer_bounded(author, offer_n(0)).is_new());
        for _ in 0..1000 {
            let mut again = offer_n(0);
            again.name = "renamed.bin".into();
            assert!(!state.record_offer_bounded(author, again).is_new());
        }
        assert_eq!(state.offers.len(), 1);
        let record = state.offers.values().next().unwrap();
        assert!(
            record.aliases.len() <= MAX_ALIASES_PER_OFFER,
            "alias sets must stay bounded, got {}",
            record.aliases.len()
        );
    }

    #[test]
    fn the_offer_table_evicts_instead_of_growing() {
        let mut state = state();
        // Spread across authors so no single quota trips first.
        for i in 0..(MAX_OFFERS + 100) {
            let author = id((i % 200) as u8);
            state.record_offer_bounded(author, offer_n(i));
        }
        assert!(
            state.offers.len() <= MAX_OFFERS,
            "offer table grew to {}",
            state.offers.len()
        );
        // The newest offer survived; something older was forgotten.
        let newest = offer_n(MAX_OFFERS + 99).blob_hash;
        assert!(state.offers.contains_key(&newest));
    }

    #[test]
    fn known_peers_stay_bounded() {
        let mut state = state();
        for i in 0..(MAX_KNOWN_PEERS + 50) {
            // 32-byte seeds give us plenty of distinct keys.
            let mut seed = [0u8; 32];
            seed[..8].copy_from_slice(&(i as u64).to_le_bytes());
            seed[31] = 1;
            let peer = EndpointId::from(SecretKey::from_bytes(&seed).public());
            state.note_peer(peer);
        }
        assert!(
            state.known_peers.len() <= MAX_KNOWN_PEERS,
            "peer set grew to {}",
            state.known_peers.len()
        );
    }

    #[test]
    fn unknown_frames_are_retained_only_within_budget() {
        let mut state = state();
        for _ in 0..(MAX_UNKNOWN_FRAMES + 500) {
            state.retain_unknown_frame(Bytes::from_static(b"opaque extension frame"));
        }
        let page = state.sync_frames(0, usize::MAX);
        assert_eq!(
            page.frames.len(),
            MAX_UNKNOWN_FRAMES,
            "relaying unknown kinds must not be a way to fill our history"
        );
    }
}
