//! Provider tracking and selection.
//!
//! The provider index may become stale: peers leave without withdrawing.
//! Selection therefore tries known providers in a deterministic order,
//! deprioritizes recent failures, and the caller continues to the next
//! provider on failure while preserving session health.

use n0_future::time::Instant;
use std::collections::HashMap;

use iroh::EndpointId;

/// Largest number of providers tracked for one blob. Beyond this, the
/// least useful entry (most failures, then oldest assertion) is dropped:
/// a swarm needs a handful of healthy sources, not an unbounded list.
pub const MAX_PROVIDERS_PER_BLOB: usize = 64;

/// Per-provider failure bookkeeping.
#[derive(Clone, Debug, Default)]
pub struct ProviderInfo {
    /// Number of consecutive failed fetch attempts.
    pub failures: u32,
    /// When the last failure happened.
    pub last_failure: Option<Instant>,
    /// Whether the provider explicitly announced availability.
    pub announced: bool,
    /// The author's own timestamp for its most recent assertion about this
    /// blob, in milliseconds since the Unix epoch. Used only to order that
    /// author's own claims (see [`ProviderSet::record`]).
    pub announced_at_ms: u64,
    /// The frame id of the assertion that set `announced_at_ms`; the
    /// deterministic tie-break for equal timestamps.
    pub last_frame_id: [u8; 16],
    /// Whether the provider has withdrawn. Withdrawn entries are kept as
    /// tombstones so a stale relay cannot resurrect them.
    pub withdrawn: bool,
}

/// The set of peers known to (claim to) serve one blob.
#[derive(Clone, Debug, Default)]
pub struct ProviderSet {
    /// The original offerer, tried first among equals.
    original: Option<EndpointId>,
    /// All known providers.
    providers: HashMap<EndpointId, ProviderInfo>,
}

impl ProviderSet {
    /// Add a provider. `is_original` marks the author of the first offer.
    pub fn add(&mut self, id: EndpointId, is_original: bool) {
        if is_original && self.original.is_none() {
            self.original = Some(id);
        }
        self.providers.entry(id).or_default();
    }

    /// Mark an explicit availability announcement.
    pub fn mark_announced(&mut self, id: EndpointId) {
        self.record(id, u64::MAX, [0xFF; 16], false);
    }

    /// Apply an assertion from `id` about itself, newest-wins.
    ///
    /// `announced_at_ms` is the author's own clock. Because a peer can only
    /// assert things about itself, using its clock to order its own claims is
    /// safe from third-party interference, and it is what stops an old
    /// `Available` — replayed later by some other peer's catch-up log — from
    /// undoing a `Withdrawing` that came after it.
    ///
    /// Returns whether this assertion changed anything.
    pub fn record(
        &mut self,
        id: EndpointId,
        announced_at_ms: u64,
        frame_id: [u8; 16],
        withdrawn: bool,
    ) -> bool {
        let entry = self.providers.entry(id).or_default();
        // An assertion older than what we already have is stale. Ties are
        // broken by frame id so that every peer — whatever order its relay
        // delivered the two assertions in — converges on the same winner.
        if (announced_at_ms, frame_id) < (entry.announced_at_ms, entry.last_frame_id) {
            return false;
        }
        let changed = entry.withdrawn != withdrawn || !entry.announced;
        entry.announced = true;
        entry.announced_at_ms = announced_at_ms;
        entry.last_frame_id = frame_id;
        entry.withdrawn = withdrawn;
        if withdrawn && self.original == Some(id) {
            self.original = None;
        }
        self.enforce_cap();
        changed
    }

    /// Remove a provider outright, forgetting even the tombstone.
    pub fn remove(&mut self, id: EndpointId) {
        self.providers.remove(&id);
        if self.original == Some(id) {
            self.original = None;
        }
    }

    /// Drop the least useful entries once the cap is exceeded.
    fn enforce_cap(&mut self) {
        if self.providers.len() <= MAX_PROVIDERS_PER_BLOB {
            return;
        }
        let mut ranked: Vec<(EndpointId, u32, u64, bool)> = self
            .providers
            .iter()
            .map(|(id, info)| (*id, info.failures, info.announced_at_ms, info.withdrawn))
            .collect();
        // Worst last: withdrawn first, then most failures, then oldest.
        ranked.sort_by(|a, b| {
            b.3.cmp(&a.3)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        for (id, ..) in ranked
            .into_iter()
            .take(self.providers.len() - MAX_PROVIDERS_PER_BLOB)
        {
            self.remove(id);
        }
    }

    /// Record a failed fetch attempt from a provider.
    pub fn mark_failure(&mut self, id: EndpointId) {
        let info = self.providers.entry(id).or_default();
        info.failures = info.failures.saturating_add(1);
        info.last_failure = Some(Instant::now());
    }

    /// Record a successful fetch from a provider, resetting its backoff.
    pub fn mark_success(&mut self, id: EndpointId) {
        if let Some(info) = self.providers.get_mut(&id) {
            info.failures = 0;
            info.last_failure = None;
        }
    }

    /// All known providers in fetch order: the original offerer first, then
    /// by ascending failure count, then by id for determinism.
    pub fn ordered(&self) -> Vec<EndpointId> {
        let mut providers: Vec<(EndpointId, &ProviderInfo)> = self
            .providers
            .iter()
            .filter(|(_, info)| !info.withdrawn)
            .map(|(id, info)| (*id, info))
            .collect();
        providers.sort_by(|(a_id, a), (b_id, b)| {
            a.failures
                .cmp(&b.failures)
                .then_with(|| {
                    let a_orig = self.original == Some(*a_id);
                    let b_orig = self.original == Some(*b_id);
                    b_orig.cmp(&a_orig)
                })
                .then_with(|| a_id.cmp(b_id))
        });
        providers.into_iter().map(|(id, _)| id).collect()
    }

    /// All known providers in fetch order, excluding the given id (usually
    /// ourselves).
    pub fn ordered_excluding(&self, id: EndpointId) -> Vec<EndpointId> {
        self.ordered().into_iter().filter(|p| *p != id).collect()
    }

    /// Number of live (non-withdrawn) providers.
    pub fn len(&self) -> usize {
        self.providers.values().filter(|i| !i.withdrawn).count()
    }

    /// Whether no live providers are known.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over live provider ids.
    pub fn iter(&self) -> impl Iterator<Item = EndpointId> + '_ {
        self.providers
            .iter()
            .filter(|(_, info)| !info.withdrawn)
            .map(|(id, _)| *id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn id(seed: u8) -> EndpointId {
        EndpointId::from(SecretKey::from_bytes(&[seed; 32]).public())
    }

    #[test]
    fn original_comes_first() {
        let mut set = ProviderSet::default();
        set.add(id(1), false);
        set.add(id(2), true);
        set.add(id(3), false);
        assert_eq!(set.ordered()[0], id(2));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn failures_deprioritize() {
        let mut set = ProviderSet::default();
        set.add(id(1), true);
        set.add(id(2), false);
        set.mark_failure(id(1));
        set.mark_failure(id(1));
        set.mark_failure(id(2));
        // id(2) has fewer failures, so it now leads.
        assert_eq!(set.ordered()[0], id(2));
        set.mark_success(id(1));
        // Back to equal failures; original leads again.
        assert_eq!(set.ordered()[0], id(1));
    }

    #[test]
    fn withdrawal_removes() {
        let mut set = ProviderSet::default();
        set.add(id(1), true);
        set.add(id(2), false);
        set.remove(id(1));
        assert_eq!(set.ordered(), vec![id(2)]);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn exclusion() {
        let mut set = ProviderSet::default();
        set.add(id(1), true);
        set.add(id(2), false);
        let ordered = set.ordered_excluding(id(1));
        assert_eq!(ordered, vec![id(2)]);
    }
}
