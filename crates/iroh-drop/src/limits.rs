//! Per-peer flood control.
//!
//! Everything a peer can make us do — verify a signature, remember an offer,
//! answer a request, serve a page of history — costs us something. Gossip has
//! no admission control, so the protocol needs its own: a token bucket per
//! peer, per activity, with the bucket table itself bounded so that tracking
//! attackers cannot become the attack.
//!
//! These are deliberately generous for humans and stingy for scripts: a burst
//! is fine, a sustained flood is not.

use std::collections::HashMap;
use std::time::Duration;

use n0_future::time::Instant;

use iroh::EndpointId;

/// A classic token bucket: `burst` tokens, refilled at `per_second`.
#[derive(Clone, Copy, Debug)]
pub struct Rate {
    /// Tokens available at once.
    pub burst: f64,
    /// Tokens added per second.
    pub per_second: f64,
}

impl Rate {
    /// A rate allowing `burst` immediately and `per_second` thereafter.
    pub const fn new(burst: f64, per_second: f64) -> Self {
        Self { burst, per_second }
    }
}

/// Default: how many messages one peer may deliver to us.
pub const MESSAGES: Rate = Rate::new(64.0, 16.0);

/// Default: how often we answer one peer's blob requests.
pub const REQUESTS: Rate = Rate::new(8.0, 1.0);

/// Default: how many catch-up pages we serve one peer.
pub const SYNC_PAGES: Rate = Rate::new(32.0, 4.0);

/// Largest number of peers tracked per activity. Beyond this the least
/// recently seen peer is forgotten, which at worst grants a stale attacker a
/// fresh bucket — far better than unbounded memory.
pub const MAX_TRACKED_PEERS: usize = 4096;

#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Token buckets keyed by peer, bounded in size.
#[derive(Debug)]
pub struct PeerRateLimiter {
    rate: Rate,
    buckets: HashMap<EndpointId, Bucket>,
    order: std::collections::VecDeque<EndpointId>,
}

impl PeerRateLimiter {
    /// A limiter enforcing `rate` per peer.
    pub fn new(rate: Rate) -> Self {
        Self {
            rate,
            buckets: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    /// Take one token for `peer`, returning whether it was allowed.
    pub fn allow(&mut self, peer: EndpointId) -> bool {
        self.allow_at(peer, Instant::now())
    }

    /// Testable form of [`Self::allow`].
    pub fn allow_at(&mut self, peer: EndpointId, now: Instant) -> bool {
        let rate = self.rate;
        let fresh = !self.buckets.contains_key(&peer);
        let bucket = self.buckets.entry(peer).or_insert(Bucket {
            tokens: rate.burst,
            last: now,
        });
        if !fresh {
            let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * rate.per_second).min(rate.burst);
        }
        bucket.last = now;
        let allowed = if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        };
        if fresh {
            self.order.push_back(peer);
            self.evict();
        }
        allowed
    }

    /// Forget the least recently added peers past the cap.
    fn evict(&mut self) {
        while self.order.len() > MAX_TRACKED_PEERS {
            if let Some(old) = self.order.pop_front() {
                self.buckets.remove(&old);
            }
        }
    }

    /// Number of peers currently tracked.
    pub fn tracked(&self) -> usize {
        self.buckets.len()
    }
}

/// How long a peer must wait between identical request answers.
pub const REQUEST_REPLY_INTERVAL: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn id(seed: u8) -> EndpointId {
        EndpointId::from(SecretKey::from_bytes(&[seed; 32]).public())
    }

    #[test]
    fn allows_a_burst_then_throttles() {
        let mut limiter = PeerRateLimiter::new(Rate::new(3.0, 1.0));
        let peer = id(1);
        let now = Instant::now();
        assert!(limiter.allow_at(peer, now));
        assert!(limiter.allow_at(peer, now));
        assert!(limiter.allow_at(peer, now));
        assert!(!limiter.allow_at(peer, now), "burst is spent");

        // A second later, one token is back.
        let later = now + Duration::from_secs(1);
        assert!(limiter.allow_at(peer, later));
        assert!(!limiter.allow_at(peer, later));
    }

    #[test]
    fn peers_are_limited_independently() {
        let mut limiter = PeerRateLimiter::new(Rate::new(1.0, 0.0));
        let now = Instant::now();
        assert!(limiter.allow_at(id(1), now));
        assert!(!limiter.allow_at(id(1), now));
        assert!(
            limiter.allow_at(id(2), now),
            "one peer cannot starve another"
        );
    }

    #[test]
    fn bucket_table_is_bounded() {
        let mut limiter = PeerRateLimiter::new(Rate::new(1.0, 1.0));
        let now = Instant::now();
        for seed in 0..=255u8 {
            limiter.allow_at(id(seed), now);
        }
        assert!(limiter.tracked() <= MAX_TRACKED_PEERS);
        assert_eq!(limiter.tracked(), 256, "under the cap, everyone is tracked");
    }
}
