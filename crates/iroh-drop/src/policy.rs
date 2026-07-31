//! Local fetch policy.
//!
//! Manual fetching is the default. Automatic fetching requires explicit
//! configuration so that an untrusted participant cannot consume arbitrary
//! disk space merely by publishing offers to the gossip topic.

use std::collections::HashSet;
use std::path::PathBuf;

use crate::error::PolicyError;
use crate::message::OfferV1;

/// Policy controlling when offered blobs are fetched automatically.
#[derive(Clone, Debug)]
pub struct DropPolicy {
    /// Whether to fetch offered blobs automatically. Defaults to `false`.
    pub auto_fetch: bool,
    /// Maximum size of a single blob that may be fetched automatically.
    pub max_blob_size: u64,
    /// Maximum number of concurrent automatic fetches.
    pub max_concurrent_fetches: usize,
    /// Total byte budget for automatic fetches within one session.
    pub max_total_auto_fetch_bytes: u64,
    /// If set, only these media types are fetched automatically.
    pub accepted_media_types: Option<HashSet<String>>,
    /// Directory fetched blobs are exported to.
    pub output_directory: PathBuf,
}

impl Default for DropPolicy {
    fn default() -> Self {
        Self {
            auto_fetch: false,
            max_blob_size: 500 * 1024 * 1024,
            max_concurrent_fetches: 3,
            max_total_auto_fetch_bytes: 2 * 1024 * 1024 * 1024,
            accepted_media_types: None,
            output_directory: PathBuf::from("./downloads"),
        }
    }
}

impl DropPolicy {
    /// Decide whether an offer may be fetched automatically.
    ///
    /// Manual fetches are never subject to this check.
    pub fn check_auto_fetch(
        &self,
        offer: &OfferV1,
        active_fetches: usize,
        spent_bytes: u64,
    ) -> Result<(), PolicyError> {
        if offer.size > self.max_blob_size {
            return Err(PolicyError::TooLarge {
                size: offer.size,
                max: self.max_blob_size,
            });
        }
        if let Some(accepted) = &self.accepted_media_types {
            let mt = offer.media_type.clone().unwrap_or_default();
            if !accepted.contains(&mt) {
                return Err(PolicyError::MediaTypeRejected(mt));
            }
        }
        if active_fetches >= self.max_concurrent_fetches {
            return Err(PolicyError::ConcurrencyLimit {
                active: active_fetches,
                max: self.max_concurrent_fetches,
            });
        }
        if spent_bytes.saturating_add(offer.size) > self.max_total_auto_fetch_bytes {
            return Err(PolicyError::TotalQuotaExceeded {
                spent: spent_bytes,
                size: offer.size,
                max: self.max_total_auto_fetch_bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::hash::BlobHash;

    fn offer(size: u64, media_type: Option<&str>) -> OfferV1 {
        OfferV1 {
            blob_hash: BlobHash::from_bytes([0u8; 32]),
            name: "file.bin".into(),
            size,
            media_type: media_type.map(Into::into),
            created_at_ms: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn manual_fetch_is_default() {
        assert!(!DropPolicy::default().auto_fetch);
    }

    #[test]
    fn size_limit() {
        let policy = DropPolicy {
            max_blob_size: 100,
            ..Default::default()
        };
        assert!(policy.check_auto_fetch(&offer(100, None), 0, 0).is_ok());
        assert!(matches!(
            policy.check_auto_fetch(&offer(101, None), 0, 0),
            Err(PolicyError::TooLarge {
                size: 101,
                max: 100
            })
        ));
    }

    #[test]
    fn media_type_filter() {
        let policy = DropPolicy {
            accepted_media_types: Some(HashSet::from(["text/plain".to_string()])),
            ..Default::default()
        };
        assert!(policy
            .check_auto_fetch(&offer(1, Some("text/plain")), 0, 0)
            .is_ok());
        assert!(matches!(
            policy.check_auto_fetch(&offer(1, Some("video/mp4")), 0, 0),
            Err(PolicyError::MediaTypeRejected(_))
        ));
    }

    #[test]
    fn total_quota() {
        let policy = DropPolicy {
            max_total_auto_fetch_bytes: 1000,
            max_blob_size: u64::MAX,
            ..Default::default()
        };
        assert!(policy.check_auto_fetch(&offer(600, None), 0, 400).is_ok());
        assert!(matches!(
            policy.check_auto_fetch(&offer(601, None), 0, 400),
            Err(PolicyError::TotalQuotaExceeded { .. })
        ));
    }

    #[test]
    fn concurrency_limit() {
        let policy = DropPolicy {
            max_concurrent_fetches: 2,
            ..Default::default()
        };
        assert!(policy.check_auto_fetch(&offer(1, None), 1, 0).is_ok());
        assert!(matches!(
            policy.check_auto_fetch(&offer(1, None), 2, 0),
            Err(PolicyError::ConcurrencyLimit { active: 2, max: 2 })
        ));
    }
}

/// A hook for deciding what to do with an incoming offer.
///
/// [`DropPolicy`] answers with *limits*; a decider answers with *judgement* —
/// an allowlist of peers, a prompt to the user, a quota per project, a check
/// against an inventory the application already has. Deciders run after the
/// protocol has verified and validated an offer and after policy limits have
/// been applied, so an implementation cannot be used to bypass either: it can
/// only be more conservative.
pub trait OfferDecider: std::fmt::Debug + Send + Sync + 'static {
    /// Judge one offer.
    fn decide(&self, offer: &OfferV1, context: &OfferContext) -> OfferDecision;
}

/// What the protocol knows about an offer when asking a decider.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OfferContext {
    /// The verified author of the offer.
    pub author: iroh::EndpointId,
    /// The peer that delivered it (may differ from the author).
    pub delivered_from: iroh::EndpointId,
    /// Whether this hash is new to this session.
    pub is_new: bool,
    /// Whether policy limits would allow an automatic fetch.
    pub policy_allows_auto_fetch: bool,
}

/// A decider's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OfferDecision {
    /// Record the offer and fetch it if policy allows.
    Accept,
    /// Record the offer but do not fetch it automatically.
    RecordOnly,
    /// Ignore the offer entirely, with a reason for the event log.
    Reject(String),
}

/// The default decider: defer entirely to [`DropPolicy`].
#[derive(Clone, Copy, Debug, Default)]
pub struct PolicyDecider;

impl OfferDecider for PolicyDecider {
    fn decide(&self, _offer: &OfferV1, context: &OfferContext) -> OfferDecision {
        if context.policy_allows_auto_fetch {
            OfferDecision::Accept
        } else {
            OfferDecision::RecordOnly
        }
    }
}
