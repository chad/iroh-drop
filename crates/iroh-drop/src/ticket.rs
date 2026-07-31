//! Drop tickets: the bearer capability required to join a drop.
//!
//! A ticket is a compact binary document (postcard-encoded) rendered for
//! humans as lowercase base32 with a `drop1` prefix:
//!
//! ```text
//! drop1q...
//! ```
//!
//! Anyone possessing the ticket can participate in the drop. It is not a
//! durable identity, not revocable, and not an ACL. If a ticket leaks, create
//! a new drop with a new topic and distribute a new ticket.

use std::{fmt, str::FromStr};

use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};

use crate::error::TicketError;
use crate::message::WIRE_VERSION;

/// Human-readable prefix for encoded tickets.
pub const TICKET_PREFIX: &str = "drop1";

/// Maximum accepted length of an encoded ticket, in characters.
///
/// Decoders must reject longer inputs before allocating any collections.
pub const MAX_TICKET_LEN: usize = 8192;

/// Maximum number of bootstrap nodes in a ticket.
pub const MAX_BOOTSTRAP_NODES: usize = 16;

/// Maximum length of the optional display name, in UTF-8 bytes.
pub const MAX_DISPLAY_NAME_LEN: usize = 255;

/// Version 1 of the drop ticket schema.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DropTicketV1 {
    /// Wire major version of the drop protocol this ticket belongs to.
    pub version: u16,
    /// The gossip topic that carries drop coordination messages.
    pub topic_id: [u8; 32],
    /// Peers to bootstrap from. Bounded to [`MAX_BOOTSTRAP_NODES`].
    pub bootstrap_nodes: Vec<EndpointAddr>,
    /// Optional, untrusted display hints.
    pub options: DropTicketOptionsV1,
}

/// Optional ticket metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DropTicketOptionsV1 {
    /// Whether the creator suggests enabling automatic fetching.
    pub auto_fetch_recommended: bool,
    /// Untrusted display name for the drop.
    pub display_name: Option<String>,
}

/// A joinable drop ticket. Currently only version 1 exists; the enum wrapper
/// leaves room for a future versioned family.
#[derive(Clone, Debug)]
pub enum DropTicket {
    /// A version-1 ticket.
    V1(DropTicketV1),
}

impl DropTicket {
    /// Create a new version-1 ticket.
    pub fn new(
        topic_id: [u8; 32],
        bootstrap_nodes: Vec<EndpointAddr>,
        options: DropTicketOptionsV1,
    ) -> Self {
        Self::V1(DropTicketV1 {
            version: WIRE_VERSION,
            topic_id,
            bootstrap_nodes,
            options,
        })
    }

    /// The wire major version declared by this ticket.
    pub fn version(&self) -> u16 {
        match self {
            DropTicket::V1(t) => t.version,
        }
    }

    /// The gossip topic ID.
    pub fn topic_id(&self) -> [u8; 32] {
        match self {
            DropTicket::V1(t) => t.topic_id,
        }
    }

    /// Bootstrap peers to dial when joining.
    pub fn bootstrap_nodes(&self) -> &[EndpointAddr] {
        match self {
            DropTicket::V1(t) => &t.bootstrap_nodes,
        }
    }

    /// Ticket options.
    pub fn options(&self) -> &DropTicketOptionsV1 {
        match self {
            DropTicket::V1(t) => &t.options,
        }
    }

    /// Replace the bootstrap set (used to keep shared tickets fresh).
    pub fn set_bootstrap_nodes(&mut self, nodes: Vec<EndpointAddr>) {
        match self {
            DropTicket::V1(t) => t.bootstrap_nodes = nodes,
        }
    }

    /// Encode to the `drop1…` string form.
    pub fn to_string_prefixed(&self) -> String {
        match self {
            DropTicket::V1(t) => {
                let bytes = postcard::to_allocvec(t).expect("ticket serialization is infallible");
                format!(
                    "{TICKET_PREFIX}{}",
                    data_encoding::BASE32_NOPAD
                        .encode(&bytes)
                        .to_ascii_lowercase()
                )
            }
        }
    }

    /// Decode and validate a `drop1…` string.
    ///
    /// All bounds are enforced before or immediately after allocation:
    /// the input length is capped first, postcard decoding is bounded by the
    /// capped input size, and collection limits are checked after decoding.
    pub fn from_string_prefixed(s: &str) -> Result<Self, TicketError> {
        let s = s.trim();
        if s.len() > MAX_TICKET_LEN {
            return Err(TicketError::TooLong(s.len()));
        }
        let encoded = s
            .strip_prefix(TICKET_PREFIX)
            .ok_or(TicketError::BadPrefix)?;
        let bytes = data_encoding::BASE32_NOPAD
            .decode(encoded.to_ascii_uppercase().as_bytes())
            .map_err(|e| TicketError::Malformed(format!("base32: {e}")))?;
        // `take_from_bytes` tolerates trailing bytes so that future additive
        // fields within a major version do not break older decoders.
        let (ticket, _rest): (DropTicketV1, _) = postcard::take_from_bytes(&bytes)
            .map_err(|e| TicketError::Malformed(format!("postcard: {e}")))?;
        if ticket.version != WIRE_VERSION {
            return Err(TicketError::UnsupportedVersion(ticket.version));
        }
        if ticket.bootstrap_nodes.len() > MAX_BOOTSTRAP_NODES {
            return Err(TicketError::TooManyBootstrap(ticket.bootstrap_nodes.len()));
        }
        if let Some(name) = &ticket.options.display_name {
            if name.len() > MAX_DISPLAY_NAME_LEN {
                return Err(TicketError::DisplayNameTooLong);
            }
        }
        Ok(DropTicket::V1(ticket))
    }
}

impl fmt::Display for DropTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_prefixed())
    }
}

impl FromStr for DropTicket {
    type Err = TicketError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_string_prefixed(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{EndpointId, SecretKey};

    fn addr(seed: u8) -> EndpointAddr {
        let key = SecretKey::from_bytes(&[seed; 32]);
        EndpointAddr::from(EndpointId::from(key.public()))
    }

    fn sample_ticket() -> DropTicket {
        DropTicket::new(
            [7u8; 32],
            vec![addr(1), addr(2)],
            DropTicketOptionsV1 {
                auto_fetch_recommended: true,
                display_name: Some("test drop".into()),
            },
        )
    }

    #[test]
    fn roundtrip() {
        let ticket = sample_ticket();
        let encoded = ticket.to_string();
        assert!(encoded.starts_with(TICKET_PREFIX));
        let decoded = DropTicket::from_str(&encoded).unwrap();
        assert_eq!(decoded.version(), WIRE_VERSION);
        assert_eq!(decoded.topic_id(), [7u8; 32]);
        assert_eq!(decoded.bootstrap_nodes().len(), 2);
        assert_eq!(decoded.options().display_name.as_deref(), Some("test drop"));
        assert!(decoded.options().auto_fetch_recommended);
    }

    #[test]
    fn rejects_bad_prefix() {
        let err = DropTicket::from_str("nope1abcdef").unwrap_err();
        assert!(matches!(err, TicketError::BadPrefix));
    }

    #[test]
    fn rejects_too_long() {
        let s = format!("{TICKET_PREFIX}{}", "a".repeat(MAX_TICKET_LEN));
        let err = DropTicket::from_str(&s).unwrap_err();
        assert!(matches!(err, TicketError::TooLong(_)));
    }

    #[test]
    fn rejects_malformed() {
        let err = DropTicket::from_str("drop1zzz***").unwrap_err();
        assert!(matches!(err, TicketError::Malformed(_)));
        let err = DropTicket::from_str("drop1a").unwrap_err();
        assert!(matches!(err, TicketError::Malformed(_)));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut t = DropTicketV1 {
            version: 99,
            topic_id: [0u8; 32],
            bootstrap_nodes: vec![],
            options: Default::default(),
        };
        let bytes = postcard::to_allocvec(&t).unwrap();
        let s = format!(
            "{TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        );
        let err = DropTicket::from_str(&s).unwrap_err();
        assert!(matches!(err, TicketError::UnsupportedVersion(99)));
        t.version = WIRE_VERSION;
    }

    #[test]
    fn rejects_too_many_bootstrap_nodes() {
        let t = DropTicketV1 {
            version: WIRE_VERSION,
            topic_id: [0u8; 32],
            bootstrap_nodes: (0..17).map(addr).collect(),
            options: Default::default(),
        };
        let bytes = postcard::to_allocvec(&t).unwrap();
        let s = format!(
            "{TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        );
        let err = DropTicket::from_str(&s).unwrap_err();
        assert!(matches!(err, TicketError::TooManyBootstrap(17)));
    }

    #[test]
    fn tolerates_trailing_additive_bytes() {
        let ticket = sample_ticket();
        let DropTicket::V1(t) = &ticket;
        let mut bytes = postcard::to_allocvec(t).unwrap();
        bytes.extend_from_slice(&[1, 2, 3, 4]); // future additive fields
        let s = format!(
            "{TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        );
        let decoded = DropTicket::from_str(&s).unwrap();
        assert_eq!(decoded.topic_id(), [7u8; 32]);
    }
}
