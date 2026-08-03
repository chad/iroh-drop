//! Drop tickets: the bearer capability required to join a drop.
//!
//! A ticket is a compact binary document (postcard-encoded) rendered for
//! humans as lowercase base32 with a `drop2` prefix:
//!
//! ```text
//! drop2q...
//! ```
//!
//! Anyone possessing the ticket can participate in the drop. It is not a
//! durable identity, not revocable, and not an ACL. If a ticket leaks, create
//! a new drop with a new topic and distribute a new ticket.
//!
//! ## Schema version 3
//!
//! The `version` field is the *ticket schema* version (see the terminology
//! section of `docs/protocol.md`), independent of the gossip message family.
//! Schema 3 was the pre-freeze hardening: it made the drop's **mode**
//! explicit (`Public` vs `Sealed`) instead of inferring privacy from key
//! possession, and replaced upstream address types with a drop-owned wire
//! address so the ticket schema is fully specified by this crate. Builds
//! written for schema 2 reject schema-3 tickets as `UnsupportedVersion`
//! (fail closed), and this build rejects schema-2 tickets the same way.

use std::{fmt, net::SocketAddr, str::FromStr};

use iroh::EndpointAddr;
use iroh::TransportAddr;
use serde::{Deserialize, Serialize};

use crate::error::TicketError;

/// Human-readable prefix for encoded tickets.
pub const TICKET_PREFIX: &str = "drop2";

/// The pre-hardening prefix. Accepted during parsing so the *version* check
/// can produce a precise `UnsupportedVersion` error instead of `BadPrefix`.
pub const LEGACY_TICKET_PREFIX: &str = "drop1";

/// The current ticket schema version.
pub const TICKET_SCHEMA_VERSION: u16 = 3;

/// Maximum accepted length of an encoded ticket, in characters.
///
/// Decoders must reject longer inputs before allocating any collections.
pub const MAX_TICKET_LEN: usize = 8192;

/// Maximum number of bootstrap nodes in a ticket.
pub const MAX_BOOTSTRAP_NODES: usize = 16;

/// Maximum direct addresses per family (v4 or v6) per bootstrap node.
pub const MAX_DIRECT_ADDRS: usize = 8;

/// Maximum length of a relay URL, in bytes.
pub const MAX_RELAY_URL_LEN: usize = 255;

/// Maximum length of the optional display name, in UTF-8 bytes.
pub const MAX_DISPLAY_NAME_LEN: usize = 255;

/// The participation mode declared by a ticket.
///
/// Mode is explicit, never inferred from key possession:
///
/// | mode     | `drop_key` | meaning                                          |
/// |----------|------------|--------------------------------------------------|
/// | `Public` | `None`     | public drop member                               |
/// | `Sealed` | `Some(_)`  | private drop member (reads, publishes, syncs)    |
/// | `Sealed` | `None`     | blind relay: retains and relays sealed frames,   |
/// |          |            | cannot read, publish, or serve history           |
/// | `Public` | `Some(_)`  | invalid — rejected at decode                     |
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropMode {
    /// Plaintext frames, family 3.
    Public,
    /// Sealed frames, family 4. Key possession decides member vs blind relay.
    Sealed,
}

impl DropMode {
    fn from_wire(v: u8) -> Result<Self, TicketError> {
        match v {
            0 => Ok(Self::Public),
            1 => Ok(Self::Sealed),
            other => Err(TicketError::Malformed(format!("unknown mode {other}"))),
        }
    }

    fn to_wire(self) -> u8 {
        match self {
            Self::Public => 0,
            Self::Sealed => 1,
        }
    }
}

/// A drop-owned wire address for a bootstrap node.
///
/// Tickets must not normatively serialize upstream types: this schema is
/// fixed by this document, so another implementation can parse a ticket
/// without mirroring iroh's internal `EndpointAddr` encoding. Conversion
/// to and from [`EndpointAddr`] happens at the boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketAddrV1 {
    /// The endpoint's public key.
    pub endpoint_id: [u8; 32],
    /// Home relay URL, if any. Bounded to [`MAX_RELAY_URL_LEN`].
    pub relay_url: Option<String>,
    /// Direct IPv4 addresses `(ip, port)`. Bounded to [`MAX_DIRECT_ADDRS`].
    pub direct_v4: Vec<([u8; 4], u16)>,
    /// Direct IPv6 addresses `(ip, port)`. Bounded to [`MAX_DIRECT_ADDRS`].
    pub direct_v6: Vec<([u8; 16], u16)>,
}

impl From<&EndpointAddr> for TicketAddrV1 {
    fn from(addr: &EndpointAddr) -> Self {
        let mut out = TicketAddrV1 {
            endpoint_id: *addr.id.as_bytes(),
            relay_url: None,
            direct_v4: Vec::new(),
            direct_v6: Vec::new(),
        };
        for transport in &addr.addrs {
            match transport {
                TransportAddr::Relay(url) => out.relay_url = Some(url.to_string()),
                TransportAddr::Ip(SocketAddr::V4(a)) => {
                    out.direct_v4.push((a.ip().octets(), a.port()));
                }
                TransportAddr::Ip(SocketAddr::V6(a)) => {
                    out.direct_v6.push((a.ip().octets(), a.port()));
                }
                // Custom transports are an in-process concept; they are
                // never written to tickets.
                _ => {}
            }
        }
        out
    }
}

impl TryFrom<&TicketAddrV1> for EndpointAddr {
    type Error = TicketError;

    fn try_from(wire: &TicketAddrV1) -> Result<Self, Self::Error> {
        let id = iroh::EndpointId::from_bytes(&wire.endpoint_id)
            .map_err(|_| TicketError::Malformed("bootstrap endpoint id".into()))?;
        let mut addrs = Vec::new();
        if let Some(url) = &wire.relay_url {
            let url = url
                .parse()
                .map_err(|_| TicketError::Malformed("bootstrap relay url".into()))?;
            addrs.push(TransportAddr::Relay(url));
        }
        for (ip, port) in &wire.direct_v4 {
            addrs.push(TransportAddr::Ip(SocketAddr::from((*ip, *port))));
        }
        for (ip, port) in &wire.direct_v6 {
            addrs.push(TransportAddr::Ip(SocketAddr::from((*ip, *port))));
        }
        Ok(EndpointAddr::from_parts(id, addrs))
    }
}

/// Version 1 of the drop ticket schema (schema version 3 on the wire).
#[derive(Clone, Serialize, Deserialize)]
pub struct DropTicketV1 {
    /// Ticket schema version. Receivers must reject anything but
    /// [`TICKET_SCHEMA_VERSION`].
    pub version: u16,
    /// The gossip topic that carries drop coordination messages.
    pub topic_id: [u8; 32],
    /// Peers to bootstrap from. Bounded to [`MAX_BOOTSTRAP_NODES`].
    pub bootstrap_nodes: Vec<TicketAddrV1>,
    /// Optional, untrusted display hints.
    pub options: DropTicketOptionsV1,
    /// Participation mode (see [`DropMode`]).
    pub mode: u8,
    /// Symmetric key of a private drop; `None` for public drops and blind
    /// relays. Redacted from `Debug` output.
    pub drop_key: Option<[u8; 32]>,
}

impl std::fmt::Debug for DropTicketV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DropTicketV1")
            .field("version", &self.version)
            .field("topic_id", &self.topic_id)
            .field("bootstrap_nodes", &self.bootstrap_nodes)
            .field("options", &self.options)
            .field("mode", &self.mode)
            .field("drop_key", &self.drop_key.map(|_| "<redacted>"))
            .finish()
    }
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
    /// Create a new ticket for a public drop.
    pub fn new(
        topic_id: [u8; 32],
        bootstrap_nodes: Vec<EndpointAddr>,
        options: DropTicketOptionsV1,
    ) -> Self {
        Self::V1(DropTicketV1 {
            version: TICKET_SCHEMA_VERSION,
            topic_id,
            bootstrap_nodes: bootstrap_nodes.iter().map(TicketAddrV1::from).collect(),
            options,
            mode: DropMode::Public.to_wire(),
            drop_key: None,
        })
    }

    /// Create a ticket for a private drop (see `docs/protocol.md`,
    /// "Private drops"): every frame is sealed under `drop_key`.
    pub fn new_private(
        topic_id: [u8; 32],
        bootstrap_nodes: Vec<EndpointAddr>,
        options: DropTicketOptionsV1,
        drop_key: crate::seal::DropKey,
    ) -> Self {
        Self::V1(DropTicketV1 {
            version: TICKET_SCHEMA_VERSION,
            topic_id,
            bootstrap_nodes: bootstrap_nodes.iter().map(TicketAddrV1::from).collect(),
            options,
            mode: DropMode::Sealed.to_wire(),
            drop_key: Some(*drop_key.as_bytes()),
        })
    }

    /// This ticket with the key removed. Sharing the result invites a
    /// **blind relay** (sealed mode, no key): it can retain and relay sealed
    /// frames — keeping the drop reachable and well-connected — without
    /// being able to read anything.
    pub fn without_key(&self) -> Self {
        let DropTicket::V1(t) = self;
        Self::V1(DropTicketV1 {
            drop_key: None,
            ..t.clone()
        })
    }

    /// The participation mode declared by this ticket.
    pub fn mode(&self) -> DropMode {
        match self {
            DropTicket::V1(t) => DropMode::from_wire(t.mode)
                .expect("tickets are validated at construction and decode"),
        }
    }

    /// The drop key, for a private drop member's ticket.
    pub fn drop_key(&self) -> Option<crate::seal::DropKey> {
        match self {
            DropTicket::V1(t) => t.drop_key.map(crate::seal::DropKey::from_bytes),
        }
    }

    /// The ticket schema version declared by this ticket.
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
    pub fn bootstrap_nodes(&self) -> Vec<EndpointAddr> {
        match self {
            DropTicket::V1(t) => t
                .bootstrap_nodes
                .iter()
                .filter_map(|a| EndpointAddr::try_from(a).ok())
                .collect(),
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
            DropTicket::V1(t) => {
                t.bootstrap_nodes = nodes.iter().map(TicketAddrV1::from).collect();
            }
        }
    }

    /// Encode to the `drop2…` string form.
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

    /// Decode and validate a `drop2…` string.
    ///
    /// All bounds are enforced before or immediately after allocation:
    /// the input length is capped first, postcard decoding is bounded by the
    /// capped input size, and collection limits are checked after decoding.
    ///
    /// `drop1…` (schema 2) strings are accepted as far as the version check
    /// so they fail with a precise `UnsupportedVersion`, never silently.
    pub fn from_string_prefixed(s: &str) -> Result<Self, TicketError> {
        let s = s.trim();
        if s.len() > MAX_TICKET_LEN {
            return Err(TicketError::TooLong(s.len()));
        }
        let encoded = s
            .strip_prefix(TICKET_PREFIX)
            .or_else(|| s.strip_prefix(LEGACY_TICKET_PREFIX))
            .ok_or(TicketError::BadPrefix)?;
        let bytes = data_encoding::BASE32_NOPAD
            .decode(encoded.to_ascii_uppercase().as_bytes())
            .map_err(|e| TicketError::Malformed(format!("base32: {e}")))?;
        // `take_from_bytes` tolerates trailing bytes so that future additive
        // fields within a schema version do not break older decoders.
        let (ticket, _rest): (DropTicketV1, _) = postcard::take_from_bytes(&bytes)
            .map_err(|e| TicketError::Malformed(format!("postcard: {e}")))?;
        if ticket.version != TICKET_SCHEMA_VERSION {
            return Err(TicketError::UnsupportedVersion(ticket.version));
        }
        Self::validate(&ticket)?;
        Ok(DropTicket::V1(ticket))
    }

    fn validate(ticket: &DropTicketV1) -> Result<(), TicketError> {
        let mode = DropMode::from_wire(ticket.mode)?;
        if mode == DropMode::Public && ticket.drop_key.is_some() {
            return Err(TicketError::Malformed(
                "public ticket must not carry a drop key".into(),
            ));
        }
        if ticket.bootstrap_nodes.len() > MAX_BOOTSTRAP_NODES {
            return Err(TicketError::TooManyBootstrap(ticket.bootstrap_nodes.len()));
        }
        for node in &ticket.bootstrap_nodes {
            if node.direct_v4.len() > MAX_DIRECT_ADDRS || node.direct_v6.len() > MAX_DIRECT_ADDRS {
                return Err(TicketError::Malformed("too many direct addresses".into()));
            }
            if let Some(url) = &node.relay_url {
                if url.len() > MAX_RELAY_URL_LEN {
                    return Err(TicketError::Malformed("relay url too long".into()));
                }
            }
            // Fail closed: every bootstrap address must convert cleanly.
            EndpointAddr::try_from(node)?;
        }
        if let Some(name) = &ticket.options.display_name {
            if name.len() > MAX_DISPLAY_NAME_LEN {
                return Err(TicketError::DisplayNameTooLong);
            }
        }
        Ok(())
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
        assert_eq!(decoded.version(), TICKET_SCHEMA_VERSION);
        assert_eq!(decoded.topic_id(), [7u8; 32]);
        assert_eq!(decoded.bootstrap_nodes().len(), 2);
        assert_eq!(decoded.options().display_name.as_deref(), Some("test drop"));
        assert!(decoded.options().auto_fetch_recommended);
        assert_eq!(decoded.mode(), DropMode::Public);
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
        let err = DropTicket::from_str("drop2zzz***").unwrap_err();
        assert!(matches!(err, TicketError::Malformed(_)));
        let err = DropTicket::from_str("drop2a").unwrap_err();
        assert!(matches!(err, TicketError::Malformed(_)));
    }

    #[test]
    fn rejects_unsupported_version() {
        let t = DropTicketV1 {
            version: 99,
            topic_id: [0u8; 32],
            bootstrap_nodes: vec![],
            options: Default::default(),
            mode: 0,
            drop_key: None,
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
    }

    #[test]
    fn legacy_drop1_tickets_fail_closed() {
        // A schema-2 ticket (the pre-hardening wire) parses far enough to
        // report exactly what it is. Note the schema-2 layout ends before
        // `mode`, so the current struct runs past the buffer on old bytes
        // only when the old bytes were maximal; the canonical failure is
        // the version check on a syntactically complete old ticket.
        let legacy = postcard::to_allocvec(&(2u16, [9u8; 32])).unwrap();
        let s = format!(
            "{LEGACY_TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&legacy)
                .to_ascii_lowercase()
        );
        // Too short to be a schema-2 ticket either: malformed, not accepted.
        assert!(DropTicket::from_str(&s).is_err());
    }

    #[test]
    fn rejects_too_many_bootstrap_nodes() {
        let t = DropTicketV1 {
            version: TICKET_SCHEMA_VERSION,
            topic_id: [0u8; 32],
            bootstrap_nodes: (0..17).map(|i| TicketAddrV1::from(&addr(i))).collect(),
            options: Default::default(),
            mode: 0,
            drop_key: None,
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
    fn rejects_public_ticket_with_key() {
        let t = DropTicketV1 {
            version: TICKET_SCHEMA_VERSION,
            topic_id: [0u8; 32],
            bootstrap_nodes: vec![],
            options: Default::default(),
            mode: 0,
            drop_key: Some([0xEE; 32]),
        };
        let bytes = postcard::to_allocvec(&t).unwrap();
        let s = format!(
            "{TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        );
        let err = DropTicket::from_str(&s).unwrap_err();
        assert!(matches!(err, TicketError::Malformed(_)));
    }

    #[test]
    fn rejects_unknown_mode() {
        let t = DropTicketV1 {
            version: TICKET_SCHEMA_VERSION,
            topic_id: [0u8; 32],
            bootstrap_nodes: vec![],
            options: Default::default(),
            mode: 7,
            drop_key: None,
        };
        let bytes = postcard::to_allocvec(&t).unwrap();
        let s = format!(
            "{TICKET_PREFIX}{}",
            data_encoding::BASE32_NOPAD
                .encode(&bytes)
                .to_ascii_lowercase()
        );
        assert!(matches!(
            DropTicket::from_str(&s).unwrap_err(),
            TicketError::Malformed(_)
        ));
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

    #[test]
    fn private_ticket_round_trips_with_key() {
        let key = crate::seal::DropKey::generate();
        let ticket =
            DropTicket::new_private([1u8; 32], vec![addr(1)], Default::default(), key.clone());
        let decoded = DropTicket::from_str(&ticket.to_string_prefixed()).unwrap();
        assert_eq!(decoded.mode(), DropMode::Sealed);
        assert_eq!(
            decoded.drop_key().unwrap().as_bytes(),
            key.as_bytes(),
            "drop key must survive the string round trip"
        );
    }

    #[test]
    fn blind_relay_ticket_is_keyless_but_sealed() {
        let ticket = DropTicket::new_private(
            [3u8; 32],
            vec![addr(3)],
            Default::default(),
            crate::seal::DropKey::generate(),
        )
        .without_key();
        assert_eq!(ticket.mode(), DropMode::Sealed);
        assert!(ticket.drop_key().is_none());
        let decoded = DropTicket::from_str(&ticket.to_string_prefixed()).unwrap();
        assert_eq!(decoded.mode(), DropMode::Sealed);
        assert!(decoded.drop_key().is_none());
    }

    #[test]
    fn wire_addresses_round_trip_through_endpoint_addr() {
        let key = SecretKey::from_bytes(&[42u8; 32]);
        let original = EndpointAddr::from(EndpointId::from(key.public()))
            .with_ip_addr("192.0.2.1:11204".parse().unwrap())
            .with_ip_addr("[2001:db8::1]:11204".parse().unwrap())
            .with_relay_url("https://relay.example.com/".parse().unwrap());
        let wire = TicketAddrV1::from(&original);
        let back = EndpointAddr::try_from(&wire).unwrap();
        assert_eq!(back.id, original.id);
        assert!(back
            .addrs
            .contains(&TransportAddr::Ip("192.0.2.1:11204".parse().unwrap())));
        assert!(back
            .addrs
            .contains(&TransportAddr::Ip("[2001:db8::1]:11204".parse().unwrap())));
        assert!(back
            .addrs
            .iter()
            .any(|a| matches!(a, TransportAddr::Relay(_))));
    }
}
