//! Structured error model for `iroh-drop`.
//!
//! Categories follow the specification: ticket errors fail a join immediately,
//! protocol errors drop a single message but keep the session alive, policy
//! errors record an offer without fetching it, network errors are retryable,
//! integrity errors permanently fail a transfer, and lifecycle errors shut the
//! session down cleanly.

use serde::{Deserialize, Serialize};

use crate::ticket::{MAX_BOOTSTRAP_NODES, MAX_TICKET_LEN};

/// Top-level error type for all `iroh-drop` operations.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum DropError {
    #[error("ticket error: {0}")]
    Ticket(#[from] TicketError),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("policy rejected operation: {0}")]
    Policy(#[from] PolicyError),

    #[error("network error: {0}")]
    Network(#[from] NetworkError),

    #[error("content integrity error: {0}")]
    Integrity(#[from] IntegrityError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("protocol has shut down")]
    Shutdown,
}

/// Errors decoding or validating a [`crate::DropTicket`].
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
pub enum TicketError {
    #[error("unsupported ticket version {0}")]
    UnsupportedVersion(u16),

    #[error("malformed ticket encoding: {0}")]
    Malformed(String),

    #[error("ticket does not start with the `drop1` prefix")]
    BadPrefix,

    #[error("ticket too long ({0} chars, max {MAX_TICKET_LEN})")]
    TooLong(usize),

    #[error("too many bootstrap nodes ({0}, max {MAX_BOOTSTRAP_NODES})")]
    TooManyBootstrap(usize),

    #[error("display name too long (max 255 UTF-8 bytes)")]
    DisplayNameTooLong,
}

/// Errors produced while decoding or validating a wire message.
///
/// These never terminate a session: the offending message is dropped and a
/// [`crate::DropEvent::ProtocolWarning`] is emitted instead.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ProtocolError {
    #[error("unsupported wire version {0}")]
    UnsupportedVersion(u16),

    #[error("message exceeds maximum size ({0} bytes)")]
    MessageTooLarge(usize),

    #[error("malformed message: {0}")]
    Malformed(String),

    #[error("author is over its offer quota")]
    QuotaExceeded,

    #[error("peer is sending too fast")]
    RateLimited,

    #[error("invalid message signature")]
    InvalidSignature,

    #[error("invalid author public key")]
    InvalidAuthor,

    #[error("invalid name: {0}")]
    InvalidName(String),

    #[error("metadata limit exceeded: {0}")]
    MetadataLimit(String),
}

/// A fetch or publish rejected by the local [`crate::DropPolicy`].
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
pub enum PolicyError {
    #[error("blob size {size} exceeds maximum {max} bytes")]
    TooLarge { size: u64, max: u64 },

    #[error("media type {0:?} is not accepted by policy")]
    MediaTypeRejected(String),

    #[error("auto-fetch byte quota exceeded ({spent} + {size} > {max})")]
    TotalQuotaExceeded { spent: u64, size: u64, max: u64 },

    #[error("too many concurrent fetches ({active}, max {max})")]
    ConcurrencyLimit { active: usize, max: usize },
}

/// Network and transfer failures. These are retryable.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum NetworkError {
    #[error("endpoint error: {0}")]
    Endpoint(String),

    #[error("gossip error: {0}")]
    Gossip(String),

    #[error("no providers known for blob {0}")]
    NoProviders(String),

    #[error("transfer failed: {0}")]
    Transfer(String),

    #[error("timed out: {0}")]
    Timeout(String),
}

/// Content failed verification. Never announce availability after one of these.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
pub enum IntegrityError {
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("size mismatch: advertised {advertised}, actual {actual}")]
    SizeMismatch { advertised: u64, actual: u64 },
}

/// Local storage failures.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum StorageError {
    #[error("import failed: {0}")]
    Import(String),

    #[error("export failed: {0}")]
    Export(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("io error: {0}")]
    Io(String),
}

/// Why an incoming offer was rejected.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RejectReason {
    #[error("malformed message: {0}")]
    Malformed(String),

    #[error("author is over its offer quota")]
    QuotaExceeded,

    #[error("peer is sending too fast")]
    RateLimited,

    #[error("unsupported wire version {0}")]
    UnsupportedVersion(u16),

    #[error("invalid signature")]
    InvalidSignature,

    #[error("invalid name: {0}")]
    InvalidName(String),

    #[error("policy: {0}")]
    Policy(String),
}

/// Non-fatal protocol anomalies, surfaced through
/// [`crate::DropEvent::ProtocolWarning`].
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ProtocolWarningKind {
    #[error("ignoring message with unsupported wire version {version}")]
    UnsupportedVersion { version: u16 },

    #[error("ignoring malformed message: {reason}")]
    Malformed { reason: String },

    #[error("ignoring message with invalid signature")]
    InvalidSignature,

    #[error("ignoring oversized message ({size} bytes)")]
    Oversized { size: usize },

    #[error("gossip receiver lagged, messages were dropped")]
    Lagged,

    #[error("ignoring message kind {kind}, which this build does not implement")]
    UnknownKind { kind: u16 },

    #[error("rate limiting a peer that is sending too fast")]
    RateLimited,

    #[error("forgot {count} offer(s) to stay within the inventory limit")]
    InventoryEvicted { count: usize },

    #[error("provider {provider} failed for blob {hash}: {reason}")]
    ProviderFailed {
        provider: String,
        hash: String,
        reason: String,
    },
}
