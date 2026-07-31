//! Content-addressed identity for blobs.
//!
//! The blob hash is the canonical identity of a piece of content in a drop.
//! `BlobHash` is a small newtype around the 32 raw bytes of a BLAKE3 hash so
//! the wire schema and the public API do not depend on the `iroh-blobs` crate
//! version. Conversions to and from [`iroh_blobs::Hash`] are provided at the
//! boundary where bytes are actually transferred.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The 32-byte BLAKE3 content hash that canonically identifies a blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    /// Create from raw bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw bytes of the hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Hex string of the full hash.
    pub fn to_hex(&self) -> String {
        data_encoding::HEXLOWER.encode(&self.0)
    }

    /// Hex string of the first 4 bytes, for compact display.
    pub fn fmt_short(&self) -> String {
        data_encoding::HEXLOWER.encode(&self.0[..4])
    }

    /// Parse a full 64-character hex string. For user input, prefer
    /// [`BlobHash::matches_prefix`] for prefix matching.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars, got {}", s.len()));
        }
        let bytes = data_encoding::HEXLOWER
            .decode(s.as_bytes())
            .map_err(|e| e.to_string())?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "expected 32 bytes".to_string())?;
        Ok(Self(bytes))
    }

    /// Whether the hex encoding of this hash starts with the given prefix.
    pub fn matches_prefix(&self, prefix: &str) -> bool {
        let prefix = prefix.trim().to_ascii_lowercase();
        !prefix.is_empty() && self.to_hex().starts_with(&prefix)
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobHash({}…)", self.fmt_short())
    }
}

impl FromStr for BlobHash {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl From<iroh_blobs::Hash> for BlobHash {
    fn from(hash: iroh_blobs::Hash) -> Self {
        Self(*hash.as_bytes())
    }
}

impl From<BlobHash> for iroh_blobs::Hash {
    fn from(hash: BlobHash) -> Self {
        iroh_blobs::Hash::from_bytes(hash.0)
    }
}

impl AsRef<[u8]> for BlobHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
