//! Human-friendly conventions on top of the [`iroh_drop`] protocol.
//!
//! The protocol crate speaks hashes, signed frames, and policy. It does not
//! know what a "directory" is, what number `3` in a listing means, or where
//! a user's downloads live — on purpose. This crate adds exactly that, using
//! nothing but the protocol's public API:
//!
//! - [`collections`]: publish and materialize directory trees through a
//!   manifest blob (a *convention*, never enforced on the wire).
//! - [`inventory`]: stable numbered listings and "pick" resolution, so users
//!   can say `3`, `report.pdf`, or a hash prefix instead of 64 hex digits.
//! - [`config`]: on-disk defaults (store, identity, download directory) so
//!   apps behave consistently across restarts.
//! - [`rooms`]: saved drops under names, so a ticket is typed once ever.
//! - [`nearby`]: browsing (and advertising) drops on the local network.
//!
//! Anything here could be written by a third party; nothing here changes
//! bytes on the wire. See `docs/roadmap.md` for the layering rule.

#![deny(missing_docs)]

pub mod collections;
pub mod config;
pub mod inventory;
#[cfg(feature = "mdns")]
pub mod nearby;
pub mod rooms;

pub use collections::{fetch_any, publish_path, Manifest, ManifestEntry, COLLECTION_MEDIA_TYPE};
pub use config::Config;
pub use inventory::{inventory, resolve_pick, InventoryItem};
#[cfg(feature = "mdns")]
pub use nearby::{browse, NearbyDrop};
pub use rooms::{Room, Rooms};

/// Errors from the SDK layer.
#[derive(thiserror::Error, Debug)]
pub enum SdkError {
    /// The underlying protocol failed.
    #[error(transparent)]
    Drop(#[from] iroh_drop::DropError),

    /// Filesystem trouble.
    #[error("io error: {0}")]
    Io(String),

    /// A manifest could not be parsed or was rejected.
    #[error("invalid collection manifest: {0}")]
    Manifest(String),

    /// A user-supplied selection matched nothing (or was ambiguous).
    #[error("{0}")]
    Pick(String),

    /// Config file trouble.
    #[error("config error: {0}")]
    Config(String),
}

impl From<std::io::Error> for SdkError {
    fn from(e: std::io::Error) -> Self {
        SdkError::Io(e.to_string())
    }
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, SdkError>;
