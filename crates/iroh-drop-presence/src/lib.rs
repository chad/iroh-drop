//! Presence beacons: the canonical `iroh-drop` extension.
//!
//! This crate exists to prove a point — see `docs/extending.md` in the
//! repository root. An extension protocol (a new gossip message kind) can:
//!
//! * be built by a third party using only `iroh-drop`'s public API
//!   (this crate's entire dependency surface),
//! * propagate through peers that do not implement it (they verify, relay,
//!   and retain unknown kinds), and
//! * be pulled by late joiners from those peers' retained history.
//!
//! The protocol itself is deliberately trivial: members broadcast a short
//! UTF-8 status in the [`PRESENCE_NAMESPACE`]. There is no handshake, no
//! state, and no guarantee — presence is a hint, and the payload is
//! untrusted text.
//!
//! ```rust,no_run
//! # async fn demo(session: iroh_drop::DropSession) -> Result<(), iroh_drop::DropError> {
//! // Tell the drop we are here.
//! iroh_drop_presence::announce(&session, "online").await?;
//!
//! // Watch who else is here.
//! let mut rx = iroh_drop_presence::subscribe(&session);
//! while let Ok(frame) = rx.recv().await {
//!     if let Some(presence) = iroh_drop_presence::decode(&frame) {
//!         println!("{} says: {}", presence.author.fmt_short(), presence.status);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use bytes::Bytes;
use iroh::EndpointId;
use iroh_drop::{DropError, DropSession, ExtensionFrame, ProtocolError};
use tokio::sync::broadcast;

/// The namespace presence beacons ride in: the first 16 bytes of
/// SHA-256("iroh-drop-presence/v1"), truncated deterministically and fixed
/// forever. Extensions never register numbers with the core protocol; a
/// namespace derived from a name you own cannot collide with anyone else's.
pub const PRESENCE_NAMESPACE: [u8; 16] = [
    0xab, 0xab, 0xf4, 0xba, 0x7b, 0x13, 0x6d, 0xa5, 0xb4, 0xdb, 0xc7, 0x71, 0x08, 0x27, 0x3f, 0x76,
];

/// Presence's own message number for a status beacon. Local to the
/// namespace — the core protocol never sees it.
pub const PRESENCE_KIND_STATUS: u32 = 1;

/// Presence payload schema version.
pub const PRESENCE_SCHEMA_VERSION: u16 = 1;

/// Maximum status length in UTF-8 bytes. Presence is a one-liner, not a
/// document; the cap keeps beacons cheap to relay.
pub const MAX_STATUS_LEN: usize = 140;

/// A decoded presence beacon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Presence {
    /// The cryptographically verified author of the beacon.
    pub author: EndpointId,
    /// What they say they're up to. Untrusted display text.
    pub status: String,
}

/// Broadcast a status to every member of the drop (present and future:
/// peers that joined later can replay the beacon from retained history).
///
/// The frame is signed by the session's endpoint key, so `author` on the
/// receiving side is attributable — but anyone in the drop can say
/// anything, so display the status, never execute it.
pub async fn announce(session: &DropSession, status: &str) -> Result<(), DropError> {
    if status.is_empty() || status.len() > MAX_STATUS_LEN {
        return Err(ProtocolError::Malformed(format!(
            "presence status must be 1..={MAX_STATUS_LEN} bytes, got {}",
            status.len()
        ))
        .into());
    }
    session
        .send_extension(
            PRESENCE_NAMESPACE,
            PRESENCE_KIND_STATUS,
            PRESENCE_SCHEMA_VERSION,
            Bytes::copy_from_slice(status.as_bytes()),
        )
        .await
}

/// Subscribe to presence frames: everything already retained (including
/// what catch-up sync pulled before we joined), then everything live.
///
/// The raw receiver is returned so the caller owns its event loop; decode
/// each frame with [`decode`] and skip what does not parse. Lagging
/// receivers lose frames — presence is lossy by nature, so that is fine.
pub fn subscribe(session: &DropSession) -> broadcast::Receiver<ExtensionFrame> {
    session.on_extension(PRESENCE_NAMESPACE)
}

/// Interpret a frame as a presence beacon. `None` for other protocols,
/// unknown local kinds, invalid UTF-8, or over-long statuses — all normal
/// outcomes with untrusted input.
pub fn decode(frame: &ExtensionFrame) -> Option<Presence> {
    if frame.namespace != PRESENCE_NAMESPACE || frame.local_kind != PRESENCE_KIND_STATUS {
        return None;
    }
    let status = std::str::from_utf8(&frame.payload).ok()?;
    if status.is_empty() || status.len() > MAX_STATUS_LEN {
        return None;
    }
    Some(Presence {
        author: frame.author,
        status: status.to_string(),
    })
}
