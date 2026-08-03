//! # iroh-drop
//!
//! A minimal announced-blob meta-protocol for [Iroh](https://iroh.computer):
//! subscribe to a drop, and when any member publishes a blob, every member
//! learns that it exists and can retrieve it directly through Iroh.
//!
//! `iroh-drop` deliberately does **not** reimplement gossip dissemination,
//! byte transfer, resumable downloads, content verification, NAT traversal,
//! or endpoint identity. It composes two existing protocols on one shared
//! endpoint:
//!
//! * [`iroh-gossip`](https://docs.rs/iroh-gossip) — membership and offers
//! * [`iroh-blobs`](https://docs.rs/iroh-blobs) — storage and verified transfer
//!
//! ```text
//!                        iroh-drop
//!                    policy + coordination
//!                       /            \
//!              iroh-gossip        iroh-blobs
//!           membership + offers  storage + transfer
//!                      \            /
//!                 shared Iroh endpoint
//! ```
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use iroh_drop::{DropBuilder, DropEvent, FetchOutput, StackOptions};
//!
//! # async fn example() -> Result<(), iroh_drop::DropError> {
//! let protocol = DropBuilder::from_options(StackOptions::default())
//!     .await?
//!     .build()
//!     .await?;
//!
//! // Create a drop and share the ticket.
//! let session = protocol.create(Default::default()).await?;
//! println!("ticket: {}", session.ticket());
//!
//! // Publish a file; every member receives an offer.
//! let published = session.publish_path("./slides.pdf").await?;
//!
//! // A peer on another machine would now:
//! //   let session = protocol.join(ticket).await?;
//! //   session.fetch(published.hash, FetchOutput::Directory("./downloads".into())).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Security model in one paragraph
//!
//! The ticket is a bearer capability: anyone possessing it can join, observe,
//! and publish. Messages are signed by their author's endpoint key and
//! verified on receipt; the blob hash is the canonical content identity and
//! all transferred bytes are verified against it by `iroh-blobs`. Filenames,
//! sizes, and media types in offers are untrusted display metadata. Manual
//! fetching is the default so that publishing an offer cannot consume another
//! peer's disk.

pub mod builder;
pub mod error;
pub mod hash;
pub mod limits;
pub mod message;
pub mod policy;
pub mod provider;
pub mod seal;
pub mod session;
pub mod state;
mod sync;
pub mod ticket;
pub mod transport;

pub use builder::{CreateOptions, DropBuilder, DropProtocol, DropStack, StackOptions};
// Re-exported so hosts that manage identity themselves (browsers, mobile
// keychains) can construct `StackOptions::secret_key` without depending on
// iroh directly.
pub use error::{
    DropError, IntegrityError, NetworkError, PolicyError, ProtocolError, ProtocolWarningKind,
    RejectReason, StorageError, TicketError,
};
pub use hash::BlobHash;
pub use seal::{DropKey, KIND_SEALED, SEALED_WIRE_VERSION};
pub use transport::{DropTransport, GossipTransport, TransportEvent};

/// The gossip topic identifier used throughout the public API.
pub use iroh_gossip::proto::TopicId;

/// Wire-format hooks for fuzz targets and external conformance harnesses.
/// Not public API: names, fields, and presence here may change with the
/// wire version. Everything reachable from a peer's bytes must be decoded
/// through these types somewhere — the fuzzers prove none of it panics.
#[doc(hidden)]
pub mod wire {
    pub use crate::sync::{
        ControlRequestV1, ControlResponseV1, HelloV1, SyncRequestV1, SyncResponseV1,
    };
}
pub use iroh::SecretKey;
pub use message::{
    ExtensionV1, MessageBodyV1, MessageV1, OfferV1, ProviderState, ProviderV1, RequestV1,
    VerifiedMessage, KIND_EXTENSION, MAX_MESSAGE_SIZE, WIRE_VERSION,
};
pub use policy::DropPolicy;
pub use session::{
    DropEvent, DropSession, ExtensionFrame, FetchOutput, FetchResult, PublishedBlob,
};
pub use state::{LocalBlobStatus, OfferRecord, ResolveOfferError};
pub use ticket::{
    DropMode, DropTicket, DropTicketOptionsV1, DropTicketV1, TicketAddrV1, TICKET_PREFIX,
};

/// Reserved ALPN for future direct control operations (inventory exchange,
/// provider queries, historical sync). Unused in wire version 1: coordination
/// is carried through gossip.
pub const DROP_ALPN: &[u8] = b"/iroh-drop/1";
