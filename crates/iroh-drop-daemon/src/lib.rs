//! The daemon and its control API.
//!
//! Today the process *is* the peer: `share` runs in the foreground and the drop
//! dies with the terminal. That is honest for a protocol and useless for an
//! app — a peer that exists only while a terminal is open can never be a
//! replica for anybody, so the announce-fetch-replicate design never actually
//! engages.
//!
//! This crate hosts sessions in a long-lived process and exposes one API that
//! every client speaks: CLI, GUI, TUI, MCP server. See `docs/daemon-api.md`.
//!
//! ## Decentralization constraints
//!
//! - No account, no cloud, no registry, no telemetry. Identity is a local
//!   keypair.
//! - The only server is on your own machine, reachable only by you.
//! - The daemon is *optional*: [`Client::connect_memory`] runs the whole thing
//!   in the calling process, so "no background service" stays supported.
//! - LAN-only ([`ServiceOptions::offline`] + `mdns`) is a first-class posture:
//!   no relay, no DNS, no pkarr, no third party.
//!
//! ## Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use iroh_drop_daemon::{Client, Hello, Service, ServiceOptions};
//! use serde_json::json;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let service = Service::new(ServiceOptions {
//!     offline: true, mdns: true, ..Default::default()
//! }).await?;
//! let client = Client::connect_memory(&service, Hello::ui("my-gui/0.1"), None).await?;
//!
//! let drop = client.call("drop.create", json!({"name": "slides"})).await?;
//! client.call("offer.publish", json!({
//!     "drop": drop["drop"], "path": "./slides.pdf"
//! })).await?;
//! println!("ticket: {}", drop["ticket"]);
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

pub mod client;
pub mod frame;
pub mod persist;
pub mod service;
pub mod transport;

pub use client::{AskHandler, AskRequest, Client};
pub use frame::{Envelope, Frame, Hello, Role, API_VERSION, MAX_FRAME_BYTES};
pub use service::{ApiError, Service, ServiceOptions, CONSENT_TIMEOUT, EVENT_RING};
pub use transport::{connect, default_socket_path, ControlListener};
