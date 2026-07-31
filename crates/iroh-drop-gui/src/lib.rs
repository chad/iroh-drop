//! The non-painting half of the iroh-drop desktop app.
//!
//! Everything that decides *what* the window shows lives here, so it can be
//! tested without a display: the worker thread, the daemon connection, the
//! consent queue, and the transfer list. `main.rs` is only the painting.

#![deny(missing_docs)]

pub mod bridge;
pub mod qr;
