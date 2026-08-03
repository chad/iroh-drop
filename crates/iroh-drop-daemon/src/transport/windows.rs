//! Windows named pipe listener.
//!
//! ## What the DACL does and does not do
//!
//! A named pipe created without explicit security attributes gets a default
//! DACL granting the creating user and `SYSTEM`/administrators. That is the
//! right shape — another *user* on the machine cannot connect — but note two
//! things honestly:
//!
//! - Local administrators can always reach it. So can anything running as you,
//!   which is the same situation as the Unix socket: this protects you from other
//!   users, not from your own compromised processes.
//! - We pass `reject_remote_clients(true)`, because named pipes are otherwise
//!   reachable over SMB. Without it, "local socket" would be a lie on a domain
//!   network.
//!
//! Unlike a Unix socket there is no filesystem entry to leave behind, so the
//! stale-path dance does not exist here. Instead, `first_pipe_instance(true)`
//! makes a second daemon fail to create the pipe at all, which is exactly the
//! "already running" answer we want.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::windows::named_pipe::{ClientOptions, ServerOptions};
use tracing::warn;

use crate::client::{AskHandler, Client};
use crate::frame::Hello;
use crate::service::{ApiError, Service};

use super::{connect_io, serve_io};

/// The default pipe name.
pub const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\iroh-drop\control";

/// A bound named pipe, accepting clients until dropped.
#[derive(Debug)]
pub struct ControlListener {
    name: PathBuf,
    service: Arc<Service>,
    /// The first server instance, created eagerly so `bind` fails fast when a
    /// daemon is already running.
    first: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
}

impl ControlListener {
    /// Create the pipe. Fails with `already_running` if one already exists.
    pub async fn bind(service: Arc<Service>, path: &Path) -> Result<Self, ApiError> {
        let name = path.to_string_lossy().to_string();
        let first = ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create(&name)
            .map_err(|e| {
                // ERROR_ACCESS_DENIED is what `first_pipe_instance` returns when
                // the pipe exists, i.e. a daemon is already listening.
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    ApiError::new(
                        "already_running",
                        format!("a daemon is already listening on {name}"),
                    )
                } else {
                    ApiError::new("io", format!("cannot create {name}: {e}"))
                }
            })?;
        Ok(Self {
            name: PathBuf::from(name),
            service,
            first: Some(first),
        })
    }

    /// The pipe name.
    pub fn path(&self) -> &Path {
        &self.name
    }

    /// Accept clients forever.
    ///
    /// The named pipe pattern is: wait for a client on the current instance,
    /// hand it off, then create the next instance. The next instance must exist
    /// before the handed-off one is used, or a client connecting in the gap gets
    /// `FILE_NOT_FOUND`.
    pub async fn serve(mut self) {
        let name = self.name.to_string_lossy().to_string();
        let mut server = match self.first.take() {
            Some(server) => server,
            None => return,
        };
        loop {
            if let Err(e) = server.connect().await {
                warn!("pipe connect failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            // Create the successor before serving the connected instance.
            let next = match ServerOptions::new()
                .reject_remote_clients(true)
                .create(&name)
            {
                Ok(next) => next,
                Err(e) => {
                    warn!("cannot create the next pipe instance: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            let connected = std::mem::replace(&mut server, next);
            let (reader, writer) = tokio::io::split(connected);
            serve_io(Arc::clone(&self.service), reader, writer);
        }
    }
}

/// Connect to a running daemon.
pub async fn connect(
    path: &Path,
    hello: Hello,
    on_ask: Option<AskHandler>,
) -> Result<Client, ApiError> {
    let name = path.to_string_lossy().to_string();
    let client = ClientOptions::new()
        .open(&name)
        .map_err(|e| ApiError::new("no_daemon", format!("cannot reach a daemon at {name}: {e}")))?;
    let (reader, writer) = tokio::io::split(client);
    connect_io(reader, writer, hello, on_ask).await
}
