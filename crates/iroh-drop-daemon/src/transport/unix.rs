//! Unix domain socket listener.
//!
//! The socket lives in a `0700` directory and is itself `0600`. The directory
//! mode is what actually enforces access: historically some platforms ignored
//! permission bits on socket files, but every one of them enforces them on a
//! directory you must traverse to reach the socket.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, warn};

use crate::client::{AskHandler, Client};
use crate::frame::Hello;
use crate::service::{ApiError, Service};

use super::{connect_io, serve_io};

/// A bound listener, accepting clients until dropped.
#[derive(Debug)]
pub struct ControlListener {
    listener: UnixListener,
    path: PathBuf,
    service: Arc<Service>,
}

impl ControlListener {
    /// Bind the control socket, replacing a stale one if nobody is home.
    pub async fn bind(service: Arc<Service>, path: &Path) -> Result<Self, ApiError> {
        let dir = path
            .parent()
            .ok_or_else(|| ApiError::new("bad_path", "socket path has no parent"))?;
        std::fs::create_dir_all(dir).map_err(|e| ApiError::new("io", e))?;
        restrict(dir, 0o700)?;

        if path.exists() {
            // Either a live daemon or a leftover from a crash. Asking is the
            // only way to tell.
            match UnixStream::connect(path).await {
                Ok(_) => {
                    return Err(ApiError::new(
                        "already_running",
                        format!("a daemon is already listening on {}", path.display()),
                    ))
                }
                Err(_) => {
                    debug!("removing stale socket {}", path.display());
                    std::fs::remove_file(path).map_err(|e| ApiError::new("io", e))?;
                }
            }
        }

        let listener = UnixListener::bind(path).map_err(|e| ApiError::new("io", e))?;
        restrict(path, 0o600)?;
        Ok(Self {
            listener,
            path: path.to_path_buf(),
            service,
        })
    }

    /// The bound path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Accept clients forever.
    pub async fn serve(self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _addr)) => {
                    let (reader, writer) = stream.into_split();
                    serve_io(Arc::clone(&self.service), reader, writer);
                }
                Err(e) => {
                    warn!("accept failed: {e}");
                    // A single bad accept must not kill the daemon.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

impl Drop for ControlListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Connect to a running daemon.
pub async fn connect(
    path: &Path,
    hello: Hello,
    on_ask: Option<AskHandler>,
) -> Result<Client, ApiError> {
    let stream = UnixStream::connect(path).await.map_err(|e| {
        ApiError::new(
            "no_daemon",
            format!("cannot reach a daemon at {}: {e}", path.display()),
        )
    })?;
    let (reader, writer) = stream.into_split();
    connect_io(reader, writer, hello, on_ask).await
}

fn restrict(path: &Path, mode: u32) -> Result<(), ApiError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .map_err(|e| ApiError::new("io", format!("cannot restrict {}: {e}", path.display())))
}
