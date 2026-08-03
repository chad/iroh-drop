//! JSONL framing over a local, user-private channel.
//!
//! Deliberately **not** a localhost TCP port. A TCP port is reachable by every
//! process on the machine and, worse, by any web page the user visits: a
//! drive-by `fetch()` to `127.0.0.1:PORT` could publish your files. OS-level
//! access control on the channel is the whole authorization model here, which is
//! why the API needs no tokens and stores no secrets.
//!
//! - **Unix**: a socket in a `0700` directory. The *directory* mode is the real
//!   guard, because some platforms have historically ignored permission bits on
//!   socket files, and nobody ignores them on a directory you must traverse.
//! - **Windows**: a named pipe under `\\.\pipe\`, whose default DACL grants the
//!   creating user (see the `windows` module for what that does and does not cover).
//!
//! Only the listener differs per platform. Everything below is generic over
//! [`AsyncRead`]/[`AsyncWrite`], and everything above [`Service::attach`] does
//! not know a transport exists at all.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::client::{AskHandler, Client};
use crate::frame::{Frame, Hello, MAX_FRAME_BYTES};
use crate::service::{ApiError, Service};

#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

#[cfg(unix)]
pub use unix::{connect, ControlListener};
#[cfg(windows)]
pub use windows::{connect, ControlListener};

/// Where the daemon listens by default.
///
/// - Unix: `$XDG_RUNTIME_DIR/iroh-drop/control.sock`, falling back to a path
///   under the user's data directory (macOS has no runtime dir).
/// - Windows: `\\.\pipe\iroh-drop\control`.
pub fn default_socket_path() -> PathBuf {
    #[cfg(windows)]
    {
        return PathBuf::from(windows::DEFAULT_PIPE_NAME);
    }
    #[cfg(unix)]
    {
        if let Some(runtime) = std::env::var_os("XDG_RUNTIME_DIR") {
            let dir = PathBuf::from(runtime);
            if dir.is_dir() {
                return dir.join("iroh-drop").join("control.sock");
            }
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home)
                .join(".local")
                .join("share")
                .join("iroh-drop")
                .join("control.sock");
        }
        PathBuf::from("iroh-drop-control.sock")
    }
}

/// Bridge one accepted connection to [`Service::attach`].
///
/// Generic over the halves so a Unix socket and a Windows pipe share every line
/// of framing logic.
pub(crate) fn serve_io<R, W>(service: Arc<Service>, reader: R, writer: W)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (to_daemon, daemon_rx) = mpsc::channel::<Frame>(64);
    let (to_client, client_rx) = mpsc::channel::<Frame>(256);
    service.attach(daemon_rx, to_client);
    spawn_reader(reader, to_daemon, "client");
    spawn_writer(writer, client_rx);
}

/// Drive a [`Client`] over one connection's halves.
pub(crate) async fn connect_io<R, W>(
    reader: R,
    writer: W,
    hello: Hello,
    on_ask: Option<AskHandler>,
) -> Result<Client, ApiError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (to_socket, socket_rx) = mpsc::channel::<Frame>(64);
    let (to_client, client_rx) = mpsc::channel::<Frame>(256);
    spawn_writer(writer, socket_rx);
    spawn_reader(reader, to_client, "daemon");
    Client::start(to_socket, client_rx, hello, on_ask).await
}

/// Read newline-delimited frames until the peer goes away.
fn spawn_reader<R>(reader: R, sink: mpsc::Sender<Frame>, peer: &'static str)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.len() > MAX_FRAME_BYTES {
                        warn!("frame of {} bytes exceeds the cap; closing", line.len());
                        break;
                    }
                    if line.trim().is_empty() {
                        continue;
                    }
                    match Frame::from_line(&line) {
                        Ok(frame) => {
                            if sink.send(frame).await.is_err() {
                                break;
                            }
                        }
                        // An unparseable line is the peer's bug, not a reason
                        // to drop a working connection.
                        Err(e) => debug!("ignoring unparseable frame from {peer}: {e}"),
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    debug!("read error from {peer}: {e}");
                    break;
                }
            }
        }
    });
}

/// Write frames as newline-delimited JSON.
fn spawn_writer<W>(mut writer: W, mut source: mpsc::Receiver<Frame>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(frame) = source.recv().await {
            let mut line = frame.to_line();
            line.push('\n');
            if writer.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            // Pipes and sockets both benefit: a UI waiting on a progress event
            // should not wait on a buffer.
            if writer.flush().await.is_err() {
                break;
            }
        }
    });
}
