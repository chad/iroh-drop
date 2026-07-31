//! The iroh-drop daemon.
//!
//! Hosts drops so they outlive terminals, and keeps serving what you received
//! so you are actually a replica for other people rather than in theory.
//!
//! ```sh
//! iroh-dropd                  # relays when needed, mDNS on
//! iroh-dropd --lan-only       # no relay, no DNS, no pkarr: nothing leaves the network
//!
//! When a GUI starts this helper it passes --accept-when-no-ui, so consent that
//! arrives while no window is open is still answered — the helper is exactly
//! what "keep sharing" means. Without that flag (a hand-run daemon) an
//! unanswered question is a refusal, because a person with no UI did not ask.
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use iroh_drop_daemon::{ControlListener, Service, ServiceOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // This binary's own target is `iroh_dropd`; without it the
                // daemon starts completely silently.
                .unwrap_or_else(|_| "iroh_dropd=info,iroh_drop_daemon=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let mut lan_only = false;
    let mut socket: Option<PathBuf> = None;
    let mut data_dir: Option<PathBuf> = None;
    let mut download_dir: Option<PathBuf> = None;
    let mut accept_when_no_ui = false;
    let mut link_base: Option<String> = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--lan-only" => lan_only = true,
            "--socket" => socket = args.next().map(PathBuf::from),
            "--data-dir" => data_dir = args.next().map(PathBuf::from),
            "--downloads" => download_dir = args.next().map(PathBuf::from),
            "--accept-when-no-ui" => accept_when_no_ui = true,
            "--link-base" => link_base = args.next(),
            "-h" | "--help" => {
                println!(
                    "iroh-dropd [--lan-only] [--socket PATH] [--data-dir PATH] [--downloads PATH]\n           [--link-base https://your.page]"
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }

    let data_dir = data_dir.unwrap_or_else(default_data_dir);
    std::fs::create_dir_all(&data_dir)?;
    let download_dir = download_dir.unwrap_or_else(default_download_dir);
    std::fs::create_dir_all(&download_dir)?;
    let socket = socket.unwrap_or_else(iroh_drop_daemon::default_socket_path);

    let options = ServiceOptions {
        // Persistent: the point of a daemon is to still be here later.
        store_path: Some(data_dir.join("blobs")),
        identity_path: Some(data_dir.join("identity")),
        offline: lan_only,
        mdns: true,
        download_dir: download_dir.clone(),
        auto_accept: accept_when_no_ui,
        link_base,
    };

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let service = Service::new(options).await?;
        let listener = ControlListener::bind(Arc::clone(&service), &socket).await?;

        tracing::info!(
            endpoint = %service.endpoint_id(),
            socket = %listener.path().display(),
            downloads = %download_dir.display(),
            lan_only,
            "iroh-dropd ready"
        );

        // Ctrl-C and SIGTERM both shut down politely — state is persisted,
        // but no withdrawal is announced: shutting down is not leaving.
        // Anything harder is a crash, which the protocol already tolerates
        // (see tests/publisher_exit.rs).
        #[cfg(unix)]
        let sigterm = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler")
                .recv()
                .await;
        };
        #[cfg(not(unix))]
        let sigterm = std::future::pending::<()>();
        tokio::select! {
            _ = listener.serve() => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down (SIGINT)");
                service.shutdown().await;
            }
            _ = sigterm => {
                tracing::info!("shutting down (SIGTERM)");
                service.shutdown().await;
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

fn default_data_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_DATA_HOME") {
        return PathBuf::from(dir).join("iroh-drop");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("iroh-drop");
    }
    PathBuf::from(".iroh-drop")
}

fn default_download_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let downloads = PathBuf::from(&home).join("Downloads");
        if downloads.is_dir() {
            return downloads.join("iroh-drop");
        }
        return PathBuf::from(home).join("iroh-drop");
    }
    PathBuf::from("iroh-drop-downloads")
}
