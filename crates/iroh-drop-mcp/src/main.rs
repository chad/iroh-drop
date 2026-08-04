//! `iroh-drop-mcp` — MCP stdio server attached to a running iroh-drop
//! daemon. Configure it in an MCP-capable agent (Claude Desktop, generic
//! JSON config) and the agent can list, create, join, publish, and fetch —
//! within the daemon's consent rules (see `docs/daemon-api.md`).

use std::path::PathBuf;

use iroh_drop_daemon::default_socket_path;

#[tokio::main]
async fn main() {
    let mut socket: PathBuf = default_socket_path();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => {
                socket = args
                    .next()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| fatal("--socket needs a path"));
            }
            "--help" | "-h" => {
                eprintln!(
                    "iroh-drop-mcp — MCP stdio server for a running iroh-drop daemon\n\
                     \n\
                     Usage: iroh-drop-mcp [--socket PATH]\n\
                     \n\
                     The daemon must already be running (iroh-dropd). stdout is the\n\
                     protocol channel; all diagnostics go to stderr."
                );
                return;
            }
            other => fatal(&format!("unknown argument {other}")),
        }
    }

    if let Err(e) = iroh_drop_mcp::serve_stdio(&socket).await {
        fatal(&format!("{e}"));
    }
}

fn fatal(msg: &str) -> ! {
    eprintln!("iroh-drop-mcp: {msg}");
    std::process::exit(1)
}
