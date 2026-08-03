//! `iroh-drop` — hand files to people with one string, and keep them alive
//! together.
//!
//! Two commands cover the common cases:
//!
//! ```text
//! iroh-drop share ./report.pdf ./photos   # prints a ticket, keeps serving
//! iroh-drop receive <ticket>              # fetches everything, then exits
//! ```
//!
//! Everything else (`open`, `new`) is the same session with an interactive
//! prompt, where files are picked by number or name. Hashes are an
//! implementation detail the user never has to type.

mod daemon_client;
mod ui;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use iroh_drop::{
    CreateOptions, DropBuilder, DropError, DropPolicy, DropProtocol, DropSession, DropTicket,
    StackOptions,
};
use iroh_drop_sdk::collections::{fetch_any_reporting, publish_path, MemberProgress};
use iroh_drop_sdk::inventory::{human_bytes, inventory, resolve_pick};
use iroh_drop_sdk::{Config, NearbyDrop, Rooms};
use tokio::io::{AsyncBufReadExt, BufReader};

/// How long `receive` waits for a drop's contents to appear.
const RECEIVE_WAIT: Duration = Duration::from_secs(60);

#[derive(Parser)]
#[command(
    name = "iroh-drop",
    version,
    about = "Share files with a ticket. Everyone who receives them helps serve them."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Share files or folders and print a ticket. Keeps serving until Ctrl-C.
    Share {
        /// Files or directories to share.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Also write the ticket to this file.
        #[arg(long)]
        ticket_file: Option<PathBuf>,
        /// Show the ticket as a QR code to scan.
        #[arg(long)]
        qr: bool,
        /// Advertise this drop on the local network so `nearby` finds it.
        /// Everyone on the network can then receive these files.
        #[arg(long)]
        lan: bool,
        /// Create a private drop: the ticket carries a drop key and every
        /// frame is sealed. Peers without the key learn nothing.
        #[arg(long)]
        private: bool,
        /// Share into a saved room (created and remembered if new).
        #[arg(long)]
        room: Option<String>,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// Receive files from a ticket, then exit.
    Receive {
        /// The ticket, or `@path/to/file` to read it from a file. Omit when
        /// using --room or --nearby.
        ticket: Option<String>,
        /// What to fetch: a number, a name, or `all` (default).
        #[arg(default_value = "all")]
        what: String,
        /// Where to save. Defaults to the current directory.
        #[arg(long, short)]
        out: Option<PathBuf>,
        /// Keep serving what you received instead of exiting.
        #[arg(long)]
        keep_serving: bool,
        /// Receive from a saved room instead of a ticket.
        #[arg(long)]
        room: Option<String>,
        /// Receive from a drop advertised on the local network, by number.
        #[arg(long)]
        nearby: Option<usize>,
        /// Remember this drop under a name for next time.
        #[arg(long)]
        save_as: Option<String>,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// Join a drop and stay at an interactive prompt.
    Open {
        /// The ticket, or `@path/to/file` to read it from a file. Omit when
        /// using --room.
        ticket: Option<String>,
        /// Join a saved room instead of a ticket.
        #[arg(long)]
        room: Option<String>,
        /// Remember this drop under a name for next time.
        #[arg(long)]
        save_as: Option<String>,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// Start an empty drop and stay at an interactive prompt.
    #[command(alias = "create")]
    New {
        #[command(flatten)]
        args: CommonArgs,
    },
    /// Show a ticket as a QR code for someone to scan.
    Qr {
        /// The ticket, `@path/to/file`, or `--room <name>`.
        ticket: Option<String>,
        /// Show the QR for a saved room instead.
        #[arg(long)]
        room: Option<String>,
    },
    /// Show what a ticket contains, without joining.
    #[command(alias = "inspect-ticket")]
    Inspect {
        /// The ticket, or `@path/to/file` to read it from a file.
        ticket: String,
    },
    /// List drops being shared on the local network.
    Nearby {
        /// Seconds to listen for advertisements.
        #[arg(long, default_value = "3")]
        seconds: u64,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// List, or forget, saved rooms.
    Rooms {
        /// Forget this room.
        #[arg(long)]
        forget: Option<String>,
    },
    /// Show where iroh-drop keeps things, and write a config file.
    Config {
        /// Create the config file with current defaults.
        #[arg(long)]
        init: bool,
    },

    // ── daemon-backed commands ────────────────────────────────────────────
    // These hand work to a running `iroh-dropd`, so they return instead of
    // holding the drop open in the foreground.
    /// Share files through the daemon and exit. It keeps serving them.
    Send {
        /// Files or directories to share.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Name for the drop (untrusted display metadata).
        #[arg(long)]
        name: Option<String>,
        /// Show the ticket as a QR code to scan.
        #[arg(long)]
        qr: bool,
        /// Control socket (defaults to the daemon's usual path).
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Receive files through the daemon, then keep serving them.
    Get {
        /// A ticket, or @file to read one from disk.
        ticket: String,
        /// Which item: a number, a name, or a hash prefix. Default: all.
        pick: Option<String>,
        /// Where to save. Defaults to the daemon's download directory.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Control socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Enter a drop without fetching, so offers arrive as prompts.
    Join {
        /// A ticket, or @file to read one from disk.
        ticket: String,
        /// Control socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Show incoming offers and approve them. This is the receiving end.
    Watch {
        /// Accept everything without asking. Dangerous: anyone with the
        /// ticket can then write to your download directory.
        #[arg(long = "yes")]
        accept_all: bool,
        /// Control socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Leave a drop you joined (or stop hosting one you started).
    Leave {
        /// The drop handle from `iroh-drop drops` (like `d2`), or `all`.
        drop: String,
        /// Control socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// List what the daemon is hosting.
    Drops {
        /// Stop hosting this drop (a handle like `d1`).
        #[arg(long)]
        forget: Option<String>,
        /// Print a fresh ticket for this drop, listing us first.
        #[arg(long)]
        ticket: Option<String>,
        /// Control socket.
        #[arg(long)]
        socket: Option<PathBuf>,
    },
}

#[derive(clap::Args, Default)]
struct CommonArgs {
    /// Where to save received files.
    #[arg(long)]
    dir: Option<PathBuf>,
    /// Blob store directory (defaults to the configured store).
    #[arg(long)]
    store: Option<PathBuf>,
    /// Identity file, so peers recognize you across restarts.
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Forget everything on exit: fresh identity, memory-only store.
    #[arg(long)]
    ephemeral: bool,
    /// Direct connections only: no relays, no public address lookup (LAN).
    #[arg(long)]
    offline: bool,
    /// Find peers on the local network with mDNS (implied by --offline).
    #[arg(long)]
    mdns: bool,
    /// Print the full ticket with socket addresses instead of the short one.
    #[arg(long)]
    full_ticket: bool,
    /// Fetch everything offered, automatically.
    #[arg(long)]
    auto: bool,
    /// Display name embedded in the ticket (untrusted metadata).
    #[arg(long)]
    name: Option<String>,
    /// Show hashes and protocol chatter.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Inspect { ticket } => {
            let ticket = read_ticket(&ticket)?;
            println!("{ticket:#?}");
            Ok(())
        }
        Command::Qr { ticket, room } => {
            let ticket = match (ticket, room) {
                (Some(ticket), _) => read_ticket(&ticket)?,
                (None, Some(room)) => Rooms::load()?.ticket(&room)?,
                (None, None) => bail!("give a ticket, @file, or --room <name>"),
            };
            println!("{ticket}");
            ui::print_qr(&ticket.to_string());
            Ok(())
        }
        Command::Config { init } => config_command(init),
        Command::Send {
            paths,
            name,
            qr,
            socket,
        } => daemon_client::send(paths, name, qr, daemon_client::socket_arg(socket)).await,
        Command::Get {
            ticket,
            pick,
            out,
            socket,
        } => daemon_client::get(ticket, pick, out, daemon_client::socket_arg(socket)).await,
        Command::Join { ticket, socket } => {
            daemon_client::join(ticket, daemon_client::socket_arg(socket)).await
        }
        Command::Watch { accept_all, socket } => {
            daemon_client::watch(accept_all, daemon_client::socket_arg(socket)).await
        }
        Command::Leave { drop, socket } => {
            daemon_client::leave(drop, daemon_client::socket_arg(socket)).await
        }
        Command::Drops {
            forget,
            ticket,
            socket,
        } => daemon_client::drops(forget, ticket, daemon_client::socket_arg(socket)).await,
        Command::Rooms { forget } => rooms_command(forget),
        Command::Nearby { seconds, args } => nearby_command(seconds, args).await,
        Command::Share {
            paths,
            ticket_file,
            qr,
            lan,
            private,
            room,
            args,
        } => share(paths, ticket_file, qr, lan, private, room, args).await,
        Command::Receive {
            ticket,
            what,
            out,
            keep_serving,
            room,
            nearby,
            save_as,
            args,
        } => {
            let ticket = resolve_source(ticket, room.clone(), nearby, &args).await?;
            receive(ticket, what, out, keep_serving, save_as, room, args).await
        }
        Command::Open {
            ticket,
            room,
            save_as,
            args,
        } => {
            let ticket = resolve_source(ticket, room.clone(), None, &args).await?;
            let (protocol, session, ctx) = start(&args, Some(ticket), false).await?;
            interactive(protocol, session, ctx, save_as, room).await
        }
        Command::New { args } => {
            let (protocol, session, ctx) = start(&args, None, false).await?;
            interactive(protocol, session, ctx, None, None).await
        }
    }
}

/// Shut down in the right order: announce withdrawal, stop the event
/// printer (it holds a session handle), release the last session handle,
/// then close the endpoint. Skipping the ordering leaves the endpoint to be
/// dropped without closing, which iroh rightly complains about.
async fn teardown(
    protocol: DropProtocol,
    session: DropSession,
    printer: tokio::task::JoinHandle<()>,
) {
    // `shutdown` consumes the handle after announcing withdrawal.
    session.shutdown().await.ok();
    // The printer holds its own session clone; let it go before closing the
    // endpoint, or the stack has live references and cannot close cleanly.
    printer.abort();
    let _ = printer.await;
    protocol.shutdown().await.ok();
}

/// Resolved runtime settings for one command.
struct Ctx {
    /// Whether tickets we hand out can name peers by id alone.
    short_tickets: bool,
    out_dir: PathBuf,
    store: Option<PathBuf>,
    mode: String,
    verbose: bool,
    /// Whether we joined someone else's drop (so there may be contents to
    /// catch up on) rather than starting our own.
    joined: bool,
}

/// Build a stack, then create or join a drop.
async fn start(
    args: &CommonArgs,
    ticket: Option<DropTicket>,
    private: bool,
) -> Result<(DropProtocol, DropSession, Ctx)> {
    init_tracing(args.verbose);
    let config = Config::load().context("loading config")?;

    // Offline mode has no relay and no DNS lookup, so local discovery is the
    // only way to reach a peer by id: turn it on automatically there.
    let mdns = args.mdns || args.offline;
    let mut options = if args.ephemeral {
        Config::ephemeral_stack_options(args.offline, mdns)
    } else {
        config.prepare_dirs().context("creating directories")?;
        config.stack_options(args.offline, mdns)
    };
    if let Some(store) = &args.store {
        options = StackOptions {
            store_path: Some(store.clone()),
            ..options
        };
    }
    if let Some(identity) = &args.identity {
        options = StackOptions {
            identity_path: Some(identity.clone()),
            ..options
        };
    }

    let out_dir = args
        .dir
        .clone()
        .unwrap_or_else(|| config.download_dir.clone());
    std::fs::create_dir_all(&out_dir).with_context(|| format!("creating {}", out_dir.display()))?;

    let policy = DropPolicy {
        auto_fetch: args.auto,
        output_directory: out_dir.clone(),
        ..config.policy()
    };

    let store = options.store_path.clone();
    let offline = options.offline;
    let protocol = DropBuilder::from_options(options)
        .await?
        .policy(policy)
        .build()
        .await?;

    // A short ticket names us by id, so it is only usable once our address is
    // actually published. Printing it earlier hands out a string that fails
    // for the first few seconds — worse than waiting.
    if !offline {
        protocol.stack().wait_online().await;
    }

    let ticket_was_given = ticket.is_some();
    let session = match ticket {
        None => {
            protocol
                .create(CreateOptions {
                    display_name: args.name.clone(),
                    auto_fetch_recommended: args.auto,
                    private,
                })
                .await?
        }
        Some(ticket) => protocol.join(ticket).await?,
    };

    let joined = ticket_was_given;
    let ctx = Ctx {
        joined,
        // A short ticket needs the joiner to be able to resolve an id: online
        // that is pkarr/DNS, offline it is mDNS. Otherwise addresses must be
        // in the string.
        short_tickets: !args.full_ticket && (!args.offline || mdns),
        out_dir,
        store,
        mode: if args.offline {
            "direct only (no relays)".into()
        } else {
            "relays + public lookup".into()
        },
        verbose: args.verbose,
    };
    Ok((protocol, session, ctx))
}

/// `share`: publish paths, print the ticket, keep serving.
#[allow(clippy::too_many_arguments)]
async fn share(
    paths: Vec<PathBuf>,
    ticket_file: Option<PathBuf>,
    qr: bool,
    lan: bool,
    private: bool,
    room: Option<String>,
    args: CommonArgs,
) -> Result<()> {
    // Sharing "into a room" reuses that room's drop, so people who already
    // hold its ticket keep working.
    let existing = match &room {
        Some(name) => Rooms::load()?.ticket(name).ok(),
        None => None,
    };
    let (protocol, session, ctx) = start(&args, existing, private).await?;

    // Publishing before anyone has joined is fine: offers are retained and
    // handed to late joiners by catch-up sync.
    for path in &paths {
        let published = publish_path(&session, path, None)
            .await
            .with_context(|| format!("sharing {}", path.display()))?;
        println!("sharing {}", describe_published(&published));
    }

    let ticket = ticket_for(&session, &ctx);
    ui::banner(
        &session,
        &ticket,
        &ctx.mode,
        ctx.store.as_deref(),
        &ctx.out_dir,
    );
    if qr {
        ui::print_qr(&ticket.to_string());
    }
    persist_ticket(&ticket, ctx.store.as_deref(), ticket_file.as_deref());
    if let Some(name) = &room {
        remember_room(name, &ticket)?;
        eprintln!("Saved as room \"{name}\" — next time: iroh-drop share --room {name} <paths>");
    }
    if lan {
        iroh_drop_sdk::nearby::advertise(protocol.stack(), &ticket)?;
        eprintln!();
        eprintln!("⚠ Advertising on this network: anyone here can list and");
        eprintln!("  receive these files with `iroh-drop nearby`.");
    }

    let mut events = session.subscribe();
    let printer = tokio::spawn({
        let session = session.clone();
        async move { ui::print_events(session, &mut events, ctx.verbose).await }
    });

    eprintln!("Serving. Press Ctrl-C to stop.");
    tokio::signal::ctrl_c().await.ok();
    eprintln!("\nStopping (telling peers you are going away)...");
    teardown(protocol, session, printer).await;
    Ok(())
}

/// `receive`: join, wait for the inventory, fetch, report where files landed.
async fn receive(
    ticket: DropTicket,
    what: String,
    out: Option<PathBuf>,
    keep_serving: bool,
    save_as: Option<String>,
    room: Option<String>,
    mut args: CommonArgs,
) -> Result<()> {
    let out_dir = out.unwrap_or_else(|| PathBuf::from("."));
    args.dir = Some(out_dir.clone());
    let (protocol, session, ctx) = start(&args, Some(ticket), false).await?;

    let mut events = session.subscribe();
    let printer = tokio::spawn({
        let session = session.clone();
        async move { ui::print_events(session, &mut events, ctx.verbose).await }
    });

    eprintln!("Connecting to the drop...");
    let items = wait_for_inventory(&session).await?;
    eprintln!(
        "Found {} item{} in this drop.",
        items.len(),
        if items.len() == 1 { "" } else { "s" }
    );

    let wanted: Vec<_> = if what == "all" {
        items.iter().map(|item| item.hash).collect()
    } else {
        vec![resolve_pick(&session, &what)?]
    };

    let mut saved = Vec::new();
    for hash in wanted {
        let paths = fetch_any_reporting(&session, hash, &out_dir, report_member).await?;
        saved.extend(paths);
    }

    println!();
    if saved.is_empty() {
        println!("Nothing was saved.");
    } else {
        println!("Saved {} file(s) to {}:", saved.len(), out_dir.display());
        for path in saved.iter().take(20) {
            println!("  {}", path.display());
        }
        if saved.len() > 20 {
            println!("  ... and {} more", saved.len() - 20);
        }
    }

    // Remember the drop with a ticket that points at us too, so the room
    // keeps working once the original sharer is gone. Joining an existing
    // room refreshes it quietly; only an explicit --save-as is announced.
    if let Some(name) = save_as.as_ref().or(room.as_ref()) {
        remember_room(name, &ticket_for(&session, &ctx))?;
        if save_as.is_some() {
            println!();
            println!("Saved as room \"{name}\" — next time: iroh-drop receive --room {name}");
        }
    }

    if keep_serving {
        println!();
        println!("Still serving these files. Press Ctrl-C to stop.");
        println!();
        println!("Others can get them from you with this ticket:");
        println!();
        println!("    {}", ticket_for(&session, &ctx));
        tokio::signal::ctrl_c().await.ok();
    }

    teardown(protocol, session, printer).await;
    Ok(())
}

/// Wait for the drop's contents to appear, or time out quietly.
async fn await_inventory(
    session: &DropSession,
    timeout: Duration,
) -> Vec<iroh_drop_sdk::InventoryItem> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut nudged = false;
    loop {
        let items = inventory(session);
        if !items.is_empty() || tokio::time::Instant::now() >= deadline {
            return items;
        }
        if !nudged && session.peers().is_empty() {
            nudged = true;
            eprintln!("Waiting for a peer to answer...");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Same, but a drop with nothing in it is an error for `receive`.
async fn wait_for_inventory(session: &DropSession) -> Result<Vec<iroh_drop_sdk::InventoryItem>> {
    let items = await_inventory(session, RECEIVE_WAIT).await;
    if items.is_empty() {
        bail!(
            "no files appeared within {}s — is the sharing peer still running?",
            RECEIVE_WAIT.as_secs()
        );
    }
    Ok(items)
}

/// Interactive prompt: numbered picks, no hashes required.
async fn interactive(
    protocol: DropProtocol,
    session: DropSession,
    ctx: Ctx,
    save_as: Option<String>,
    room: Option<String>,
) -> Result<()> {
    let ticket = ticket_for(&session, &ctx);
    ui::banner(
        &session,
        &ticket,
        &ctx.mode,
        ctx.store.as_deref(),
        &ctx.out_dir,
    );
    persist_ticket(&ticket, ctx.store.as_deref(), None);
    if let Some(name) = save_as.as_ref().or(room.as_ref()) {
        remember_room(name, &ticket)?;
        if save_as.is_some() {
            eprintln!("Saved as room \"{name}\".");
        }
    }
    eprintln!("Type `help` for commands.");

    let mut events = session.subscribe();
    let printer = tokio::spawn({
        let session = session.clone();
        async move { ui::print_events(session, &mut events, ctx.verbose).await }
    });

    if ctx.joined {
        eprintln!("Looking at what is in this drop...");
        // Catch-up sync usually answers in well under a second; do not make
        // the user type `ls` to find out.
        let _ = await_inventory(&session, Duration::from_secs(10)).await;
        ui::print_listing(&session);
    }

    let result = repl(&session, &ctx).await;
    eprintln!("Leaving (telling peers you are going away)...");
    teardown(protocol, session, printer).await;
    result
}

async fn repl(session: &DropSession, ctx: &Ctx) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    loop {
        let line = tokio::select! {
            _ = &mut ctrl_c => {
                eprintln!();
                return Ok(());
            }
            line = lines.next_line() => match line? {
                Some(line) => line,
                None => return Ok(()),
            },
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match command(session, ctx, line).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => match e.downcast_ref::<DropError>() {
                Some(DropError::Shutdown) => return Ok(()),
                _ => eprintln!("✗ {e:#}"),
            },
        }
    }
}

/// Run one command. Returns `true` when the user wants to leave.
async fn command(session: &DropSession, ctx: &Ctx, line: &str) -> Result<bool> {
    let mut parts = line.split_whitespace();
    let Some(cmd) = parts.next() else {
        return Ok(false);
    };
    let rest: Vec<&str> = parts.collect();
    match cmd {
        "add" | "share" => {
            let (path, name) = parse_add(&rest)?;
            let published = publish_path(session, &path, name).await?;
            println!("sharing {}", describe_published(&published));
        }
        "get" => {
            let (what, out) = parse_get(&rest)?;
            let dest = out.unwrap_or_else(|| ctx.out_dir.clone());
            let hashes = if what == "all" {
                inventory(session).into_iter().map(|i| i.hash).collect()
            } else {
                vec![resolve_pick(session, &what)?]
            };
            if hashes.is_empty() {
                println!("Nothing to get yet.");
            }
            for hash in hashes {
                let saved = fetch_any_reporting(session, hash, &dest, report_member).await?;
                for path in saved {
                    println!("saved {}", path.display());
                }
            }
        }
        "ls" | "list" => ui::print_listing(session),
        "who" | "peers" => {
            let peers = session.peers();
            if peers.is_empty() {
                println!("Nobody else is connected right now.");
            }
            for peer in peers {
                println!("{}", peer.fmt_short());
            }
        }
        "ticket" => println!("{}", ticket_for(session, ctx)),
        "help" => print_help(),
        "quit" | "exit" | "q" => return Ok(true),
        other => println!("Unknown command `{other}`. Try `help`."),
    }
    Ok(false)
}

/// One line describing what was just shared.
fn describe_published(published: &iroh_drop_sdk::collections::Published) -> String {
    if published.is_collection {
        format!(
            "\"{}\" ({} file{}, {})",
            published.blob.name,
            published.members,
            if published.members == 1 { "" } else { "s" },
            human_bytes(published.total_size)
        )
    } else {
        format!(
            "\"{}\" ({})",
            published.blob.name,
            human_bytes(published.total_size)
        )
    }
}

/// Progress line for collection members, so a 500-file folder does not
/// print 500 hashes.
fn report_member(progress: MemberProgress<'_>) {
    eprintln!(
        "↓ {} {}/{}  {}",
        progress.collection, progress.index, progress.total, progress.path
    );
}

fn print_help() {
    println!(
        "\
commands
  ls                       list what is in this drop, numbered
  get <#|name|all> [to <path>]
                           fetch an item (folders come out as folders)
  add <path> [as <name>]    share a file or folder
  who                      who is connected
  ticket                   print the ticket for new joiners
  quit                     leave the drop"
    );
}

fn parse_add(args: &[&str]) -> Result<(PathBuf, Option<String>)> {
    let Some(path) = args.first() else {
        bail!("usage: add <path> [as <name>]")
    };
    let name = match args.get(1) {
        None => None,
        Some(&"as") if args.len() == 3 => Some(args[2].to_string()),
        _ => bail!("usage: add <path> [as <name>]"),
    };
    Ok((PathBuf::from(path), name))
}

fn parse_get(args: &[&str]) -> Result<(String, Option<PathBuf>)> {
    let Some(what) = args.first() else {
        bail!("usage: get <#|name|all> [to <path>]")
    };
    let out = match args.get(1) {
        None => None,
        Some(&"to") | Some(&"out") if args.len() == 3 => Some(PathBuf::from(args[2])),
        _ => bail!("usage: get <#|name|all> [to <path>]"),
    };
    Ok((what.to_string(), out))
}

/// The ticket we hand out: short when ids are resolvable, full otherwise.
fn ticket_for(session: &DropSession, ctx: &Ctx) -> DropTicket {
    if ctx.short_tickets {
        session.short_ticket()
    } else {
        session.ticket()
    }
}

/// Save (or refresh) a room.
fn remember_room(name: &str, ticket: &DropTicket) -> Result<()> {
    let mut rooms = Rooms::load()?;
    rooms.set(name, ticket);
    rooms.save()?;
    Ok(())
}

/// Work out which drop to join: an explicit ticket, a saved room, or one
/// advertised on the local network.
async fn resolve_source(
    ticket: Option<String>,
    room: Option<String>,
    nearby: Option<usize>,
    args: &CommonArgs,
) -> Result<DropTicket> {
    match (ticket, room, nearby) {
        (Some(ticket), _, _) => read_ticket(&ticket),
        (None, Some(room), _) => Rooms::load()?.ticket(&room).map_err(Into::into),
        (None, None, Some(index)) => {
            let drops = browse_nearby(args, Duration::from_secs(3)).await?;
            drops
                .into_iter()
                .find(|drop| drop.index == index)
                .map(|drop| drop.ticket)
                .ok_or_else(|| anyhow::anyhow!("no drop {index} nearby; try `iroh-drop nearby`"))
        }
        (None, None, None) => bail!("give a ticket, --room <name>, or --nearby <#>"),
    }
}

/// Build a throwaway stack just to listen for local advertisements.
async fn browse_nearby(args: &CommonArgs, window: Duration) -> Result<Vec<NearbyDrop>> {
    let protocol = DropBuilder::from_options(StackOptions {
        offline: args.offline,
        mdns: true,
        ..Config::ephemeral_stack_options(args.offline, true)
    })
    .await?
    .build()
    .await?;
    let drops = iroh_drop_sdk::nearby::browse(protocol.stack(), window).await?;
    protocol.shutdown().await.ok();
    Ok(drops)
}

async fn nearby_command(seconds: u64, args: CommonArgs) -> Result<()> {
    init_tracing(args.verbose);
    eprintln!("Listening for drops on this network for {seconds}s...");
    let drops = browse_nearby(&args, Duration::from_secs(seconds)).await?;
    if drops.is_empty() {
        println!("Nothing is being shared nearby.");
        println!("(The sharer needs to run `iroh-drop share --lan <paths>`.)");
        return Ok(());
    }
    println!("{:>3}  {:<28} who", "#", "what");
    for drop in &drops {
        println!(
            "{:>3}  {:<28} {}",
            drop.index,
            drop.display(),
            drop.peer.fmt_short()
        );
    }
    println!();
    println!("Receive with:  iroh-drop receive --nearby <#>");
    Ok(())
}

fn rooms_command(forget: Option<String>) -> Result<()> {
    let mut rooms = Rooms::load()?;
    if let Some(name) = forget {
        if rooms.remove(&name) {
            rooms.save()?;
            println!("Forgot room \"{name}\".");
        } else {
            println!("No room named \"{name}\".");
        }
        return Ok(());
    }
    if rooms.is_empty() {
        println!("No saved rooms yet.");
        println!("(Add one with `share --room <name>` or `receive ... --save-as <name>`.)");
        return Ok(());
    }
    println!("{:<20} note", "room");
    for (name, room) in rooms.list() {
        println!("{:<20} {}", name, room.note.as_deref().unwrap_or("-"));
    }
    println!();
    println!("Use with:  iroh-drop share --room <name> <paths>   |   receive --room <name>");
    Ok(())
}

fn config_command(init: bool) -> Result<()> {
    let config = Config::load()?;
    println!("config file   {}", Config::default_path().display());
    println!("identity      {}", config.identity_path.display());
    println!("blob store    {}", config.store_path.display());
    println!("downloads     {}", config.download_dir.display());
    println!("auto fetch    {}", config.auto_fetch);
    println!(
        "auto limits   {} per file, {} per session",
        human_bytes(config.max_auto_blob_size),
        human_bytes(config.max_auto_total_bytes)
    );
    if init {
        let path = config.save()?;
        println!();
        println!("wrote {}", path.display());
    }
    Ok(())
}

/// Keep the ticket somewhere the user can find it again.
fn persist_ticket(ticket: &DropTicket, store: Option<&Path>, ticket_file: Option<&Path>) {
    let ticket = ticket.to_string();
    if let Some(dir) = store {
        std::fs::create_dir_all(dir).ok();
        if let Err(e) = std::fs::write(dir.join("ticket"), &ticket) {
            eprintln!("! could not save the ticket: {e}");
        }
    }
    if let Some(path) = ticket_file {
        match std::fs::write(path, &ticket) {
            Ok(()) => eprintln!("ticket written to {}", path.display()),
            Err(e) => eprintln!("! could not write {}: {e}", path.display()),
        }
    }
}

/// Pull a ticket out of whatever the user pasted: a bare ticket, an
/// `iroh-drop://receive/<ticket>` link, an `https://host/#<ticket>` link, or any
/// of those surrounded by chat-app chatter.
pub(crate) fn ticket_from_text(input: &str) -> Option<&str> {
    // Accept both prefixes: `drop2` (current) and `drop1` (pre-hardening,
    // which the ticket parser will reject with a precise version error).
    let start = input
        .find("drop2")
        .into_iter()
        .chain(input.find("drop1"))
        .min()?;
    let end = input[start..]
        .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .map_or(input.len(), |offset| start + offset);
    let candidate = &input[start..end];
    (candidate.len() > 32).then_some(candidate)
}

pub(crate) fn read_ticket(arg: &str) -> Result<DropTicket> {
    let text = match arg.strip_prefix('@') {
        Some(path) => {
            std::fs::read_to_string(path).with_context(|| format!("reading ticket file {path}"))?
        }
        None => arg.to_string(),
    };
    let text = text.trim();
    // Accept a link as readily as a bare ticket, because a link is what people
    // are actually given.
    let text = ticket_from_text(text).unwrap_or(text);
    text.parse()
        .with_context(|| "that does not look like an iroh-drop link".to_string())
}

fn init_tracing(verbose: bool) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = if verbose {
        EnvFilter::new("iroh_drop=debug,iroh_drop_sdk=debug,iroh_drop_cli=debug")
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error"))
    };
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .init();
}

#[cfg(test)]
mod tests {
    use super::ticket_from_text;

    const TICKET: &str = "drop2aimfofis3yfxv6oqyama7hct5hgil7ozmrwh4u7ryd6ihbtadeesuaitapjhbfsnoh";

    #[test]
    fn reads_every_shape_a_person_might_paste() {
        for input in [
            TICKET.to_string(),
            format!("iroh-drop://receive/{TICKET}"),
            format!("https://drop.example/#{TICKET}"),
            format!("here you go: iroh-drop://receive/{TICKET}\n"),
            format!("<{TICKET}>"),
        ] {
            assert_eq!(
                ticket_from_text(&input),
                Some(TICKET),
                "failed on {input:?}"
            );
        }
    }

    #[test]
    fn rejects_near_misses() {
        assert_eq!(ticket_from_text(""), None);
        assert_eq!(ticket_from_text("iroh-drop://receive/"), None);
        assert_eq!(ticket_from_text("drop2short"), None);
        assert_eq!(ticket_from_text("drop1short"), None);
    }
}
