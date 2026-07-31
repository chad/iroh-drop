//! Everything the user sees.
//!
//! Rule of thumb: a person should be able to use `iroh-drop` without ever
//! reading a hash. Names come from offers, positions come from the listing,
//! and hashes only appear when there is nothing better to show (or with
//! `--verbose`).

use std::collections::HashMap;
use std::path::Path;

use iroh_drop::{BlobHash, DropEvent, DropSession};
use iroh_drop_sdk::inventory::{human_bytes, inventory, InventoryItem};

/// Print the session header: who we are, where things go, and the one string
/// the user actually needs.
pub fn banner(
    session: &DropSession,
    ticket: &iroh_drop::DropTicket,
    mode: &str,
    store: Option<&Path>,
    out: &Path,
) {
    let line = "─".repeat(66);
    eprintln!("{line}");
    eprintln!("  you      {}", session.self_id().fmt_short());
    eprintln!("  network  {mode}");
    eprintln!(
        "  keeping  {}",
        store
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "memory only (nothing survives exit)".into())
    );
    eprintln!("  saving   {}", out.display());
    eprintln!("{line}");
    eprintln!();
    eprintln!("  Share this ticket with whoever should get the files:");
    eprintln!();
    eprintln!("    {ticket}");
    eprintln!();
    eprintln!("  They run:  iroh-drop receive <ticket>");
    eprintln!("{line}");
}

/// Show a ticket as a QR code, for phones and for anyone in the room with a
/// camera. Rendered with half-block characters so it fits a terminal.
pub fn print_qr(ticket: &str) {
    match qrcode::QrCode::new(ticket.as_bytes()) {
        Ok(code) => {
            let rendered = code
                .render::<qrcode::render::unicode::Dense1x2>()
                .quiet_zone(true)
                .build();
            eprintln!();
            eprintln!("{rendered}");
        }
        Err(e) => eprintln!("! could not render a QR code: {e}"),
    }
}

/// Print the numbered listing users pick from.
pub fn print_listing(session: &DropSession) {
    let items = inventory(session);
    if items.is_empty() {
        println!("Nothing here yet. Files show up as peers offer them.");
        return;
    }
    println!("{:>3}  {:<32} {:>10}  status", "#", "name", "size");
    for item in &items {
        println!(
            "{:>3}  {:<32} {:>10}  {}",
            item.index,
            truncate(&item.name, 32),
            item.human_size(),
            describe_status(item),
        );
    }
    println!();
    println!("Fetch with:  get <#>   (or a name)");
}

fn describe_status(item: &InventoryItem) -> String {
    let kind = item.kind();
    let local = match &item.status {
        iroh_drop::LocalBlobStatus::Complete => "have it".to_string(),
        iroh_drop::LocalBlobStatus::Fetching { downloaded, total } => match total {
            Some(total) if *total > 0 => {
                format!("getting {}%", downloaded * 100 / total)
            }
            _ => "getting".to_string(),
        },
        iroh_drop::LocalBlobStatus::Missing => "available".to_string(),
        other => format!("{other}"),
    };
    format!("{kind}, {local}")
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let keep: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{keep}…")
}

/// Human-readable event log. `verbose` adds hashes and protocol chatter.
pub async fn print_events(
    session: DropSession,
    events: &mut tokio::sync::broadcast::Receiver<DropEvent>,
    verbose: bool,
) {
    let mut names: HashMap<BlobHash, String> = HashMap::new();
    let mut progress: HashMap<BlobHash, u64> = HashMap::new();
    while let Ok(event) = events.recv().await {
        match event {
            DropEvent::PeerJoined { peer } => {
                eprintln!("· {} joined", peer.fmt_short())
            }
            DropEvent::PeerLeft { peer } => {
                eprintln!("· {} left", peer.fmt_short())
            }
            DropEvent::OfferReceived { from, offer } => {
                names.insert(offer.blob_hash, offer.name.clone());
                // Prefer what the user will actually receive (a folder's tree
                // size) over the manifest blob's size.
                let shape = inventory(&session)
                    .into_iter()
                    .find(|item| item.hash == offer.blob_hash)
                    .map(|item| format!("{}, {}", item.kind(), item.human_size()))
                    .unwrap_or_else(|| human_bytes(offer.size));
                eprintln!("· {} offers \"{}\" ({shape})", from.fmt_short(), offer.name);
            }
            DropEvent::FetchStarted { hash, provider } => {
                if !verbose && !is_named(&session, hash) {
                    continue; // a collection member; the fetcher reports these
                }
                eprintln!(
                    "↓ getting {} from {}",
                    label(&session, &mut names, hash),
                    provider.fmt_short()
                )
            }
            DropEvent::FetchProgress {
                hash,
                downloaded,
                total,
            } => {
                let Some(total) = total.filter(|t| *t > 0) else {
                    continue;
                };
                let percent = downloaded * 100 / total;
                let mark = progress.entry(hash).or_insert(0);
                // Only speak up every 25%, and only for sizable transfers.
                if total > 1 << 20 && (percent >= *mark + 25 || downloaded == total) {
                    *mark = percent;
                    eprintln!(
                        "↓ {} {percent}% ({} of {})",
                        label(&session, &mut names, hash),
                        human_bytes(downloaded),
                        human_bytes(total)
                    );
                }
            }
            DropEvent::FetchCompleted { hash, provider } => {
                progress.remove(&hash);
                if !verbose && !is_named(&session, hash) {
                    continue;
                }
                eprintln!(
                    "✔ got {} from {}",
                    label(&session, &mut names, hash),
                    provider.fmt_short()
                )
            }
            DropEvent::FetchFailed { hash, error } => {
                progress.remove(&hash);
                eprintln!("✗ {} failed: {error}", label(&session, &mut names, hash))
            }
            DropEvent::ProviderAvailable { hash, peer } => {
                if verbose {
                    eprintln!(
                        "· {} also serves {}",
                        peer.fmt_short(),
                        label(&session, &mut names, hash)
                    )
                }
            }
            DropEvent::ProviderUnavailable { hash, peer } => {
                if verbose {
                    eprintln!(
                        "· {} stopped serving {}",
                        peer.fmt_short(),
                        label(&session, &mut names, hash)
                    )
                }
            }
            DropEvent::OfferRejected { from, reason } => {
                eprintln!("! ignored a message from {}: {reason}", from.fmt_short())
            }
            DropEvent::ProtocolWarning { from, warning } => {
                // Some warnings mean a peer is misbehaving badly enough that
                // the user should know even without --verbose.
                let loud = matches!(
                    warning,
                    iroh_drop::ProtocolWarningKind::RateLimited
                        | iroh_drop::ProtocolWarningKind::InventoryEvicted { .. }
                );
                if verbose || loud {
                    eprintln!(
                        "! {warning}{}",
                        from.map(|p| format!(" (from {})", p.fmt_short()))
                            .unwrap_or_default()
                    )
                }
            }
            // `DropEvent` is non-exhaustive: newer protocol builds may emit
            // things this binary predates.
            other => {
                if verbose {
                    eprintln!("· {other:?}");
                }
            }
        }
    }
}

/// Whether a blob has a real (non-hash) name in the current inventory.
fn is_named(session: &DropSession, hash: BlobHash) -> bool {
    inventory(session)
        .into_iter()
        .any(|item| item.hash == hash && item.name != hash.to_hex())
}

/// Best available human label for a blob.
fn label(session: &DropSession, names: &mut HashMap<BlobHash, String>, hash: BlobHash) -> String {
    if let Some(name) = names.get(&hash) {
        return format!("\"{name}\"");
    }
    if let Some(item) = inventory(session).into_iter().find(|i| i.hash == hash) {
        // A blob fetched by hash gets a hash-shaped "name"; that is not a
        // name a person wants to read.
        if item.name != hash.to_hex() {
            names.insert(hash, item.name.clone());
            return format!("\"{}\"", item.name);
        }
    }
    // Nothing announced this blob under a name: the hash is all we have.
    hash.fmt_short()
}
