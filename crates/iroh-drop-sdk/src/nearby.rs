//! What is being shared in this room, on this network, right now.
//!
//! When a peer opts into local advertising, it puts a short ticket into its
//! mDNS discovery record. Anyone else on the network can then list what is on
//! offer and join it without typing anything at all.
//!
//! **This is a broadcast.** Advertising a drop on a LAN hands the drop's
//! capability to everyone who can see mDNS traffic there: they can join, read
//! every offer, and publish their own. Applications must make that explicit
//! before advertising, and should treat it as suitable for networks you would
//! read a filename aloud on.

use std::collections::BTreeMap;
use std::time::Duration;

use iroh_drop::builder::DropStack;
use iroh_drop::ticket::DropTicket;
use iroh_mdns_address_lookup::DiscoveryEvent;
use tokio_stream::StreamExt;
use tracing::debug;

use crate::{Result, SdkError};

/// A drop advertised by a peer on the local network.
#[derive(Clone, Debug)]
pub struct NearbyDrop {
    /// 1-based position in the listing; what the user types.
    pub index: usize,
    /// The advertising peer.
    pub peer: iroh::EndpointId,
    /// The ticket they advertised, ready to join.
    pub ticket: DropTicket,
    /// Whatever name the sharer put in the ticket, if any.
    pub label: Option<String>,
}

impl NearbyDrop {
    /// Best available display name for a listing.
    pub fn display(&self) -> String {
        match &self.label {
            Some(label) => label.clone(),
            None => format!("drop from {}", self.peer.fmt_short()),
        }
    }
}

/// Advertise a drop to the local network.
///
/// The ticket is placed in our discovery record, so keep it short — use
/// [`iroh_drop::session::DropSession::short_ticket`], which also stays valid
/// as addresses change. Returns an error if the ticket does not fit in a
/// discovery record.
pub fn advertise(stack: &DropStack, ticket: &DropTicket) -> Result<()> {
    let encoded = ticket.to_string();
    stack.advertise(Some(&encoded))?;
    debug!(
        chars = encoded.len(),
        "advertising a drop on the local network"
    );
    Ok(())
}

/// Stop advertising.
pub fn stop_advertising(stack: &DropStack) -> Result<()> {
    stack.advertise(None)?;
    Ok(())
}

/// Listen for advertised drops for `window`, then return what was found.
///
/// Requires a stack built with [`iroh_drop::builder::StackOptions::mdns`].
/// Peers that advertise something which is not a drop ticket are ignored, and
/// a peer that advertises repeatedly is reported once with its latest ticket.
pub async fn browse(stack: &DropStack, window: Duration) -> Result<Vec<NearbyDrop>> {
    let mdns = stack.mdns().ok_or_else(|| {
        SdkError::Config("local network discovery is off; build the stack with `mdns: true`".into())
    })?;
    let mut events = mdns.subscribe().await;
    let mut found: BTreeMap<iroh::EndpointId, (DropTicket, Option<String>)> = BTreeMap::new();

    let deadline = tokio::time::Instant::now() + window;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, events.next()).await {
            Ok(Some(DiscoveryEvent::Discovered { endpoint_info, .. })) => {
                let peer = endpoint_info.endpoint_id;
                let Some(user_data) = endpoint_info.user_data() else {
                    continue;
                };
                match user_data.to_string().parse::<DropTicket>() {
                    Ok(ticket) => {
                        let label = ticket.options().display_name.clone();
                        found.insert(peer, (ticket, label));
                    }
                    // Not a drop advertisement: somebody else's user data.
                    Err(e) => debug!(peer = %peer.fmt_short(), "ignoring advert: {e}"),
                }
            }
            Ok(Some(DiscoveryEvent::Expired { endpoint_id })) => {
                found.remove(&endpoint_id);
            }
            // `DiscoveryEvent` is non-exhaustive: ignore anything new.
            Ok(Some(_)) => {}
            // Stream ended or the window closed.
            Ok(None) | Err(_) => break,
        }
    }

    Ok(found
        .into_iter()
        .enumerate()
        .map(|(i, (peer, (ticket, label)))| NearbyDrop {
            index: i + 1,
            peer,
            ticket,
            label,
        })
        .collect())
}
