//! Turning a drop's contents into something a human can point at.
//!
//! The protocol identifies blobs by hash. People do not want to type hashes,
//! so this module builds a *stable numbered listing* and resolves picks:
//!
//! - `3` — the third item in the listing
//! - `report.pdf` — an offered name (or alias)
//! - `a1b2c3` — a hash prefix, for scripts and pathological cases
//!
//! Ordering is by first-seen time, which is stable for a running session, so
//! numbers do not shuffle under the user between `list` and `get`.

use iroh_drop::hash::BlobHash;
use iroh_drop::session::DropSession;
use iroh_drop::state::LocalBlobStatus;

use crate::collections::{COLLECTION_MEDIA_TYPE, META_MEMBERS, META_TOTAL_BYTES};
use crate::{Result, SdkError};

/// One line of a human-facing listing.
#[derive(Clone, Debug)]
pub struct InventoryItem {
    /// 1-based position in the listing; what the user types.
    pub index: usize,
    /// Preferred display name.
    pub name: String,
    /// Content hash.
    pub hash: BlobHash,
    /// Advertised size in bytes.
    pub size: u64,
    /// Advertised media type, if any.
    pub media_type: Option<String>,
    /// Whether this is a collection manifest (a directory tree).
    pub is_collection: bool,
    /// For collections: number of files, if the publisher said so.
    pub members: Option<usize>,
    /// Bytes the user will actually receive: the tree total for a
    /// collection (when advertised), otherwise the blob size.
    pub content_size: u64,
    /// Local availability.
    pub status: LocalBlobStatus,
    /// Other names this hash has been offered under.
    pub aliases: Vec<String>,
}

impl InventoryItem {
    /// Human-readable size of what the user receives, e.g. `1.4 MiB`.
    pub fn human_size(&self) -> String {
        human_bytes(self.content_size)
    }

    /// Short description of what this item is, e.g. `folder, 12 files`.
    pub fn kind(&self) -> String {
        match (self.is_collection, self.members) {
            (true, Some(1)) => "folder, 1 file".to_string(),
            (true, Some(n)) => format!("folder, {n} files"),
            (true, None) => "folder".to_string(),
            (false, _) => "file".to_string(),
        }
    }
}

/// Format a byte count for humans.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// The current listing for a session, in stable order.
pub fn inventory(session: &DropSession) -> Vec<InventoryItem> {
    session
        .offers()
        .into_iter()
        .enumerate()
        .map(|(i, record)| {
            let mut aliases: Vec<String> = record
                .aliases
                .iter()
                .filter(|name| *name != &record.offer.name)
                .cloned()
                .collect();
            aliases.sort();
            let is_collection = record.offer.media_type.as_deref() == Some(COLLECTION_MEDIA_TYPE);
            // Metadata is untrusted: parse defensively and fall back to the
            // blob size rather than trusting a bogus total.
            let members = record
                .offer
                .metadata
                .get(META_MEMBERS)
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|_| is_collection);
            let content_size = record
                .offer
                .metadata
                .get(META_TOTAL_BYTES)
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|_| is_collection)
                .unwrap_or(record.offer.size);
            InventoryItem {
                index: i + 1,
                name: record.offer.name.clone(),
                hash: record.offer.blob_hash,
                size: record.offer.size,
                is_collection,
                members,
                content_size,
                media_type: record.offer.media_type.clone(),
                status: record.local_status,
                aliases,
            }
        })
        .collect()
}

/// Resolve what a user typed into a hash.
///
/// Accepts a listing number, a name or alias, or a hash prefix. Ambiguity is
/// an error rather than a guess.
pub fn resolve_pick(session: &DropSession, input: &str) -> Result<BlobHash> {
    let input = input.trim();
    if input.is_empty() {
        return Err(SdkError::Pick("nothing to pick".into()));
    }
    let items = inventory(session);

    // 1. Listing number.
    if let Ok(index) = input.parse::<usize>() {
        return items
            .iter()
            .find(|item| item.index == index)
            .map(|item| item.hash)
            .ok_or_else(|| {
                SdkError::Pick(format!(
                    "no item {index} in this drop ({} known)",
                    items.len()
                ))
            });
    }

    // 2. Exact name or alias (protocol-side resolution knows aliases too).
    let by_name: Vec<&InventoryItem> = items
        .iter()
        .filter(|item| item.name == input || item.aliases.iter().any(|a| a == input))
        .collect();
    match by_name.len() {
        1 => return Ok(by_name[0].hash),
        n if n > 1 => {
            return Err(SdkError::Pick(format!(
                "{input:?} matches {n} items; pick by number instead"
            )))
        }
        _ => {}
    }
    if let Some(hash) = session.resolve(input) {
        return Ok(hash);
    }

    // 3. Hash prefix.
    let by_prefix: Vec<&InventoryItem> = items
        .iter()
        .filter(|item| item.hash.matches_prefix(input))
        .collect();
    match by_prefix.len() {
        1 => Ok(by_prefix[0].hash),
        0 => Err(SdkError::Pick(format!(
            "nothing here matches {input:?}; try `list`"
        ))),
        n => Err(SdkError::Pick(format!(
            "{input:?} matches {n} hashes; use more characters"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::human_bytes;

    #[test]
    fn formats_sizes() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1_572_864), "1.5 MiB");
    }
}
