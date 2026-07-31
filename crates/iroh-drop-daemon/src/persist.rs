//! Drop-level persistence, so a drop survives a cold restart of every
//! surviving daemon — not just the blob cache, the drop itself.
//!
//! Layout, next to the blob store (e.g. `~/.local/share/iroh-drop/store`
//! gets `~/.local/share/iroh-drop/store-daemon/`):
//!
//! ```text
//! drops.json            table of hosted drops: handle, name, ticket
//! frames-<topic>.bin    postcard Vec<Vec<u8>> of retained signed frames
//! ```
//!
//! Files are `0600`; the parent directory follows the store's own `0700`
//! posture. The tickets in `drops.json` are bearer capabilities — that is
//! exactly the sensitivity of the identity key already on disk, and no more.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::service::ServiceOptions;

/// One hosted drop, as of the last persist.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedDrop {
    /// The daemon-local handle (`d3`), kept stable across restarts so scripts
    /// and UIs do not see drops rename themselves.
    pub handle: String,
    /// Display name for drops we created; joined drops have none.
    pub name: Option<String>,
    /// Full ticket (bootstrap addresses included, possibly stale — a join
    /// does not need any of them to be reachable).
    pub ticket: String,
}

/// Reads and writes the daemon's small persistent state.
pub struct DropStore {
    dir: PathBuf,
}

impl DropStore {
    /// The store sits beside the blob store. `None` (in-memory blobs) means
    /// no persistence at all: nothing to anchor it to.
    pub fn for_options(options: &ServiceOptions) -> Option<Self> {
        let store = options.store_path.as_ref()?;
        let name = store.file_name()?.to_str()?;
        Some(Self {
            dir: store.with_file_name(format!("{name}-daemon")),
        })
    }

    fn table_path(&self) -> PathBuf {
        self.dir.join("drops.json")
    }

    fn frames_path(&self, topic: &str) -> PathBuf {
        self.dir.join(format!("frames-{topic}.bin"))
    }

    /// Every drop the daemon was hosting when it last persisted.
    pub fn load_table(&self) -> Vec<PersistedDrop> {
        let Ok(bytes) = std::fs::read(self.table_path()) else {
            return Vec::new();
        };
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            tracing::warn!("ignoring unreadable drops.json: {e}");
            Vec::new()
        })
    }

    /// The retained signed frames for one drop, oldest first.
    pub fn load_frames(&self, topic: &str) -> Vec<Vec<u8>> {
        let Ok(bytes) = std::fs::read(self.frames_path(topic)) else {
            return Vec::new();
        };
        postcard::from_bytes(&bytes).unwrap_or_else(|e| {
            tracing::warn!("ignoring unreadable frames for {topic}: {e}");
            Vec::new()
        })
    }

    /// Persist the whole table plus one drop's frame history atomically-ish
    /// (write-then-rename per file; a crash can lose a beat, never corrupt).
    pub fn save(&self, drops: &[PersistedDrop], topic: &str, frames: &[Vec<u8>]) {
        self.save_table(drops);
        let Ok(bytes) = postcard::to_stdvec(frames) else {
            return;
        };
        self.write_private(&self.frames_path(topic), &bytes);
    }

    /// Persist just the table (used when a drop leaves and only rows change).
    pub fn save_table(&self, drops: &[PersistedDrop]) {
        let Ok(bytes) = serde_json::to_vec_pretty(drops) else {
            return;
        };
        self.write_private(&self.table_path(), &bytes);
    }

    /// Forget one drop entirely (it was deliberately left).
    pub fn remove_drop(&self, topic: &str) {
        let _ = std::fs::remove_file(self.frames_path(topic));
    }

    fn write_private(&self, path: &Path, bytes: &[u8]) {
        if std::fs::create_dir_all(&self.dir).is_err() {
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let mut b = std::fs::DirBuilder::new();
            b.mode(0o700);
            let _ = b.create(&self.dir);
        }
        // Write-then-rename so a crash mid-write never halves the table.
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, bytes).is_err() {
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        let _ = std::fs::rename(&tmp, path);
    }
}
