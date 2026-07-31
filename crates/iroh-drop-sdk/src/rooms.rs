//! Named drops, so nobody types a ticket twice.
//!
//! A *room* is just a saved ticket under a name you chose. The first exchange
//! with someone still needs one string; after that, `--room team` is enough.
//!
//! This works because of two things the protocol provides: identities are
//! stable (so a peer id stays valid across restarts) and short tickets name
//! peers by id rather than by address (so a saved ticket does not go stale
//! when someone's IP changes). Rooms accumulate ids as you meet more members,
//! which is what keeps a room joinable after the original creator leaves.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use iroh_drop::ticket::DropTicket;
use serde::{Deserialize, Serialize};

use crate::config::data_dir;
use crate::{Result, SdkError};

/// One saved drop.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Room {
    /// The most recent ticket we know for this drop.
    pub ticket: String,
    /// Unix seconds when the room was first saved.
    pub created_at: u64,
    /// Unix seconds when the room was last joined.
    pub last_used: u64,
    /// Optional human note (defaults to the ticket's display name).
    #[serde(default)]
    pub note: Option<String>,
}

/// The on-disk room book.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Rooms {
    #[serde(default)]
    rooms: BTreeMap<String, Room>,
}

impl Rooms {
    /// Default location of the room book.
    pub fn default_path() -> PathBuf {
        data_dir().join("rooms.toml")
    }

    /// Load the room book, or an empty one if it does not exist yet.
    pub fn load() -> Result<Self> {
        Self::load_from(Self::default_path())
    }

    /// Load a specific room book file.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| SdkError::Config(format!("{}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(SdkError::Config(format!("{}: {e}", path.display()))),
        }
    }

    /// Write the room book back to its default location.
    pub fn save(&self) -> Result<PathBuf> {
        self.save_to(Self::default_path())
    }

    /// Write the room book to a specific path.
    pub fn save_to(&self, path: impl AsRef<Path>) -> Result<PathBuf> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = toml::to_string_pretty(self).map_err(|e| SdkError::Config(e.to_string()))?;
        std::fs::write(path, text)?;
        Ok(path.to_path_buf())
    }

    /// Look up a room by name.
    pub fn get(&self, name: &str) -> Option<&Room> {
        self.rooms.get(name)
    }

    /// The ticket saved for a room.
    pub fn ticket(&self, name: &str) -> Result<DropTicket> {
        let room = self
            .get(name)
            .ok_or_else(|| SdkError::Pick(format!("no room named {name:?}; try `rooms`")))?;
        room.ticket
            .parse()
            .map_err(|e| SdkError::Config(format!("room {name:?} has an unusable ticket: {e}")))
    }

    /// All rooms, most recently used first.
    pub fn list(&self) -> Vec<(&str, &Room)> {
        let mut rooms: Vec<(&str, &Room)> = self
            .rooms
            .iter()
            .map(|(name, room)| (name.as_str(), room))
            .collect();
        rooms.sort_by_key(|(_, room)| std::cmp::Reverse(room.last_used));
        rooms
    }

    /// Save (or refresh) a room. Keeps the original creation time and note.
    pub fn set(&mut self, name: &str, ticket: &DropTicket) {
        validate_room_name(name);
        let now = now_secs();
        let note = ticket.options().display_name.clone();
        let entry = self.rooms.entry(name.to_string()).or_insert(Room {
            ticket: ticket.to_string(),
            created_at: now,
            last_used: now,
            note: note.clone(),
        });
        entry.ticket = ticket.to_string();
        entry.last_used = now;
        if entry.note.is_none() {
            entry.note = note;
        }
    }

    /// Forget a room. Returns whether it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.rooms.remove(name).is_some()
    }

    /// Number of saved rooms.
    pub fn len(&self) -> usize {
        self.rooms.len()
    }

    /// Whether the book is empty.
    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }
}

/// Room names are used as map keys and shown to people; keep them tame.
fn validate_room_name(name: &str) {
    debug_assert!(
        !name.is_empty() && name.len() <= 64,
        "room names must be 1..=64 bytes"
    );
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket() -> DropTicket {
        DropTicket::new([3u8; 32], vec![], Default::default())
    }

    #[test]
    fn saves_and_reloads_rooms() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rooms.toml");

        let mut rooms = Rooms::default();
        rooms.set("team", &ticket());
        rooms.save_to(&path).unwrap();

        let reloaded = Rooms::load_from(&path).unwrap();
        assert_eq!(reloaded.len(), 1);
        assert_eq!(
            reloaded.ticket("team").unwrap().topic_id(),
            ticket().topic_id()
        );
        assert!(reloaded.ticket("nope").is_err());
    }

    #[test]
    fn refreshing_a_room_keeps_its_history() {
        let mut rooms = Rooms::default();
        rooms.set("team", &ticket());
        let created = rooms.get("team").unwrap().created_at;

        // A later, richer ticket for the same room replaces the string but
        // not the room's identity.
        let refreshed = DropTicket::new([3u8; 32], vec![], Default::default());
        rooms.set("team", &refreshed);
        assert_eq!(rooms.get("team").unwrap().created_at, created);
        assert_eq!(rooms.len(), 1);
    }

    #[test]
    fn missing_file_is_an_empty_book() {
        let rooms = Rooms::load_from("/nonexistent/rooms.toml").unwrap();
        assert!(rooms.is_empty());
    }
}
