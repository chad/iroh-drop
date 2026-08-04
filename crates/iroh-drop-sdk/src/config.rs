//! Where things live, so users never have to say.
//!
//! Defaults follow the XDG layout and are created on demand:
//!
//! | what | default |
//! |---|---|
//! | identity | `$XDG_DATA_HOME/iroh-drop/identity.key` |
//! | blob store | `$XDG_DATA_HOME/iroh-drop/blobs` |
//! | downloads | `$XDG_DATA_HOME/iroh-drop/received` |
//! | config | `$XDG_CONFIG_HOME/iroh-drop/config.toml` |
//!
//! A stable identity and a persistent store are what make a peer recognizable
//! across restarts and let it keep serving what it received.

use std::path::{Path, PathBuf};

use iroh_drop::builder::StackOptions;
use iroh_drop::policy::DropPolicy;
use serde::{Deserialize, Serialize};

use crate::{Result, SdkError};

/// On-disk application defaults.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Persistent endpoint identity file.
    pub identity_path: PathBuf,
    /// Persistent blob store directory.
    pub store_path: PathBuf,
    /// Where received files are written.
    pub download_dir: PathBuf,
    /// Fetch offers automatically instead of waiting for the user.
    pub auto_fetch: bool,
    /// Largest blob to accept automatically, in bytes.
    pub max_auto_blob_size: u64,
    /// Total automatic download budget per session, in bytes.
    pub max_auto_total_bytes: u64,
    /// Base URL for web links in share responses. Set to a web-client host
    /// (e.g. `https://iroh-drop.boxd.sh`) and share results include a
    /// `web_link` a browser can open directly. `None` omits the field.
    pub link_base: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        let data = data_dir();
        Self {
            identity_path: data.join("identity.key"),
            store_path: data.join("blobs"),
            download_dir: data.join("received"),
            auto_fetch: false,
            max_auto_blob_size: DropPolicy::default().max_blob_size,
            max_auto_total_bytes: DropPolicy::default().max_total_auto_fetch_bytes,
            link_base: None,
        }
    }
}

impl Config {
    /// Load the config file, falling back to defaults when absent.
    pub fn load() -> Result<Self> {
        Self::load_from(Self::default_path())
    }

    /// Load a specific config file, falling back to defaults when absent.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .map_err(|e| SdkError::Config(format!("{}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(SdkError::Config(format!("{}: {e}", path.display()))),
        }
    }

    /// Write the config to its default location, creating directories.
    pub fn save(&self) -> Result<PathBuf> {
        let path = Self::default_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let text = toml::to_string_pretty(self).map_err(|e| SdkError::Config(e.to_string()))?;
        std::fs::write(&path, text)?;
        Ok(path)
    }

    /// The default config file path.
    pub fn default_path() -> PathBuf {
        config_dir().join("config.toml")
    }

    /// Stack options for a persistent peer (stable identity, durable store).
    ///
    /// `mdns` turns on local-network address discovery, which is what makes
    /// short tickets and `nearby` work without relays.
    pub fn stack_options(&self, offline: bool, mdns: bool) -> StackOptions {
        StackOptions {
            store_path: Some(self.store_path.clone()),
            offline,
            identity_path: Some(self.identity_path.clone()),
            secret_key: None,
            // Desktop defaults ride the public n0 relays; a custom relay is
            // a StackOptions-level knob, not (yet) a config key.
            relay_url: None,
            mdns,
        }
    }

    /// Stack options for a throwaway peer: fresh identity, memory store.
    pub fn ephemeral_stack_options(offline: bool, mdns: bool) -> StackOptions {
        StackOptions {
            store_path: None,
            offline,
            identity_path: None,
            secret_key: None,
            relay_url: None,
            mdns,
        }
    }

    /// The fetch policy these settings describe.
    pub fn policy(&self) -> DropPolicy {
        DropPolicy {
            auto_fetch: self.auto_fetch,
            max_blob_size: self.max_auto_blob_size,
            max_total_auto_fetch_bytes: self.max_auto_total_bytes,
            ..DropPolicy::default()
        }
    }

    /// Ensure the directories referenced by this config exist.
    pub fn prepare_dirs(&self) -> Result<()> {
        for dir in [&self.store_path, &self.download_dir] {
            std::fs::create_dir_all(dir)?;
        }
        if let Some(parent) = self.identity_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub(crate) fn data_dir() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".local").join("share"))
        .join("iroh-drop")
}

fn config_dir() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_dir().join(".config"))
        .join("iroh-drop")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_under_one_directory() {
        let config = Config::default();
        let data = data_dir();
        assert!(config.store_path.starts_with(&data));
        assert!(config.identity_path.starts_with(&data));
        assert!(config.download_dir.starts_with(&data));
        assert!(!config.auto_fetch, "manual fetch stays the default");
    }

    #[test]
    fn roundtrips_through_toml() {
        let config = Config::default();
        let text = toml::to_string_pretty(&config).unwrap();
        let parsed: Config = toml::from_str(&text).unwrap();
        assert_eq!(parsed.store_path, config.store_path);
        assert_eq!(parsed.max_auto_blob_size, config.max_auto_blob_size);
    }
}
