//! Directory trees as a *convention*, not a wire feature.
//!
//! A collection is one ordinary blob — a JSON manifest listing member paths,
//! hashes and sizes — announced with the [`COLLECTION_MEDIA_TYPE`] media
//! type. Members are imported into the blob store without announcements, so
//! a 500-file directory produces exactly one offer instead of 500.
//!
//! The protocol never parses manifests: a peer that does not know this
//! convention simply sees a small JSON blob, which is exactly the graceful
//! degradation we want.

use std::path::{Component, Path, PathBuf};

use bytes::Bytes;
use iroh_drop::hash::BlobHash;
use iroh_drop::session::{DropSession, FetchOutput, PublishedBlob};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{Result, SdkError};

/// Media type advertising that a blob is a collection manifest.
pub const COLLECTION_MEDIA_TYPE: &str = "application/vnd.iroh-drop.collection+json";

/// Offer metadata key: number of files in the collection.
pub const META_MEMBERS: &str = "collection.members";

/// Offer metadata key: total size of all members, in bytes.
pub const META_TOTAL_BYTES: &str = "collection.total_bytes";

/// Largest manifest we will parse (a manifest is metadata, not payload).
pub const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;

/// Most members one collection may contain.
pub const MAX_MEMBERS: usize = 20_000;

/// A collection manifest.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    /// Marker and version, so a manifest is self-describing even when the
    /// media type is unknown (e.g. a late joiner with no offer metadata).
    pub iroh_drop_collection: u16,
    /// Display name of the tree (usually the directory name).
    pub name: String,
    /// Members, in stable sorted order.
    pub entries: Vec<ManifestEntry>,
}

/// One member of a collection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Relative path with `/` separators. Never absolute, never `..`.
    pub path: String,
    /// Content hash, hex.
    pub hash: String,
    /// Size in bytes.
    pub size: u64,
}

impl Manifest {
    /// Total size of all members.
    pub fn total_size(&self) -> u64 {
        self.entries.iter().map(|e| e.size).sum()
    }
}

/// What a publish produced: the announced blob plus what it stands for.
#[derive(Clone, Debug)]
pub struct Published {
    /// The announced blob (for a directory: the manifest).
    pub blob: PublishedBlob,
    /// Number of files shared (1 for a plain file).
    pub members: usize,
    /// Total bytes shared, excluding manifest overhead.
    pub total_size: u64,
    /// Whether this was published as a collection.
    pub is_collection: bool,
}

/// Publish a file *or* a directory.
///
/// Files are published directly. Directories are imported member by member
/// and announced as a single collection manifest, with the member count and
/// total size carried in offer metadata so receivers can see what they are
/// about to download before fetching anything.
pub async fn publish_path(
    session: &DropSession,
    path: impl AsRef<Path>,
    name: Option<String>,
) -> Result<Published> {
    let path = path.as_ref();
    let meta = std::fs::metadata(path)?;
    if meta.is_file() {
        let blob = session.publish_path_as(path, name).await?;
        return Ok(Published {
            members: 1,
            total_size: blob.size,
            is_collection: false,
            blob,
        });
    }
    if !meta.is_dir() {
        return Err(SdkError::Io(format!(
            "{} is neither a file nor a directory",
            path.display()
        )));
    }

    let name = match name {
        Some(name) => name,
        None => path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("collection")
            .to_string(),
    };

    let mut files = Vec::new();
    collect_files(path, path, &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(SdkError::Io(format!("{} is empty", path.display())));
    }
    if files.len() > MAX_MEMBERS {
        return Err(SdkError::Manifest(format!(
            "{} files exceeds the {MAX_MEMBERS} member limit",
            files.len()
        )));
    }

    let mut entries = Vec::with_capacity(files.len());
    for rel in &files {
        let full = path.join(rel);
        let (hash, size) = session.import_path(&full).await?;
        entries.push(ManifestEntry {
            path: rel.to_string_lossy().replace('\\', "/"),
            hash: hash.to_hex(),
            size,
        });
    }
    let manifest = Manifest {
        iroh_drop_collection: 1,
        name: name.clone(),
        entries,
    };
    debug!(
        name = %name,
        members = manifest.entries.len(),
        bytes = manifest.total_size(),
        "publishing collection"
    );
    let json =
        serde_json::to_vec_pretty(&manifest).map_err(|e| SdkError::Manifest(e.to_string()))?;
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(META_MEMBERS.to_string(), manifest.entries.len().to_string());
    metadata.insert(
        META_TOTAL_BYTES.to_string(),
        manifest.total_size().to_string(),
    );
    let blob = session
        .publish_bytes_with(
            name,
            Bytes::from(json),
            Some(COLLECTION_MEDIA_TYPE.to_string()),
            metadata,
        )
        .await?;
    Ok(Published {
        blob,
        members: manifest.entries.len(),
        total_size: manifest.total_size(),
        is_collection: true,
    })
}

/// Fetch a blob, materializing collections as directory trees.
///
/// Returns the paths written. Plain blobs land in `dest` under a
/// collision-safe name; collections become `dest/<name>/...`.
pub async fn fetch_any(
    session: &DropSession,
    hash: BlobHash,
    dest: impl AsRef<Path>,
) -> Result<Vec<PathBuf>> {
    fetch_any_reporting(session, hash, dest, |_| {}).await
}

/// Progress of a collection fetch, member by member.
#[derive(Clone, Debug)]
pub struct MemberProgress<'a> {
    /// 1-based member index.
    pub index: usize,
    /// Total members in the collection.
    pub total: usize,
    /// Relative path of the member just fetched.
    pub path: &'a str,
    /// Collection name.
    pub collection: &'a str,
}

/// [`fetch_any`], reporting each member as it lands.
///
/// Apps use this to show "3/120 files" instead of a wall of hashes.
pub async fn fetch_any_reporting(
    session: &DropSession,
    hash: BlobHash,
    dest: impl AsRef<Path>,
    mut on_member: impl FnMut(MemberProgress<'_>),
) -> Result<Vec<PathBuf>> {
    let dest = dest.as_ref();
    std::fs::create_dir_all(dest)?;

    // Fetch the blob itself first: for a collection this is just the
    // manifest, which is small.
    let result = session.fetch(hash, FetchOutput::Store).await?;

    match read_manifest(session, hash, result.size).await? {
        Some(manifest) => {
            let root = dest.join(sanitize_component(&manifest.name)?);
            std::fs::create_dir_all(&root)?;
            let mut written: Vec<PathBuf> = Vec::with_capacity(manifest.entries.len());
            for entry in &manifest.entries {
                let rel = sanitize_relative(&entry.path)?;
                let target = root.join(&rel);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let member = BlobHash::from_hex(&entry.hash)
                    .map_err(|e| SdkError::Manifest(format!("bad member hash: {e}")))?;
                let fetched = session
                    .fetch(member, FetchOutput::Exact(target.clone()))
                    .await?;
                if fetched.size != entry.size {
                    return Err(SdkError::Manifest(format!(
                        "member {} is {} bytes, manifest says {}",
                        entry.path, fetched.size, entry.size
                    )));
                }
                written.push(fetched.path.unwrap_or(target));
                on_member(MemberProgress {
                    index: written.len(),
                    total: manifest.entries.len(),
                    path: &entry.path,
                    collection: &manifest.name,
                });
            }
            Ok(written)
        }
        None => {
            let exported = session
                .fetch(hash, FetchOutput::Directory(dest.into()))
                .await?;
            Ok(exported.path.into_iter().collect())
        }
    }
}

/// Read a manifest for `hash` if the blob is one.
///
/// Trusts the media type when the offer is known, and otherwise sniffs the
/// bytes for the self-describing marker.
async fn read_manifest(
    session: &DropSession,
    hash: BlobHash,
    size: u64,
) -> Result<Option<Manifest>> {
    let advertised = session
        .offers()
        .into_iter()
        .find(|record| record.offer.blob_hash == hash)
        .and_then(|record| record.offer.media_type.clone());
    let is_collection_type = advertised.as_deref() == Some(COLLECTION_MEDIA_TYPE);

    if size > MAX_MANIFEST_BYTES {
        if is_collection_type {
            return Err(SdkError::Manifest(format!(
                "manifest of {size} bytes exceeds the {MAX_MANIFEST_BYTES} byte limit"
            )));
        }
        return Ok(None);
    }

    let bytes = session.read_bytes(hash, MAX_MANIFEST_BYTES).await?;

    match serde_json::from_slice::<Manifest>(&bytes) {
        Ok(manifest) if manifest.iroh_drop_collection == 1 => {
            if manifest.entries.len() > MAX_MEMBERS {
                return Err(SdkError::Manifest(format!(
                    "manifest lists {} members, over the {MAX_MEMBERS} limit",
                    manifest.entries.len()
                )));
            }
            Ok(Some(manifest))
        }
        Ok(manifest) => Err(SdkError::Manifest(format!(
            "unsupported collection version {}",
            manifest.iroh_drop_collection
        ))),
        Err(e) if is_collection_type => Err(SdkError::Manifest(e.to_string())),
        Err(_) => Ok(None),
    }
}

/// Recursively collect regular files as paths relative to `root`.
///
/// Dot-entries are skipped (no `.git` in your share) and symlinks are not
/// followed, so a share cannot escape the directory it names.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.is_dir() {
            collect_files(root, &path, out)?;
        } else if meta.is_file() {
            let rel = path
                .strip_prefix(root)
                .map_err(|e| SdkError::Io(e.to_string()))?;
            out.push(rel.to_path_buf());
        }
    }
    Ok(())
}

/// Validate one path component from untrusted metadata.
fn sanitize_component(name: &str) -> Result<String> {
    if name.is_empty() || name.len() > 255 || name.contains('/') || name.contains('\\') {
        return Err(SdkError::Manifest(format!("unsafe name {name:?}")));
    }
    if name == "." || name == ".." {
        return Err(SdkError::Manifest(format!("unsafe name {name:?}")));
    }
    Ok(name.to_string())
}

/// Validate an untrusted relative path from a manifest: no absolute paths,
/// no `..`, no prefixes, no empty components.
fn sanitize_relative(path: &str) -> Result<PathBuf> {
    if path.len() > 1024 {
        return Err(SdkError::Manifest("member path too long".into()));
    }
    let candidate = PathBuf::from(path);
    let mut safe = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => {
                let part = part
                    .to_str()
                    .ok_or_else(|| SdkError::Manifest("non-utf8 member path".into()))?;
                safe.push(sanitize_component(part)?);
            }
            _ => return Err(SdkError::Manifest(format!("unsafe member path {path:?}"))),
        }
    }
    if safe.as_os_str().is_empty() {
        return Err(SdkError::Manifest("empty member path".into()));
    }
    Ok(safe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal() {
        for bad in ["../escape", "/etc/passwd", "a/../../b", "", "."] {
            assert!(
                sanitize_relative(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn accepts_nested_relative_paths() {
        let ok = sanitize_relative("docs/img/logo.png").unwrap();
        assert_eq!(ok, PathBuf::from("docs/img/logo.png"));
    }
}
