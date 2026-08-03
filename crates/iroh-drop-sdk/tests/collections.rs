//! A directory goes in one side and comes out the other, as one offer.

use std::time::Duration;

use iroh_drop::builder::{DropBuilder, DropProtocol, StackOptions};
use iroh_drop::policy::DropPolicy;
use iroh_drop::ticket::DropTicket;
use iroh_drop_sdk::collections::{fetch_any, publish_path, COLLECTION_MEDIA_TYPE};
use iroh_drop_sdk::inventory::inventory;

const TIMEOUT: Duration = Duration::from_secs(30);

async fn peer() -> DropProtocol {
    DropBuilder::from_options(StackOptions {
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .policy(DropPolicy::default())
    .build()
    .await
    .unwrap()
}

#[tokio::test]
async fn directory_roundtrip_is_a_single_offer() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("project");
    std::fs::create_dir_all(src.join("docs/img")).unwrap();
    std::fs::write(src.join("README.md"), b"# hello\n").unwrap();
    std::fs::write(src.join("docs/guide.md"), b"read me second\n").unwrap();
    std::fs::write(src.join("docs/img/logo.bin"), vec![7u8; 4096]).unwrap();
    // Hidden files are skipped: no .git in your share.
    std::fs::create_dir_all(src.join(".git")).unwrap();
    std::fs::write(src.join(".git/config"), b"secret").unwrap();

    let proto_a = peer().await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let published = publish_path(&session_a, &src, None).await.unwrap();
    assert_eq!(published.blob.name, "project");
    assert_eq!(published.members, 3, "hidden files are not shared");
    assert_eq!(published.total_size, 8 + 15 + 4096);

    // One offer for the whole tree.
    let items = inventory(&session_a);
    assert_eq!(items.len(), 1, "a collection announces exactly one offer");
    assert!(items[0].is_collection);
    assert_eq!(items[0].media_type.as_deref(), Some(COLLECTION_MEDIA_TYPE));

    // B joins *after* publishing and relies on catch-up sync.
    let proto_b = peer().await;
    let ticket = DropTicket::new(
        *session_a.topic_id().as_bytes(),
        vec![proto_a.stack().addr()],
        Default::default(),
    );
    let session_b = proto_b.join(ticket).await.unwrap();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let hash = loop {
        let items = inventory(&session_b);
        if let Some(item) = items.first() {
            assert!(item.is_collection, "collection type survives the wire");
            // The receiver knows the shape of the tree before fetching it.
            assert_eq!(item.members, Some(3));
            assert_eq!(item.content_size, 8 + 15 + 4096);
            assert_eq!(item.kind(), "folder, 3 files");
            break item.hash;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "B never learned the collection"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let out = dir.path().join("out");
    let written = fetch_any(&session_b, hash, &out).await.unwrap();
    assert_eq!(written.len(), 3, "three members, hidden files excluded");

    let root = out.join("project");
    assert_eq!(
        std::fs::read(root.join("README.md")).unwrap(),
        b"# hello\n".to_vec()
    );
    assert_eq!(
        std::fs::read(root.join("docs/guide.md")).unwrap(),
        b"read me second\n".to_vec()
    );
    assert_eq!(
        std::fs::read(root.join("docs/img/logo.bin")).unwrap(),
        vec![7u8; 4096]
    );
    assert!(!root.join(".git").exists(), "hidden entries stay home");

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    proto_b.shutdown().await.ok();
    proto_a.shutdown().await.ok();
}

/// A plain file still works through the same entry point.
#[tokio::test]
async fn single_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("note.txt");
    std::fs::write(&file, b"just a file").unwrap();

    let proto_a = peer().await;
    let session_a = proto_a.create(Default::default()).await.unwrap();
    let published = publish_path(&session_a, &file, None).await.unwrap();
    assert_eq!(published.blob.name, "note.txt");

    let out = dir.path().join("out");
    let written = fetch_any(&session_a, published.blob.hash, &out)
        .await
        .unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(std::fs::read(&written[0]).unwrap(), b"just a file".to_vec());

    drop(session_a);
    proto_a.shutdown().await.ok();
}
