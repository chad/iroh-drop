mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::FetchOutput;
use iroh_drop::{DropBuilder, DropPolicy, StackOptions};

#[tokio::test]
async fn persisted_store_serves_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("blobs");

    // First process lifetime: import a blob into a persistent store.
    let proto1 = DropBuilder::from_options(StackOptions {
        store_path: Some(store_path.clone()),
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    let session1 = proto1.create(Default::default()).await.unwrap();
    let payload = b"persist me across restarts".to_vec();
    let published = session1
        .publish_bytes("persist.bin".into(), Bytes::from(payload.clone()))
        .await
        .unwrap();
    session1.shutdown_no_announce().await;
    drop(session1);
    proto1.shutdown().await.unwrap();

    // Second process lifetime: same store, no in-memory offer state.
    let proto2 = DropBuilder::from_options(StackOptions {
        store_path: Some(store_path.clone()),
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    let session2 = proto2.create(Default::default()).await.unwrap();

    let proto_b = protocol(DropPolicy::default()).await;
    let session_b = proto_b
        .join(ticket_for(&session2, proto2.stack().addr()))
        .await
        .unwrap();

    // B requests the hash; the restarted peer answers from its persistent
    // store even though its in-memory offer index is empty.
    let result = session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(result.provider, Some(session2.self_id()));
    assert_eq!(result.size, payload.len() as u64);
}

/// A configured identity file makes a peer recognizable across restarts —
/// without one, every process is a stranger.
#[tokio::test]
async fn identity_file_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let identity = dir.path().join("nested/identity.key");

    let proto1 = DropBuilder::from_options(StackOptions {
        offline: true,
        identity_path: Some(identity.clone()),
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    let first = proto1.stack().addr().id;
    proto1.shutdown().await.unwrap();

    assert!(identity.exists(), "the identity file is created on demand");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&identity).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "secret keys are owner-only");
    }

    let proto2 = DropBuilder::from_options(StackOptions {
        offline: true,
        identity_path: Some(identity.clone()),
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    assert_eq!(proto2.stack().addr().id, first, "same identity, same peer");
    proto2.shutdown().await.unwrap();

    // Without an identity file, each process is a different peer.
    let proto3 = DropBuilder::from_options(StackOptions {
        offline: true,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();
    assert_ne!(proto3.stack().addr().id, first);
    proto3.shutdown().await.unwrap();
}
