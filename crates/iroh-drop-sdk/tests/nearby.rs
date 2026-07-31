//! Advertising a drop on the local network, and finding it again.
//!
//! This exercises real mDNS on the loopback/LAN interfaces, so it is ignored
//! by default: sandboxes and CI often block multicast. Run it with:
//!
//! ```sh
//! cargo test -p iroh-drop-sdk --test nearby -- --ignored --nocapture
//! ```

use std::time::Duration;

use iroh_drop::builder::{DropBuilder, DropProtocol, StackOptions};
use iroh_drop::policy::DropPolicy;
use iroh_drop::ticket::DropTicketOptionsV1;
use iroh_drop_sdk::nearby;

async fn peer() -> DropProtocol {
    DropBuilder::from_options(StackOptions {
        offline: true,
        mdns: true,
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
#[ignore = "requires working mDNS multicast on this machine"]
async fn advertised_drops_are_discoverable() {
    let sharer = peer().await;
    let session = sharer
        .create(iroh_drop::CreateOptions {
            display_name: Some("test share".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    // A short ticket is what fits in a discovery record.
    let ticket = session.short_ticket();
    assert!(
        ticket.to_string().len() <= 245,
        "advertisements must fit in a discovery record"
    );
    nearby::advertise(sharer.stack(), &ticket).unwrap();

    let seeker = peer().await;
    let found = nearby::browse(seeker.stack(), Duration::from_secs(10))
        .await
        .unwrap();

    let ours = found
        .iter()
        .find(|drop| drop.peer == session.self_id())
        .expect("the advertised drop should be discoverable");
    assert_eq!(ours.label.as_deref(), Some("test share"));
    assert_eq!(ours.ticket.topic_id(), ticket.topic_id());
    assert_eq!(ours.display(), "test share");

    // Joining what we found works without anyone typing a ticket.
    let joined = seeker.join(ours.ticket.clone()).await.unwrap();
    assert_eq!(joined.topic_id(), session.topic_id());

    nearby::stop_advertising(sharer.stack()).unwrap();
    joined.shutdown_no_announce().await;
    drop(joined);
    drop(session);
    seeker.shutdown().await.ok();
    sharer.shutdown().await.ok();
}

/// Browsing without mDNS enabled is a clear error, not a silent empty list.
#[tokio::test]
async fn browsing_without_mdns_explains_itself() {
    let protocol = DropBuilder::from_options(StackOptions {
        offline: true,
        mdns: false,
        ..Default::default()
    })
    .await
    .unwrap()
    .build()
    .await
    .unwrap();

    let err = nearby::browse(protocol.stack(), Duration::from_millis(50))
        .await
        .expect_err("should refuse to browse without local discovery");
    assert!(err.to_string().contains("mdns"), "got: {err}");

    // Advertising also needs somewhere to advertise, but it is allowed to be
    // a no-op online (pkarr carries user data too).
    let _ = DropTicketOptionsV1::default();
    protocol.shutdown().await.ok();
}
