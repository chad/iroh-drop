//! Does a ticket need socket addresses? Not online.
//!
//! The `N0` preset publishes each endpoint's addresses to n0's pkarr/DNS
//! service and resolves them the same way, so an `EndpointAddr` carrying only
//! an `EndpointId` is dialable. That is what makes a short ticket possible:
//! `topic_id + endpoint_id` instead of `topic_id + full address set`.
//!
//! Ignored by default because it needs the internet. Run with:
//!
//! ```sh
//! cargo test -p iroh-drop --test online_short_ticket -- --ignored --nocapture
//! ```

use std::time::Duration;

use bytes::Bytes;
use iroh::EndpointAddr;
use iroh_drop::builder::{DropBuilder, StackOptions};
use iroh_drop::policy::DropPolicy;
use iroh_drop::session::FetchOutput;
use iroh_drop::ticket::DropTicket;

#[tokio::test]
#[ignore = "requires internet: uses n0 relays and pkarr/DNS address lookup"]
async fn id_only_ticket_is_enough_online() {
    let publisher = DropBuilder::from_options(StackOptions::default())
        .await
        .unwrap()
        .policy(DropPolicy::default())
        .build()
        .await
        .unwrap();
    // Wait until our addresses are actually published, or the joiner has
    // nothing to resolve.
    publisher.stack().wait_online().await;

    let session_a = publisher.create(Default::default()).await.unwrap();
    let published = session_a
        .publish_bytes(
            "short.txt".into(),
            Bytes::from_static(b"no addresses needed"),
        )
        .await
        .unwrap();

    // The whole ticket: a topic and an endpoint id. No IPs, no relay URL.
    let ticket = DropTicket::new(
        *session_a.topic_id().as_bytes(),
        vec![EndpointAddr::from(publisher.stack().addr().id)],
        Default::default(),
    );
    let encoded = ticket.to_string();
    println!("id-only ticket ({} chars): {encoded}", encoded.len());
    assert!(
        ticket.bootstrap_nodes()[0].addrs.is_empty(),
        "this test is only meaningful with an address-free bootstrap entry"
    );

    let joiner = DropBuilder::from_options(StackOptions::default())
        .await
        .unwrap()
        .policy(DropPolicy::default())
        .build()
        .await
        .unwrap();
    let session_b = joiner
        .join(encoded.parse().unwrap())
        .await
        .expect("join with an id-only ticket");

    // Catch-up sync has to dial the publisher by id for this to work.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if session_b
            .offers()
            .iter()
            .any(|record| record.offer.name == "short.txt")
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no inventory: an id-only ticket did not resolve"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let result = session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(result.size, 19);
    println!("fetched {} bytes from an id-only ticket", result.size);

    session_b.shutdown_no_announce().await;
    drop(session_b);
    drop(session_a);
    joiner.shutdown().await.ok();
    publisher.shutdown().await.ok();
}
