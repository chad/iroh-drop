mod common;
use bytes::Bytes;
use common::*;
use iroh_drop::DropPolicy;
use iroh_drop::FetchOutput;
use iroh_drop::LocalBlobStatus;

#[tokio::test]
async fn oversized_offer_recorded_but_not_auto_fetched() {
    let bus = MemBus::new();
    let proto_a = mem_protocol(&bus, DropPolicy::default()).await;
    let session_a = proto_a.create(Default::default()).await.unwrap();

    let proto_b = mem_protocol(
        &bus,
        DropPolicy {
            auto_fetch: true,
            max_blob_size: 16,
            ..Default::default()
        },
    )
    .await;
    let session_b = proto_b
        .join(ticket_for(&session_a, proto_a.stack().addr()))
        .await
        .unwrap();
    // Subscribed before anything publishes, so no event races past us.
    let mut events_b = session_b.subscribe();

    let published = session_a
        .publish_bytes("big.bin".into(), Bytes::from(vec![7u8; 1024]))
        .await
        .unwrap();

    // The offer is visible...
    wait_event(&mut events_b, "offer", |ev| {
        matches!(ev, DropEvent::OfferReceived { .. })
    })
    .await;
    // ...but policy-rejected for auto-fetch.
    wait_event(&mut events_b, "policy rejection", |ev| {
        matches!(ev, DropEvent::OfferRejected { reason, .. } if reason.to_string().contains("policy"))
    })
    .await;

    // Recorded as missing; a manual fetch still works.
    assert_eq!(session_b.offers()[0].local_status, LocalBlobStatus::Missing);
    session_b
        .fetch(published.hash, FetchOutput::Store)
        .await
        .unwrap();
    assert_eq!(
        session_b.offers()[0].local_status,
        LocalBlobStatus::Complete
    );
}

/// An instance that cannot refuse new sessions is a memory-exhaustion
/// vector one topic at a time. The cap refuses loudly.
#[tokio::test]
async fn session_cap_is_enforced() {
    let bus = common::mem_transport::MemBus::new();
    let proto = common::mem_protocol(&bus, DropPolicy::default()).await;
    let mut sessions = Vec::new();
    for _ in 0..iroh_drop::DropProtocol::MAX_SESSIONS {
        sessions.push(proto.create(Default::default()).await.unwrap());
    }
    match proto.create(Default::default()).await {
        Err(iroh_drop::DropError::Policy(iroh_drop::PolicyError::TooManySessions {
            active,
            max,
        })) => {
            assert_eq!(active, max);
        }
        other => panic!(
            "expected TooManySessions, got {}",
            other
                .map(|_| "a session")
                .unwrap_or_else(|e| Box::leak(e.to_string().into_boxed_str()))
        ),
    }
    drop(sessions);
}
