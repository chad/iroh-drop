//! Golden conformance vectors: the wire format, frozen in bytes.
//!
//! The committed files in `tests/fixtures/` are the normative examples of
//! the wire format. This suite proves three things:
//!
//! 1. **The generator is honest** — the bytes this build produces are
//!    exactly the committed bytes (`*_match_committed_bytes`).
//! 2. **The format is frozen** — committed bytes decode to exact field
//!    values, and every must-reject vector fails with the right error.
//! 3. **Signatures actually gate** — flipping any single byte of a signed
//!    frame is fatal, with the one documented exception of the additive
//!    tail (unsigned by design, tolerated by design).
//!
//! The control-channel (sync) vectors live in a `conformance` module inside
//! `src/sync.rs`, because those envelope types are deliberately crate-private.
//! One command runs both:
//!
//! ```sh
//! cargo test -p iroh-drop conformance
//! ```

mod common;

use common::fixtures as fx;

use iroh::TransportAddr;
use iroh_drop::message::{MessageBodyV1, MessageV1, KIND_OFFER, KIND_PROVIDER, KIND_REQUEST};
use iroh_drop::{DropTicket, ProtocolError, ProviderState, WIRE_VERSION};

/// Bless-mode entry point shared by every test in this file, so test
/// execution order never matters.
fn bless_all() {
    if !fx::bless_enabled() {
        return;
    }
    fx::write_fixture("offer_full.bin", &fx::offer_frame());
    fx::write_fixture("provider_available.bin", &fx::provider_available_frame());
    fx::write_fixture(
        "provider_withdrawing.bin",
        &fx::provider_withdrawing_frame(),
    );
    fx::write_fixture("request.bin", &fx::request_frame());
    fx::write_fixture("unknown_kind.bin", &fx::unknown_kind_frame());
    fx::write_fixture("trailing_additive.bin", &fx::trailing_additive_frame());
    fx::write_fixture("reject_wrong_version.bin", &fx::wrong_version_frame());
    fx::write_fixture("reject_bad_signature.bin", &fx::bad_signature_frame());
    fx::write_fixture("reject_invalid_name.bin", &fx::invalid_name_frame());
    fx::write_fixture("reject_metadata_limit.bin", &fx::metadata_limit_frame());
    fx::write_fixture("ticket_full.txt", fx::ticket_full().to_string().as_bytes());
    fx::write_fixture(
        "ticket_short.txt",
        fx::ticket_short().to_string().as_bytes(),
    );
}

#[test]
fn conformance_frames_match_committed_bytes() {
    fx::check_or_bless("offer_full.bin", &fx::offer_frame());
    fx::check_or_bless("provider_available.bin", &fx::provider_available_frame());
    fx::check_or_bless(
        "provider_withdrawing.bin",
        &fx::provider_withdrawing_frame(),
    );
    fx::check_or_bless("request.bin", &fx::request_frame());
    fx::check_or_bless("unknown_kind.bin", &fx::unknown_kind_frame());
    fx::check_or_bless("trailing_additive.bin", &fx::trailing_additive_frame());
    fx::check_or_bless("reject_wrong_version.bin", &fx::wrong_version_frame());
    fx::check_or_bless("reject_bad_signature.bin", &fx::bad_signature_frame());
    fx::check_or_bless("reject_invalid_name.bin", &fx::invalid_name_frame());
    fx::check_or_bless("reject_metadata_limit.bin", &fx::metadata_limit_frame());
    fx::check_or_bless("ticket_full.txt", fx::ticket_full().to_string().as_bytes());
    fx::check_or_bless(
        "ticket_short.txt",
        fx::ticket_short().to_string().as_bytes(),
    );
}

#[test]
fn conformance_frames_decode_to_exact_values() {
    bless_all();

    // The offer: every field, exactly.
    let verified =
        MessageV1::decode(&fx::read_fixture("offer_full.bin"), &fx::fixture_topic()).unwrap();
    assert_eq!(verified.author, fx::author_key().public());
    assert_eq!(verified.message.version, WIRE_VERSION);
    assert_eq!(verified.message.id, [0x0F; 16]);
    assert_eq!(verified.message.sent_at_ms, fx::SENT_AT_MS);
    assert_eq!(verified.message.body.kind, KIND_OFFER);
    let Some(MessageBodyV1::Offer(offer)) = &verified.body else {
        panic!("offer fixture decoded to the wrong body");
    };
    assert_eq!(offer.blob_hash, fx::blob_hash());
    assert_eq!(offer.name, "quarterly-report.pdf");
    assert_eq!(offer.size, 4_194_304);
    assert_eq!(offer.media_type.as_deref(), Some("application/pdf"));
    assert_eq!(offer.created_at_ms, Some(1_752_600_000_000));
    assert_eq!(offer.metadata.len(), 2);
    assert_eq!(offer.metadata.get("files").map(String::as_str), Some("12"));
    assert_eq!(
        offer.metadata.get("project").map(String::as_str),
        Some("apollo")
    );

    // Provider announcements: state and self-asserted clock, exactly.
    let available = MessageV1::decode(
        &fx::read_fixture("provider_available.bin"),
        &fx::fixture_topic(),
    )
    .unwrap();
    assert_eq!(available.message.body.kind, KIND_PROVIDER);
    let Some(MessageBodyV1::Provider(p)) = &available.body else {
        panic!("provider fixture decoded to the wrong body");
    };
    assert_eq!(p.blob_hash, fx::blob_hash());
    assert_eq!(p.state, ProviderState::Available);
    assert_eq!(p.announced_at_ms, Some(1_752_700_100_000));

    let withdrawing = MessageV1::decode(
        &fx::read_fixture("provider_withdrawing.bin"),
        &fx::fixture_topic(),
    )
    .unwrap();
    let Some(MessageBodyV1::Provider(p)) = &withdrawing.body else {
        panic!("provider fixture decoded to the wrong body");
    };
    assert_eq!(p.state, ProviderState::Withdrawing);
    assert_eq!(p.announced_at_ms, Some(1_752_700_200_000));

    // The request.
    let request =
        MessageV1::decode(&fx::read_fixture("request.bin"), &fx::fixture_topic()).unwrap();
    assert_eq!(request.message.body.kind, KIND_REQUEST);
    let Some(MessageBodyV1::Request(r)) = &request.body else {
        panic!("request fixture decoded to the wrong body");
    };
    assert_eq!(r.blob_hash, fx::blob_hash());

    // The unknown kind: verifies, yields no body, payload intact for relay.
    let unknown =
        MessageV1::decode(&fx::read_fixture("unknown_kind.bin"), &fx::fixture_topic()).unwrap();
    assert_eq!(unknown.author, fx::author_key().public());
    assert_eq!(unknown.message.body.kind, fx::UNKNOWN_KIND);
    assert!(unknown.body.is_none());
    assert_eq!(unknown.message.body.payload, fx::UNKNOWN_KIND_PAYLOAD);

    // The additive tail: tolerated, and decodes to the same message as the
    // canonical offer frame.
    let trailing = MessageV1::decode(
        &fx::read_fixture("trailing_additive.bin"),
        &fx::fixture_topic(),
    )
    .unwrap();
    assert_eq!(trailing.message.id, [0x0F; 16]);
    let Some(MessageBodyV1::Offer(offer)) = &trailing.body else {
        panic!("trailing-bytes fixture decoded to the wrong body");
    };
    assert_eq!(offer.name, "quarterly-report.pdf");
}

#[test]
fn conformance_reject_fixtures_fail_with_the_right_error() {
    bless_all();

    let err = MessageV1::decode(
        &fx::read_fixture("reject_wrong_version.bin"),
        &fx::fixture_topic(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ProtocolError::UnsupportedVersion(99)),
        "wrong version must be observable, got {err:?}"
    );

    let err = MessageV1::decode(
        &fx::read_fixture("reject_bad_signature.bin"),
        &fx::fixture_topic(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidSignature),
        "a flipped payload byte must fail verification, got {err:?}"
    );

    let err = MessageV1::decode(
        &fx::read_fixture("reject_invalid_name.bin"),
        &fx::fixture_topic(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ProtocolError::InvalidName(_)),
        "a 300-byte name must be rejected, got {err:?}"
    );

    let err = MessageV1::decode(
        &fx::read_fixture("reject_metadata_limit.bin"),
        &fx::fixture_topic(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ProtocolError::MetadataLimit(_)),
        "17 metadata entries must be rejected, got {err:?}"
    );
}

/// The single most important property of a signed frame: no bit of it can
/// be flipped in transit without detection — with one documented exception,
/// the additive tail, which is unsigned and tolerated *by design* (that is
/// what makes additive evolution possible within a major version).
#[test]
fn conformance_every_byte_flip_is_fatal() {
    bless_all();

    for name in [
        "offer_full.bin",
        "provider_available.bin",
        "provider_withdrawing.bin",
        "request.bin",
        "unknown_kind.bin",
    ] {
        let original = fx::read_fixture(name);
        for i in 0..original.len() {
            let mut mutated = original.clone();
            mutated[i] ^= 0x01;
            assert!(
                MessageV1::decode(&mutated, &fx::fixture_topic()).is_err(),
                "{name}: flipping byte {i} was not detected"
            );
        }
    }

    // The exception, pinned down: bytes in the additive tail may change and
    // the frame still decodes — they carry no signed meaning today.
    let trailing = fx::read_fixture("trailing_additive.bin");
    let n = trailing.len();
    for i in (n - 3)..n {
        let mut mutated = trailing.clone();
        mutated[i] ^= 0x01;
        assert!(MessageV1::decode(&mutated, &fx::fixture_topic()).is_ok());
    }
}

#[test]
fn conformance_tickets_decode_to_exact_values() {
    bless_all();

    let full_text = String::from_utf8(fx::read_fixture("ticket_full.txt")).unwrap();
    assert!(full_text.starts_with("drop2"));
    let full = DropTicket::from_string_prefixed(&full_text).unwrap();
    assert_eq!(full.version(), iroh_drop::ticket::TICKET_SCHEMA_VERSION);
    assert_eq!(full.topic_id(), fx::TOPIC_ID);
    assert_eq!(full.bootstrap_nodes().len(), 2);
    let first = &full.bootstrap_nodes()[0];
    assert!(
        first
            .addrs
            .iter()
            .any(|a| matches!(a, TransportAddr::Relay(_))),
        "full ticket's first node must carry a relay address"
    );
    assert!(
        first
            .addrs
            .iter()
            .any(|a| matches!(a, TransportAddr::Ip(_))),
        "full ticket's first node must carry a socket address"
    );
    assert!(full.bootstrap_nodes()[1].is_empty());
    assert_eq!(full.options().display_name.as_deref(), Some("apollo drop"));
    assert!(full.options().auto_fetch_recommended);

    let short_text = String::from_utf8(fx::read_fixture("ticket_short.txt")).unwrap();
    let short = DropTicket::from_string_prefixed(&short_text).unwrap();
    assert_eq!(short.topic_id(), fx::TOPIC_ID);
    assert_eq!(short.bootstrap_nodes().len(), 2);
    assert!(short.bootstrap_nodes().iter().all(|a| a.is_empty()));
    assert!(
        short_text.len() < full_text.len(),
        "short tickets exist to be shorter"
    );
}
