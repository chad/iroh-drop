//! Deterministic golden-vector builders for the conformance suite.
//!
//! Every value here is fixed — author key, message ids, timestamps, hashes,
//! addresses — so the encoded bytes are stable forever. The committed bytes
//! in `tests/fixtures/` are the source of truth; these builders exist to
//! regenerate them after a *deliberate* wire change and to prove the
//! generator still produces the committed bytes.
//!
//! If a wire change is deliberate, re-bless and review the fixture diff like
//! the wire-format change it is:
//!
//! ```sh
//! IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance   # regenerate
//! cargo test -p iroh-drop conformance                     # verify
//! ```

use std::path::PathBuf;
use std::str::FromStr;

use iroh::{EndpointAddr, RelayUrl, SecretKey};
use iroh_drop::message::{
    BodyEnvelopeV1, MessageBodyV1, MessageV1, OfferV1, ProviderState, ProviderV1, RequestV1,
    WIRE_VERSION,
};
use iroh_drop::{BlobHash, DropTicket, DropTicketOptionsV1};

/// The fixed author key for every signed fixture.
pub fn fixture_topic() -> iroh_drop::TopicId {
    iroh_drop::TopicId::from_bytes([0xC1; 32])
}

pub fn author_key() -> SecretKey {
    SecretKey::from_bytes(&[0xA5; 32])
}

/// Fixed `sent_at_ms` for every fixture message (2025-07-22, give or take —
/// the value only has to be constant).
pub const SENT_AT_MS: u64 = 1_752_700_000_000;

/// The blob every fixture talks about.
pub fn blob_hash() -> BlobHash {
    BlobHash::from_bytes([0x42; 32])
}

/// The drop topic every fixture belongs to.
pub const TOPIC_ID: [u8; 32] = [0x11; 32];

/// An extension kind this crate does not implement (`1000..=1999` is the
/// experimental range). Foreshadows a presence extension; the payload is
/// deliberately meaningful JSON-in-text, though the protocol treats it as
/// opaque bytes.
pub const UNKNOWN_KIND: u16 = 1500;
pub const UNKNOWN_KIND_PAYLOAD: &[u8] = b"presence:v1:{\"status\":\"online\"}";

fn message(id: u8, body: MessageBodyV1) -> MessageV1 {
    MessageV1 {
        version: WIRE_VERSION,
        id: [id; 16],
        sent_at_ms: SENT_AT_MS,
        body: BodyEnvelopeV1::encode(&body).expect("core bodies always encode"),
    }
}

/// The fixture offer: every optional field set, so the vector exercises the
/// whole schema.
pub fn offer() -> OfferV1 {
    OfferV1 {
        blob_hash: blob_hash(),
        name: "quarterly-report.pdf".into(),
        size: 4_194_304,
        media_type: Some("application/pdf".into()),
        created_at_ms: Some(1_752_600_000_000),
        metadata: std::collections::BTreeMap::from([
            ("files".to_string(), "12".to_string()),
            ("project".to_string(), "apollo".to_string()),
        ]),
    }
}

/// `offer_full.bin` — a signed offer with all optional fields populated.
pub fn offer_frame() -> Vec<u8> {
    message(0x0F, MessageBodyV1::Offer(offer()))
        .encode(&author_key(), &fixture_topic())
        .unwrap()
}

/// `provider_available.bin` — signed `Available` with a self-asserted clock.
pub fn provider_available_frame() -> Vec<u8> {
    message(
        0xA0,
        MessageBodyV1::Provider(ProviderV1 {
            blob_hash: blob_hash(),
            state: ProviderState::Available,
            announced_at_ms: Some(1_752_700_100_000),
        }),
    )
    .encode(&author_key(), &fixture_topic())
    .unwrap()
}

/// `provider_withdrawing.bin` — signed `Withdrawing`, later clock.
pub fn provider_withdrawing_frame() -> Vec<u8> {
    message(
        0xA1,
        MessageBodyV1::Provider(ProviderV1 {
            blob_hash: blob_hash(),
            state: ProviderState::Withdrawing,
            announced_at_ms: Some(1_752_700_200_000),
        }),
    )
    .encode(&author_key(), &fixture_topic())
    .unwrap()
}

/// `request.bin` — a signed by-hash request.
pub fn request_frame() -> Vec<u8> {
    message(
        0xB0,
        MessageBodyV1::Request(RequestV1 {
            blob_hash: blob_hash(),
        }),
    )
    .encode(&author_key(), &fixture_topic())
    .unwrap()
}

/// `unknown_kind.bin` — a signed frame carrying a kind this build does not
/// implement. Must verify, decode to no body, and stay relayable verbatim.
pub fn unknown_kind_frame() -> Vec<u8> {
    MessageV1 {
        version: WIRE_VERSION,
        id: [0x15; 16],
        sent_at_ms: SENT_AT_MS,
        body: BodyEnvelopeV1 {
            kind: UNKNOWN_KIND,
            payload: UNKNOWN_KIND_PAYLOAD.to_vec(),
        },
    }
    .encode(&author_key(), &fixture_topic())
    .unwrap()
}

/// `trailing_additive.bin` — the offer frame with additive trailing bytes,
/// which decoders must tolerate (postcard `take_from_bytes` semantics).
pub fn trailing_additive_frame() -> Vec<u8> {
    let mut bytes = offer_frame();
    bytes.extend_from_slice(&[9, 9, 9]);
    bytes
}

/// `reject_wrong_version.bin` — validly signed, but version 99. Decoders
/// verify the signature first and then reject on the version, which is what
/// makes version mismatches observable rather than silent.
pub fn wrong_version_frame() -> Vec<u8> {
    let mut msg = message(0x0F, MessageBodyV1::Offer(offer()));
    msg.version = 99;
    msg.encode(&author_key(), &fixture_topic()).unwrap()
}

/// `reject_bad_signature.bin` — the offer frame with one payload byte
/// flipped after signing.
pub fn bad_signature_frame() -> Vec<u8> {
    let mut bytes = offer_frame();
    let n = bytes.len();
    bytes[n - 2] ^= 0xff;
    bytes
}

/// `reject_invalid_name.bin` — encodes fine (validation is a decode-side
/// duty), rejected for a 300-byte name.
pub fn invalid_name_frame() -> Vec<u8> {
    let mut offer = offer();
    offer.name = "x".repeat(300);
    message(0x1D, MessageBodyV1::Offer(offer))
        .encode(&author_key(), &fixture_topic())
        .unwrap()
}

/// `reject_metadata_limit.bin` — 17 metadata entries, one over the cap.
pub fn metadata_limit_frame() -> Vec<u8> {
    let mut offer = offer();
    offer.metadata = (0..17)
        .map(|i| (format!("key-{i:02}"), "value".to_string()))
        .collect();
    message(0x1E, MessageBodyV1::Offer(offer))
        .encode(&author_key(), &fixture_topic())
        .unwrap()
}

/// `ticket_full.txt` — a ticket carrying a full address (relay + socket)
/// and an id-only peer, with both options set.
pub fn ticket_full() -> DropTicket {
    let full = EndpointAddr::new(SecretKey::from_bytes(&[0x0B; 32]).public())
        .with_relay_url(RelayUrl::from_str("https://relay1.example.com/").unwrap())
        .with_ip_addr("203.0.113.7:4321".parse().unwrap());
    let id_only = EndpointAddr::new(SecretKey::from_bytes(&[0x0C; 32]).public());
    DropTicket::new(
        TOPIC_ID,
        vec![full, id_only],
        DropTicketOptionsV1 {
            auto_fetch_recommended: true,
            display_name: Some("apollo drop".into()),
        },
    )
}

/// `ticket_short.txt` — the same drop with id-only bootstrap entries.
pub fn ticket_short() -> DropTicket {
    let mut ticket = ticket_full();
    let nodes: Vec<EndpointAddr> = ticket
        .bootstrap_nodes()
        .iter()
        .map(|addr| EndpointAddr::new(addr.id))
        .collect();
    ticket.set_bootstrap_nodes(nodes);
    ticket
}

// ---------------------------------------------------------------------------
// Fixture file I/O (bless pattern)

/// The committed golden vectors.
pub fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Whether fixtures should be regenerated instead of checked.
pub fn bless_enabled() -> bool {
    std::env::var_os("IROH_DROP_BLESS").is_some()
}

/// Write a fixture (bless mode).
pub fn write_fixture(name: &str, bytes: &[u8]) {
    std::fs::create_dir_all(fixture_dir()).unwrap();
    std::fs::write(fixture_dir().join(name), bytes).unwrap();
}

/// Read a committed fixture, with a helpful message when it is missing.
pub fn read_fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing fixture {} ({e}) — generate the golden vectors with:\n  \
             IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance",
            path.display()
        )
    })
}

/// In bless mode, (re)write the fixture; otherwise assert the committed
/// bytes match exactly what this build produces.
pub fn check_or_bless(name: &str, bytes: &[u8]) {
    if bless_enabled() {
        write_fixture(name, bytes);
        return;
    }
    let committed = read_fixture(name);
    assert_eq!(
        &committed, bytes,
        "fixture {name} does not match the wire format this build produces. \
         If the format change was deliberate: \
         IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance, then review \
         the fixture diff like the wire-format change it is."
    );
}
