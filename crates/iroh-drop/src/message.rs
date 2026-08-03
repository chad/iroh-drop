//! Wire messages broadcast through a drop's gossip topic.
//!
//! Every message is signed by its author. The gossip layer only exposes the
//! *delivering neighbor* (`delivered_from`), not the original author, so
//! `iroh-drop` signs message payloads with the author's endpoint secret key
//! and verifies them on receipt. The `author` field inside the payload is
//! therefore cryptographically attributable, not merely trusted.
//!
//! Decoding rules (see the spec, §6.2):
//!
//! * unknown major versions are ignored with an observable warning,
//! * additive trailing bytes within a major version are tolerated,
//! * unknown message kinds never terminate a session,
//! * all variable-length fields are bounded.

use n0_future::time::SystemTime;
use std::collections::BTreeMap;

use iroh::{EndpointId, PublicKey, SecretKey, Signature};
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};

use crate::error::ProtocolError;
use crate::hash::BlobHash;

/// The wire major version implemented by this crate.
///
/// Version 2 wraps message bodies in a [`BodyEnvelopeV1`] so that unknown
/// message kinds can be ignored (and relayed) instead of poisoning a whole
/// frame. Body schemas themselves (`OfferV1`, `ProviderV1`, `RequestV1`) are
/// unchanged from version 1.
pub const WIRE_VERSION: u16 = 2;

/// Body kind for [`OfferV1`].
pub const KIND_OFFER: u16 = 1;

/// Body kind for [`ProviderV1`].
pub const KIND_PROVIDER: u16 = 2;

/// Body kind for [`RequestV1`].
pub const KIND_REQUEST: u16 = 3;

/// Frame carrying a namespaced application extension ([`ExtensionV1`]).
///
/// Kind numbers are globally assigned by the core spec; applications must
/// never mint their own. Instead they carry an `ExtensionV1` envelope with
/// a collision-resistant namespace, so two applications can never collide.
/// Frames with *unknown* numeric kinds are still verified, retained, and
/// relayed by stock peers (that is what lets future core kinds deploy),
/// but they are never delivered to extension subscribers.
pub const KIND_EXTENSION: u16 = 5;

/// Highest body kind reserved for this specification. Kinds in `1..=999` are
/// defined here or by future core versions; everything application-specific
/// rides the namespaced [`ExtensionV1`] envelope instead of minting numbers.
/// Unknown kinds must be ignored, never fatal.
pub const MAX_CORE_KIND: u16 = 999;

/// Largest encoded body accepted inside an envelope.
pub const MAX_BODY_SIZE: usize = MAX_MESSAGE_SIZE - 1024;

/// Maximum size of an encoded gossip message, in bytes.
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024;

/// Maximum length of an offer's display name, in UTF-8 bytes.
pub const MAX_NAME_LEN: usize = 255;

/// Maximum number of metadata entries in an offer.
pub const MAX_METADATA_ENTRIES: usize = 16;

/// Maximum length of a metadata key, in UTF-8 bytes.
pub const MAX_METADATA_KEY_LEN: usize = 64;

/// Maximum length of a metadata value, in UTF-8 bytes.
pub const MAX_METADATA_VALUE_LEN: usize = 512;

/// Domain separation prefix for message signatures. Bound to the wire major
/// version, so a frame from one version can never be replayed as another.
pub(crate) const SIGN_DOMAIN: &[u8] = b"iroh-drop/v3/message/";

/// The pre-review signing domain (families 1–2 signed without topic
/// binding). Used only as a diagnostic: when current verification fails we
/// re-verify against the legacy input so an old frame is reported as
/// `UnsupportedVersion` instead of a generic signature error. Never used
/// to accept a frame.
pub(crate) const LEGACY_SIGN_DOMAIN: &[u8] = b"iroh-drop/v2/message/";

/// Signature domain of sealed (family 4) frames. Declared here so the
/// public decoder can *diagnose* a sealed frame as `UnsupportedVersion(4)`
/// — never to accept one.
pub(crate) const SEALED_SIGN_DOMAIN: &[u8] = b"iroh-drop/v4/message/";

/// The bytes an author actually signs: the domain separator, the drop's
/// topic (so a frame valid in one drop is cryptographic junk in another),
/// then the encoded envelope.
pub(crate) fn signing_input(domain: &[u8], topic: &TopicId, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(domain.len() + 32 + payload.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(topic.as_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

/// A message that has been decoded, verified, and validated.
#[derive(Clone, Debug)]
pub struct VerifiedMessage {
    /// The cryptographically verified author of the message.
    pub author: EndpointId,
    /// The message itself (body still in wire form).
    pub message: MessageV1,
    /// The decoded body, or `None` for a kind this build does not know.
    pub body: Option<MessageBodyV1>,
}

/// The version-1 message envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageV1 {
    /// Must equal [`WIRE_VERSION`].
    pub version: u16,
    /// Random identifier used for deduplication.
    pub id: [u8; 16],
    /// Informational sender timestamp in milliseconds since the Unix epoch.
    /// Never used for correctness or canonical ordering.
    pub sent_at_ms: u64,
    /// The message body, wrapped so unknown kinds stay decodable.
    pub body: BodyEnvelopeV1,
}

/// A body as it appears on the wire: a kind tag and opaque bytes.
///
/// Keeping the payload opaque is what makes the protocol extensible: a peer
/// that does not know a kind can still verify the frame's signature, ignore
/// the body, and relay it to peers that do understand it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BodyEnvelopeV1 {
    /// What the payload is. See [`KIND_OFFER`] and friends.
    pub kind: u16,
    /// postcard-encoded body of that kind.
    pub payload: Vec<u8>,
}

impl BodyEnvelopeV1 {
    /// Wrap a known body.
    pub fn encode(body: &MessageBodyV1) -> Result<Self, ProtocolError> {
        let (kind, payload) = match body {
            MessageBodyV1::Offer(offer) => (KIND_OFFER, postcard::to_allocvec(offer)),
            MessageBodyV1::Provider(provider) => (KIND_PROVIDER, postcard::to_allocvec(provider)),
            MessageBodyV1::Request(request) => (KIND_REQUEST, postcard::to_allocvec(request)),
            MessageBodyV1::Extension(ext) => (KIND_EXTENSION, postcard::to_allocvec(ext)),
        };
        let payload = payload.map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        Ok(Self { kind, payload })
    }

    /// Interpret the payload, if the kind is one we know.
    ///
    /// Returns `Ok(None)` for kinds this build does not implement, which is a
    /// normal outcome and not an error.
    pub fn decode(&self) -> Result<Option<MessageBodyV1>, ProtocolError> {
        if self.payload.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(self.payload.len()));
        }
        let body = match self.kind {
            KIND_OFFER => MessageBodyV1::Offer(take(&self.payload, "offer")?),
            KIND_EXTENSION => MessageBodyV1::Extension(take(&self.payload, "extension")?),
            KIND_PROVIDER => MessageBodyV1::Provider(take(&self.payload, "provider")?),
            KIND_REQUEST => MessageBodyV1::Request(take(&self.payload, "request")?),
            _ => return Ok(None),
        };
        Ok(Some(body))
    }
}

/// Decode one body type, tolerating additive trailing fields.
fn take<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T, ProtocolError> {
    postcard::take_from_bytes::<T>(bytes)
        .map(|(value, _rest)| value)
        .map_err(|e| ProtocolError::Malformed(format!("{what}: {e}")))
}

/// The body of a version-1 message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageBodyV1 {
    /// Announce that a blob exists.
    Offer(OfferV1),
    /// Announce that the author serves (or stops serving) a blob.
    Provider(ProviderV1),
    /// Ask the group who serves a blob. Never an authorization decision.
    Request(RequestV1),
    /// A namespaced application extension frame.
    Extension(ExtensionV1),
}

/// The namespaced extension envelope (kind [`KIND_EXTENSION`]).
///
/// Everything application-specific lives behind `namespace`, so extension
/// authors never register numbers with anyone:
///
/// - `namespace`: 16 bytes identifying the application protocol. Derive it
///   from something you own — a UUID, the first 16 bytes of a hash of your
///   protocol's fully-qualified name — and publish it in your spec.
/// - `local_kind`: your protocol's own message number, meaningful only
///   inside the namespace.
/// - `schema_version`: your payload's schema version, so your protocol can
///   evolve without coordination.
///
/// The payload is opaque to `iroh-drop` (still size-capped and signed like
/// every frame). Peers that do not implement the namespace verify, retain,
/// and relay the frame, so extensions propagate across a mixed swarm.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionV1 {
    /// Which application protocol this frame belongs to.
    pub namespace: [u8; 16],
    /// The application protocol's own message number.
    pub local_kind: u32,
    /// The application protocol's payload schema version.
    pub schema_version: u16,
    /// Opaque payload bytes, owned by the application protocol. Bounded to
    /// [`MAX_BODY_SIZE`].
    pub payload: Vec<u8>,
}

/// An offer of immutable content.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfferV1 {
    /// Canonical content identity.
    pub blob_hash: BlobHash,
    /// Untrusted display name.
    pub name: String,
    /// Advisory size; only the blob protocol can confirm it.
    pub size: u64,
    /// Advisory media type.
    pub media_type: Option<String>,
    /// Informational creation timestamp.
    pub created_at_ms: Option<u64>,
    /// Bounded, untrusted free-form metadata.
    pub metadata: BTreeMap<String, String>,
}

/// A provider announcement for a blob.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderV1 {
    /// The blob being announced.
    pub blob_hash: BlobHash,
    /// Whether the author now serves it or is withdrawing.
    pub state: ProviderState,
    /// When the author asserted this, in milliseconds since the Unix epoch.
    ///
    /// Provider state is *self-asserted*: an author can only make claims
    /// about itself, so its own clock is a safe tiebreak. Receivers keep the
    /// newest assertion per (author, blob) and ignore older ones, which stops
    /// a stale relay — say a catch-up sync log that ends before a withdrawal
    /// — from resurrecting a provider that has gone away.
    #[serde(default)]
    pub announced_at_ms: Option<u64>,
}

/// Provider state transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderState {
    /// The author serves the blob.
    Available,
    /// The author intends to stop serving it. Does not delete anything.
    Withdrawing,
}

/// A request asking who can serve a blob.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestV1 {
    /// The blob being requested.
    pub blob_hash: BlobHash,
}

/// The signed wire frame actually put on the gossip topic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SignedMessageV1 {
    /// ed25519 public key (= endpoint id) of the author.
    pub(crate) author: [u8; 32],
    /// ed25519 signature over `SIGN_DOMAIN || payload`.
    /// Serialized as a vec because serde does not cover `[u8; 64]`;
    /// the length is validated during decode.
    pub(crate) signature: Vec<u8>,
    /// postcard-encoded [`MessageV1`].
    pub(crate) payload: Vec<u8>,
}

impl MessageV1 {
    /// Create a new envelope with a random dedup id and the current time.
    pub fn new(body: MessageBodyV1) -> Self {
        Self::with_envelope(BodyEnvelopeV1::encode(&body).expect("core bodies always encode"))
    }

    /// Create a new envelope around an already-encoded body, which is how
    /// extensions send kinds this crate does not know about.
    pub fn with_envelope(body: BodyEnvelopeV1) -> Self {
        Self {
            version: WIRE_VERSION,
            id: rand::random(),
            sent_at_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            body,
        }
    }

    /// Sign and encode the message for broadcast.
    ///
    /// Returns an error if the encoded frame exceeds [`MAX_MESSAGE_SIZE`].
    pub fn encode(&self, secret: &SecretKey, topic: &TopicId) -> Result<Vec<u8>, ProtocolError> {
        let payload =
            postcard::to_allocvec(self).map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let signature = secret.sign(&signing_input(SIGN_DOMAIN, topic, &payload));
        let frame = SignedMessageV1 {
            author: *secret.public().as_bytes(),
            signature: signature.to_bytes().to_vec(),
            payload,
        };
        let bytes =
            postcard::to_allocvec(&frame).map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(bytes.len()));
        }
        Ok(bytes)
    }

    /// Decode, verify, and validate a received gossip payload for `topic`.
    ///
    /// The signature covers the topic: a frame copied from another drop
    /// fails verification here. Frames from the pre-review wire (signed
    /// without topic binding) are reported as `UnsupportedVersion`, never
    /// accepted.
    pub fn decode(bytes: &[u8], topic: &TopicId) -> Result<VerifiedMessage, ProtocolError> {
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(bytes.len()));
        }
        let (frame, _rest): (SignedMessageV1, _) = postcard::take_from_bytes(bytes)
            .map_err(|e| ProtocolError::Malformed(format!("frame: {e}")))?;
        let mut verified = Self::verify_for_topic(frame, topic)?;
        if verified.message.version != WIRE_VERSION {
            return Err(ProtocolError::UnsupportedVersion(verified.message.version));
        }
        if verified.message.body.payload.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(
                verified.message.body.payload.len(),
            ));
        }
        // An unknown kind is not an error: verify, hand it back undecoded, and
        // let the session decide whether to relay it.
        let body = verified.message.body.decode()?;
        if let Some(body) = &body {
            validate_body(body)?;
        }
        verified.body = body;
        Ok(verified)
    }

    /// Verify a frame against the public family's topic-bound signature.
    /// On signature failure, diagnose *why* without ever accepting: a frame
    /// that verifies under the legacy (pre-review) or sealed domain is
    /// reported as `UnsupportedVersion` — its true problem — so operators
    /// see "wrong family" instead of a misleading signature error.
    pub(crate) fn verify_for_topic(
        frame: SignedMessageV1,
        topic: &TopicId,
    ) -> Result<VerifiedMessage, ProtocolError> {
        match Self::verify_outer(&frame, SIGN_DOMAIN, topic) {
            Ok(verified) => Ok(verified),
            Err(ProtocolError::InvalidSignature) => {
                if let Some(version) = Self::diagnose_other_family(
                    &frame,
                    &[LEGACY_SIGN_DOMAIN, SEALED_SIGN_DOMAIN],
                    topic,
                ) {
                    return Err(ProtocolError::UnsupportedVersion(version));
                }
                Err(ProtocolError::InvalidSignature)
            }
            Err(e) => Err(e),
        }
    }

    /// If the frame verifies under any of `domains`, return its declared
    /// version. Diagnostic only — callers must never accept the frame.
    pub(crate) fn diagnose_other_family(
        frame: &SignedMessageV1,
        domains: &[&[u8]],
        topic: &TopicId,
    ) -> Option<u16> {
        for domain in domains {
            if *domain == LEGACY_SIGN_DOMAIN {
                if let Ok(legacy) = Self::verify_legacy(frame) {
                    return Some(legacy.message.version);
                }
            } else if let Ok(verified) = Self::verify_outer(frame, domain, topic) {
                return Some(verified.message.version);
            }
        }
        None
    }

    /// Verify against the pre-review signing input (legacy domain, no topic
    /// binding). Diagnostic only — callers must never accept the result.
    fn verify_legacy(frame: &SignedMessageV1) -> Result<VerifiedMessage, ProtocolError> {
        if frame.payload.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(frame.payload.len()));
        }
        let author =
            PublicKey::from_bytes(&frame.author).map_err(|_| ProtocolError::InvalidAuthor)?;
        let signature_bytes: &[u8; 64] = frame
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| ProtocolError::Malformed("signature must be 64 bytes".into()))?;
        let mut signing_bytes = Vec::with_capacity(LEGACY_SIGN_DOMAIN.len() + frame.payload.len());
        signing_bytes.extend_from_slice(LEGACY_SIGN_DOMAIN);
        signing_bytes.extend_from_slice(&frame.payload);
        let signature = Signature::from_bytes(signature_bytes);
        author
            .verify(&signing_bytes, &signature)
            .map_err(|_| ProtocolError::InvalidSignature)?;
        let (message, _rest): (MessageV1, _) = postcard::take_from_bytes(&frame.payload)
            .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))?;
        Ok(VerifiedMessage {
            author,
            message,
            body: None,
        })
    }

    /// Verify a frame's signature against `domain || topic || payload` and
    /// parse its envelope, without checking the version or decoding the
    /// body. Shared by the public decoder and the sealed family's decoder
    /// (`seal.rs`), which performs its own version check after this common
    /// verification.
    pub(crate) fn verify_outer(
        frame: &SignedMessageV1,
        domain: &[u8],
        topic: &TopicId,
    ) -> Result<VerifiedMessage, ProtocolError> {
        if frame.payload.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(frame.payload.len()));
        }
        let author =
            PublicKey::from_bytes(&frame.author).map_err(|_| ProtocolError::InvalidAuthor)?;
        let signature_bytes: &[u8; 64] = frame
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| ProtocolError::Malformed("signature must be 64 bytes".into()))?;
        let signature = Signature::from_bytes(signature_bytes);
        author
            .verify(&signing_input(domain, topic, &frame.payload), &signature)
            .map_err(|_| ProtocolError::InvalidSignature)?;
        let (message, _rest): (MessageV1, _) = postcard::take_from_bytes(&frame.payload)
            .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))?;
        Ok(VerifiedMessage {
            author: EndpointId::from(author),
            message,
            body: None,
        })
    }
}

/// Validate all bounded fields of a message body.
pub(crate) fn validate_body(body: &MessageBodyV1) -> Result<(), ProtocolError> {
    match body {
        MessageBodyV1::Offer(offer) => {
            validate_name(&offer.name)?;
            if offer.metadata.len() > MAX_METADATA_ENTRIES {
                return Err(ProtocolError::MetadataLimit(format!(
                    "{} entries, max {MAX_METADATA_ENTRIES}",
                    offer.metadata.len()
                )));
            }
            for (k, v) in &offer.metadata {
                if k.len() > MAX_METADATA_KEY_LEN {
                    return Err(ProtocolError::MetadataLimit(format!(
                        "key too long ({} bytes, max {MAX_METADATA_KEY_LEN})",
                        k.len()
                    )));
                }
                if v.len() > MAX_METADATA_VALUE_LEN {
                    return Err(ProtocolError::MetadataLimit(format!(
                        "value too long ({} bytes, max {MAX_METADATA_VALUE_LEN})",
                        v.len()
                    )));
                }
            }
            Ok(())
        }
        MessageBodyV1::Provider(_) | MessageBodyV1::Request(_) => Ok(()),
        MessageBodyV1::Extension(ext) => {
            // Namespace, local kind, and schema version are the
            // application protocol's own business; only the size bound is
            // ours to enforce.
            if ext.payload.len() > MAX_BODY_SIZE {
                return Err(ProtocolError::MessageTooLarge(ext.payload.len()));
            }
            Ok(())
        }
    }
}

/// Validate a display name for use in an offer.
///
/// Names are untrusted display metadata. They must never be interpreted as
/// paths. Rejects empty names, path separators, control characters, `.`/`..`
/// components, and names over 255 bytes.
pub fn validate_name(name: &str) -> Result<(), ProtocolError> {
    if name.is_empty() {
        return Err(ProtocolError::InvalidName("empty name".into()));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(ProtocolError::InvalidName(format!(
            "too long ({} bytes, max {MAX_NAME_LEN})",
            name.len()
        )));
    }
    if name == "." || name == ".." {
        return Err(ProtocolError::InvalidName("dot component".into()));
    }
    if name.contains(['/', '\\']) {
        return Err(ProtocolError::InvalidName("path separator".into()));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(ProtocolError::InvalidName("control character".into()));
    }
    if name.starts_with('.') {
        return Err(ProtocolError::InvalidName("leading dot".into()));
    }
    if name.ends_with(['.', ' ']) {
        return Err(ProtocolError::InvalidName("trailing dot or space".into()));
    }
    Ok(())
}

/// Build a collision-safe output path for an offered name inside `dir`.
///
/// The name must already have passed [`validate_name`]. If the target file
/// exists, `-<hash8>` and then `-N` suffixes are inserted before the
/// extension until a free name is found:
///
/// ```text
/// slides.pdf
/// slides-91c8aa02.pdf
/// slides-91c8aa02-2.pdf
/// ```
///
/// The returned path is guaranteed to be a direct child of `dir`.
pub fn collision_safe_path(
    dir: &std::path::Path,
    name: &str,
    hash: &BlobHash,
) -> Result<std::path::PathBuf, ProtocolError> {
    validate_name(name)?;
    let short = hash.fmt_short();
    let (stem, ext) = match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem.to_string(), format!(".{ext}")),
        _ => (name.to_string(), String::new()),
    };
    for attempt in 0..1000u32 {
        let candidate = match attempt {
            0 => format!("{stem}{ext}"),
            1 => format!("{stem}-{short}{ext}"),
            n => format!("{stem}-{short}-{n}{ext}"),
        };
        let path = dir.join(&candidate);
        // Defense in depth: the joined path must stay inside `dir`.
        if path.parent() != Some(dir) {
            return Err(ProtocolError::InvalidName(
                "escapes output directory".into(),
            ));
        }
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(ProtocolError::InvalidName(
        "could not find a free output name".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SecretKey {
        SecretKey::from_bytes(&[seed; 32])
    }

    fn topic() -> TopicId {
        TopicId::from_bytes([0xAB; 32])
    }

    fn offer(name: &str) -> MessageV1 {
        MessageV1::new(MessageBodyV1::Offer(OfferV1 {
            blob_hash: BlobHash::from_bytes([9u8; 32]),
            name: name.into(),
            size: 1234,
            media_type: Some("application/pdf".into()),
            created_at_ms: None,
            metadata: BTreeMap::new(),
        }))
    }

    #[test]
    fn roundtrip() {
        let msg = offer("slides.pdf");
        let bytes = msg.encode(&key(1), &topic()).unwrap();
        let verified = MessageV1::decode(&bytes, &topic()).unwrap();
        assert_eq!(verified.author, EndpointId::from(key(1).public()));
        assert_eq!(verified.message.version, WIRE_VERSION);
        assert_eq!(verified.message.body.kind, KIND_OFFER);
        match verified.body {
            Some(MessageBodyV1::Offer(o)) => {
                assert_eq!(o.name, "slides.pdf");
                assert_eq!(o.size, 1234);
            }
            other => panic!("wrong body: {other:?}"),
        }
    }

    #[test]
    fn rejects_wrong_version() {
        let mut msg = offer("a.txt");
        msg.version = 99;
        let bytes = msg.encode(&key(1), &topic()).unwrap();
        let err = MessageV1::decode(&bytes, &topic()).unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(99)));
    }

    #[test]
    fn rejects_forged_signature() {
        let msg = offer("a.txt");
        let mut bytes = msg.encode(&key(1), &topic()).unwrap();
        // Corrupt one byte inside the payload region.
        let n = bytes.len();
        bytes[n - 2] ^= 0xff;
        assert!(MessageV1::decode(&bytes, &topic()).is_err());
    }

    #[test]
    fn rejects_oversized() {
        let bytes = vec![0u8; MAX_MESSAGE_SIZE + 1];
        let err = MessageV1::decode(&bytes, &topic()).unwrap_err();
        assert!(matches!(err, ProtocolError::MessageTooLarge(_)));
    }

    #[test]
    fn rejects_bad_names() {
        for bad in [
            "", "..", ".", ".hidden", "a/b", "a\\b", "a\0b", "trail.", "trail ", "lí\nne",
        ] {
            let msg = offer(bad);
            let bytes = msg.encode(&key(1), &topic()).unwrap();
            assert!(
                matches!(
                    MessageV1::decode(&bytes, &topic()),
                    Err(ProtocolError::InvalidName(_))
                ),
                "expected rejection of {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_metadata_over_limits() {
        let mut offer_body = OfferV1 {
            blob_hash: BlobHash::from_bytes([1u8; 32]),
            name: "ok.txt".into(),
            size: 1,
            media_type: None,
            created_at_ms: None,
            metadata: (0..17).map(|i| (format!("k{i}"), "v".into())).collect(),
        };
        offer_body.size = 1;
        let msg = MessageV1::new(MessageBodyV1::Offer(offer_body));
        let bytes = msg.encode(&key(1), &topic()).unwrap();
        assert!(matches!(
            MessageV1::decode(&bytes, &topic()),
            Err(ProtocolError::MetadataLimit(_))
        ));
    }

    #[test]
    fn unknown_kinds_verify_but_do_not_decode() {
        // An extension kind this build knows nothing about.
        let envelope = BodyEnvelopeV1 {
            kind: 1234,
            payload: vec![7, 7, 7],
        };
        let msg = MessageV1::with_envelope(envelope);
        let bytes = msg.encode(&key(1), &topic()).unwrap();

        let verified = MessageV1::decode(&bytes, &topic()).expect("unknown kinds are not errors");
        assert_eq!(verified.author, EndpointId::from(key(1).public()));
        assert_eq!(verified.message.body.kind, 1234);
        assert!(
            verified.body.is_none(),
            "an unknown kind decodes to no body, not an error"
        );
        // The frame is intact, so it can be relayed verbatim to peers that do
        // understand kind 1234.
        assert_eq!(
            MessageV1::decode(&bytes, &topic())
                .unwrap()
                .message
                .body
                .payload,
            vec![7, 7, 7]
        );
    }

    #[test]
    fn unknown_kinds_still_require_a_valid_signature() {
        let msg = MessageV1::with_envelope(BodyEnvelopeV1 {
            kind: 4321,
            payload: vec![1],
        });
        let mut bytes = msg.encode(&key(1), &topic()).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        assert!(
            MessageV1::decode(&bytes, &topic()).is_err(),
            "extensibility must not become a signature bypass"
        );
    }

    #[test]
    fn provider_announcements_carry_their_own_clock() {
        let msg = MessageV1::new(MessageBodyV1::Provider(ProviderV1 {
            blob_hash: BlobHash::from_bytes([2u8; 32]),
            state: ProviderState::Withdrawing,
            announced_at_ms: Some(1_700_000_000_000),
        }));
        let bytes = msg.encode(&key(2), &topic()).unwrap();
        let verified = MessageV1::decode(&bytes, &topic()).unwrap();
        match verified.body {
            Some(MessageBodyV1::Provider(p)) => {
                assert_eq!(p.announced_at_ms, Some(1_700_000_000_000));
                assert_eq!(p.state, ProviderState::Withdrawing);
            }
            other => panic!("wrong body: {other:?}"),
        }
    }

    #[test]
    fn tolerates_trailing_additive_bytes() {
        let msg = offer("slides.pdf");
        let mut bytes = msg.encode(&key(1), &topic()).unwrap();
        bytes.extend_from_slice(&[9, 9, 9]);
        let verified = MessageV1::decode(&bytes, &topic()).unwrap();
        assert_eq!(verified.message.version, WIRE_VERSION);
    }

    #[test]
    fn collision_safe_names() {
        let dir = std::env::temp_dir().join(format!("iroh-drop-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let hash = BlobHash::from_bytes([1u8; 32]);
        let p1 = collision_safe_path(&dir, "slides.pdf", &hash).unwrap();
        assert_eq!(p1.file_name().unwrap(), "slides.pdf");
        std::fs::write(&p1, b"x").unwrap();
        let p2 = collision_safe_path(&dir, "slides.pdf", &hash).unwrap();
        assert_eq!(
            p2.file_name().unwrap().to_str().unwrap(),
            format!("slides-{}.pdf", hash.fmt_short())
        );
        std::fs::write(&p2, b"x").unwrap();
        let p3 = collision_safe_path(&dir, "slides.pdf", &hash).unwrap();
        assert_eq!(
            p3.file_name().unwrap().to_str().unwrap(),
            format!("slides-{}-2.pdf", hash.fmt_short())
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn decode_never_panics_on_arbitrary_input() {
        // Poor man's fuzz: sweep deterministic pseudo-random buffers.
        let mut state = 0x12345678u64;
        for len in 0..600usize {
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *b = (state >> 33) as u8;
            }
            let _ = MessageV1::decode(&buf, &topic());
        }
    }
}
