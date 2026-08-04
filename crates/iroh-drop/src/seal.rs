//! Sealed wire family (message family 4): private drops.
//!
//! A session is private iff its ticket selects [`DropMode::Sealed`](crate::DropMode::Sealed).
//! Private sessions speak and accept only family 4; public sessions only
//! family 3 — there are no mixed drops. The full format, the per-family
//! signature domains, and the threat model are specified in
//! `docs/protocol.md` ("Private drops").
//!
//! In one paragraph: the inner body (any `BodyEnvelopeV1` — core kinds and
//! extension envelopes alike) is encrypted with XChaCha20-Poly1305 under
//! `HKDF-SHA256(drop_key, topic_id)`, wrapped in an ordinary `MessageV1`
//! with `version: 4` and kind [`KIND_SEALED`], and signed over the sealed
//! family's domain plus the topic, like any other frame. Authorship,
//! dedup, retention, and relaying all work on the outer frame, so key-less
//! peers can carry private traffic without reading it.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::XChaCha20Poly1305;
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use iroh_gossip::proto::TopicId;
use sha2::Sha256;

use crate::error::ProtocolError;
use crate::message::{
    BodyEnvelopeV1, MessageV1, SignedMessageV1, VerifiedMessage, LEGACY_SIGN_DOMAIN, MAX_BODY_SIZE,
    MAX_MESSAGE_SIZE, SEALED_SIGN_DOMAIN, SIGN_DOMAIN,
};

/// Wire family of sealed frames.
pub const SEALED_WIRE_VERSION: u16 = 4;

/// Signature domain for sealed outer frames (see
/// `crate::message::SEALED_SIGN_DOMAIN`); the signing input includes the
/// topic — see `crate::message::signing_input`.
///
/// Body kind carrying a sealed inner envelope:
pub const KIND_SEALED: u16 = 4;

/// HKDF info string deriving the AEAD key from the drop key and topic.
const SEAL_KDF_INFO: &[u8] = b"iroh-drop/v4/seal";

/// HMAC domain for sync key-proofs.
/// HKDF info for deriving the sync-proof key. Separate from
/// `SEAL_KDF_INFO`, so the key that decrypts frames and the key that
/// proves history access are different keys — an HMAC oracle for one says
/// nothing about the other.
const SYNC_HKDF_INFO: &[u8] = b"iroh-drop/v4/sync";

const NONCE_LEN: usize = 24;

/// The symmetric key shared by every member of a private drop, carried by
/// the ticket. `Debug` is redacted; the bytes never appear in logs.
#[derive(Clone)]
pub struct DropKey([u8; 32]);

impl DropKey {
    /// A fresh random drop key (what `create` uses for private drops).
    pub fn generate() -> Self {
        Self(rand::random())
    }

    /// Wrap raw key bytes (e.g. from a ticket).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw key bytes (for ticket encoding).
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// AEAD key for `topic`: HKDF-SHA256(ikm = drop_key, salt = topic).
    fn aead_key(&self, topic: &TopicId) -> [u8; 32] {
        let hk = Hkdf::<Sha256>::new(Some(topic.as_bytes()), &self.0);
        let mut key = [0u8; 32];
        hk.expand(SEAL_KDF_INFO, &mut key)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        key
    }

    /// Proof of key possession for catch-up sync:
    /// `HMAC-SHA256(drop_key, domain || topic || salt)`.
    /// Prove possession of the drop key for one specific sync request.
    ///
    /// The proof is bound to the connection (`requester`, `responder`) and
    /// to the exact encoded request, so it is not a portable capability:
    /// replaying it to a different peer, from a different peer, or against
    /// a different page fails. Replaying the identical request between the
    /// same pair is harmless — page requests are idempotent.
    pub fn sync_proof(
        &self,
        topic: &TopicId,
        requester: &iroh::EndpointId,
        responder: &iroh::EndpointId,
        request_bytes: &[u8],
    ) -> [u8; 32] {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.sync_key(topic))
            .expect("HMAC takes any key length");
        mac.update(requester.as_bytes());
        mac.update(responder.as_bytes());
        mac.update(request_bytes);
        mac.finalize().into_bytes().into()
    }

    /// Constant-time check of a sync proof.
    pub fn verify_sync_proof(
        &self,
        topic: &TopicId,
        requester: &iroh::EndpointId,
        responder: &iroh::EndpointId,
        request_bytes: &[u8],
        proof: &[u8; 32],
    ) -> bool {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.sync_key(topic))
            .expect("HMAC takes any key length");
        mac.update(requester.as_bytes());
        mac.update(responder.as_bytes());
        mac.update(request_bytes);
        mac.verify_slice(proof).is_ok()
    }

    /// `HKDF-SHA256(drop_key, salt = topic, info = SYNC_HKDF_INFO)` — the
    /// sync-proof key, key-separated from the frame AEAD key.
    fn sync_key(&self, topic: &TopicId) -> [u8; 32] {
        let hk = Hkdf::<Sha256>::new(Some(topic.as_bytes()), &self.0);
        let mut key = [0u8; 32];
        hk.expand(SYNC_HKDF_INFO, &mut key)
            .expect("32 bytes is a valid HKDF output length");
        key
    }
}

impl std::fmt::Debug for DropKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DropKey(<redacted>)")
    }
}

impl MessageV1 {
    /// Sign, seal, and encode this message's body for a private drop.
    ///
    /// The message's `id` becomes the outer frame's id (bound into the
    /// AEAD's additional data); `version` and `body.kind` are replaced by
    /// the sealed family's values. The result is an ordinary signed frame
    /// any peer can verify and relay, but only key holders can read.
    pub fn encode_sealed(
        &self,
        secret: &iroh::SecretKey,
        drop_key: &DropKey,
        topic: &TopicId,
    ) -> Result<Vec<u8>, ProtocolError> {
        let inner_bytes = postcard::to_allocvec(&self.body)
            .map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        if inner_bytes.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(inner_bytes.len()));
        }
        let nonce_bytes: [u8; NONCE_LEN] = rand::random();
        self.encode_sealed_with_nonce(secret, drop_key, topic, nonce_bytes, inner_bytes)
    }

    /// The nonce-explicit core of [`Self::encode_sealed`], split out so the
    /// golden-fixture conformance test can produce byte-stable frames. The
    /// `inner_bytes` must be the postcard encoding of `self.body`.
    fn encode_sealed_with_nonce(
        &self,
        secret: &iroh::SecretKey,
        drop_key: &DropKey,
        topic: &TopicId,
        nonce_bytes: [u8; NONCE_LEN],
        inner_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, ProtocolError> {
        let author = *secret.public().as_bytes();
        let key = drop_key.aead_key(topic);
        let cipher = XChaCha20Poly1305::new((&key).into());
        let nonce = chacha20poly1305::XNonce::from_slice(&nonce_bytes);
        let mut aad = Vec::with_capacity(48);
        aad.extend_from_slice(&author);
        aad.extend_from_slice(&self.id);
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: &inner_bytes,
                    aad: &aad,
                },
            )
            .map_err(|_| ProtocolError::Malformed("seal: encrypt failed".into()))?;
        let mut sealed = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        sealed.extend_from_slice(nonce.as_slice());
        sealed.extend_from_slice(&ciphertext);

        let outer = MessageV1 {
            version: SEALED_WIRE_VERSION,
            id: self.id,
            sent_at_ms: self.sent_at_ms,
            body: BodyEnvelopeV1 {
                kind: KIND_SEALED,
                payload: sealed,
            },
        };
        let payload =
            postcard::to_allocvec(&outer).map_err(|e| ProtocolError::Malformed(e.to_string()))?;
        let signature = secret.sign(&crate::message::signing_input(
            SEALED_SIGN_DOMAIN,
            topic,
            &payload,
        ));
        let frame = SignedMessageV1 {
            author,
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

    /// Verify a sealed frame's outer signature and family *without*
    /// decrypting — the blind-relay path (sealed ticket, no key). The body
    /// is returned undecoded, exactly like an unknown extension kind, so
    /// relays retain and forward what they cannot read.
    pub(crate) fn verify_sealed_outer(
        bytes: &[u8],
        topic: &TopicId,
    ) -> Result<VerifiedMessage, ProtocolError> {
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(bytes.len()));
        }
        let (frame, _rest): (SignedMessageV1, _) = postcard::take_from_bytes(bytes)
            .map_err(|e| ProtocolError::Malformed(format!("frame: {e}")))?;
        let verified = Self::verify_outer(&frame, SEALED_SIGN_DOMAIN, topic)?;
        if verified.message.version != SEALED_WIRE_VERSION {
            return Err(ProtocolError::UnsupportedVersion(verified.message.version));
        }
        if verified.message.body.kind != KIND_SEALED {
            return Err(ProtocolError::Malformed(
                "sealed family frame with non-sealed kind".into(),
            ));
        }
        if verified.message.body.payload.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(
                verified.message.body.payload.len(),
            ));
        }
        Ok(verified)
    }

    /// Decode, verify, and unseal a family-4 frame.
    ///
    /// `UnsupportedVersion(3)` frames — a downgrade attempt in a private
    /// session — and any unseal failure are errors; callers report and
    /// ignore, the session is undisturbed.
    pub fn decode_sealed(
        bytes: &[u8],
        drop_key: &DropKey,
        topic: &TopicId,
    ) -> Result<VerifiedMessage, ProtocolError> {
        if bytes.len() > MAX_MESSAGE_SIZE {
            return Err(ProtocolError::MessageTooLarge(bytes.len()));
        }
        let (frame, _rest): (SignedMessageV1, _) = postcard::take_from_bytes(bytes)
            .map_err(|e| ProtocolError::Malformed(format!("frame: {e}")))?;
        let verified_outer = match Self::verify_outer(&frame, SEALED_SIGN_DOMAIN, topic) {
            Ok(verified) => verified,
            Err(ProtocolError::InvalidSignature) => {
                // Diagnose public-family and legacy frames as the version
                // errors they are; never accept them.
                if let Some(version) =
                    Self::diagnose_other_family(&frame, &[SIGN_DOMAIN, LEGACY_SIGN_DOMAIN], topic)
                {
                    return Err(ProtocolError::UnsupportedVersion(version));
                }
                return Err(ProtocolError::InvalidSignature);
            }
            Err(e) => return Err(e),
        };
        if verified_outer.message.version != SEALED_WIRE_VERSION {
            return Err(ProtocolError::UnsupportedVersion(
                verified_outer.message.version,
            ));
        }
        let outer = verified_outer.message;
        if outer.body.kind != KIND_SEALED {
            return Err(ProtocolError::Malformed(format!(
                "version {SEALED_WIRE_VERSION} frame with non-sealed kind {}",
                outer.body.kind
            )));
        }
        let sealed = &outer.body.payload;
        if sealed.len() < NONCE_LEN {
            return Err(ProtocolError::Malformed("sealed body too short".into()));
        }
        let (nonce, ciphertext) = sealed.split_at(NONCE_LEN);
        let key = drop_key.aead_key(topic);
        let cipher = XChaCha20Poly1305::new((&key).into());
        let mut aad = Vec::with_capacity(48);
        aad.extend_from_slice(verified_outer.author.as_bytes());
        aad.extend_from_slice(&outer.id);
        let inner_bytes = cipher
            .decrypt(
                nonce.into(),
                Payload {
                    msg: ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| ProtocolError::Unseal)?;
        if inner_bytes.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(inner_bytes.len()));
        }
        let (inner, _rest): (BodyEnvelopeV1, _) = postcard::take_from_bytes(&inner_bytes)
            .map_err(|e| ProtocolError::Malformed(format!("sealed inner: {e}")))?;
        if inner.payload.len() > MAX_BODY_SIZE {
            return Err(ProtocolError::MessageTooLarge(inner.payload.len()));
        }
        let body = inner.decode()?;
        if let Some(body) = &body {
            crate::message::validate_body(body)?;
        }
        Ok(VerifiedMessage {
            author: verified_outer.author,
            message: MessageV1 {
                version: SEALED_WIRE_VERSION,
                id: outer.id,
                sent_at_ms: outer.sent_at_ms,
                body: inner,
            },
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{MessageBodyV1, OfferV1, WIRE_VERSION};
    use crate::BlobHash;

    fn test_offer() -> MessageV1 {
        MessageV1::new(MessageBodyV1::Offer(OfferV1 {
            blob_hash: BlobHash::from_bytes([7u8; 32]),
            name: "secret-plans.txt".into(),
            size: 42,
            media_type: None,
            created_at_ms: None,
            metadata: Default::default(),
        }))
    }

    #[test]
    fn sealed_round_trip() {
        let secret = iroh::SecretKey::generate();
        let key = DropKey::generate();
        let topic = TopicId::from_bytes([1u8; 32]);
        let msg = test_offer();
        let frame = msg.encode_sealed(&secret, &key, &topic).unwrap();
        let verified = MessageV1::decode_sealed(&frame, &key, &topic).unwrap();
        assert_eq!(verified.author, secret.public());
        assert_eq!(verified.message.version, SEALED_WIRE_VERSION);
        assert_eq!(verified.message.id, msg.id);
        let Some(MessageBodyV1::Offer(offer)) = verified.body else {
            panic!("expected offer")
        };
        assert_eq!(offer.name, "secret-plans.txt");
        // The wire bytes are ciphertext: the offer name must not appear.
        let needle = b"secret-plans";
        assert!(
            !frame.windows(needle.len()).any(|w| w == needle),
            "offer name leaked into the sealed frame"
        );
    }

    #[test]
    fn wrong_key_does_not_unseal() {
        let secret = iroh::SecretKey::generate();
        let topic = TopicId::from_bytes([1u8; 32]);
        let frame = test_offer()
            .encode_sealed(&secret, &DropKey::generate(), &topic)
            .unwrap();
        let err = MessageV1::decode_sealed(&frame, &DropKey::generate(), &topic).unwrap_err();
        assert!(matches!(err, ProtocolError::Unseal));
    }

    #[test]
    fn wrong_topic_is_rejected() {
        // The topic is in the signing input *and* the key derivation: a
        // frame copied into another drop fails at signature verification,
        // before decryption is even attempted.
        let secret = iroh::SecretKey::generate();
        let key = DropKey::generate();
        let frame = test_offer()
            .encode_sealed(&secret, &key, &TopicId::from_bytes([1u8; 32]))
            .unwrap();
        let err =
            MessageV1::decode_sealed(&frame, &key, &TopicId::from_bytes([2u8; 32])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidSignature));
    }

    #[test]
    fn tampered_ciphertext_does_not_unseal() {
        let secret = iroh::SecretKey::generate();
        let key = DropKey::generate();
        let topic = TopicId::from_bytes([1u8; 32]);
        let mut frame = test_offer().encode_sealed(&secret, &key, &topic).unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 0x01;
        // Either the AEAD tag or the outer signature catches it — never Ok.
        assert!(MessageV1::decode_sealed(&frame, &key, &topic).is_err());
    }

    #[test]
    fn public_decoder_reports_unsupported_version() {
        // The old-build story: a family-2 decoder cleanly rejects sealed
        // frames as an unsupported *version*, after verifying the signature.
        let secret = iroh::SecretKey::generate();
        let key = DropKey::generate();
        let topic = TopicId::from_bytes([1u8; 32]);
        let frame = test_offer().encode_sealed(&secret, &key, &topic).unwrap();
        let err = MessageV1::decode(&frame, &topic).unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(v) if v == SEALED_WIRE_VERSION));
    }

    #[test]
    fn private_decoder_rejects_public_frames() {
        // Downgrade injection: a plaintext family-2 frame must not be read
        // by a private session.
        let secret = iroh::SecretKey::generate();
        let key = DropKey::generate();
        let topic = TopicId::from_bytes([1u8; 32]);
        let frame = test_offer().encode(&secret, &topic).unwrap();
        let err = MessageV1::decode_sealed(&frame, &key, &topic).unwrap_err();
        assert!(matches!(err, ProtocolError::UnsupportedVersion(v) if v == WIRE_VERSION));
    }

    /// WS1 ceremony for the sealed family: a byte-stable golden frame,
    /// pinned in `tests/fixtures/`. Regenerate deliberately with
    /// `IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance` and review
    /// the diff like the wire-format change it is.
    mod conformance {
        use super::super::{DropKey, SEALED_WIRE_VERSION};
        use crate::message::{BodyEnvelopeV1, MessageBodyV1, MessageV1, OfferV1};
        use crate::BlobHash;
        use iroh_gossip::proto::TopicId;

        const FIXTURE: &str = "sealed_offer.bin";
        const NONCE: [u8; 24] = [0xC3; 24];

        fn fixture_frame() -> (Vec<u8>, DropKey, TopicId) {
            let secret = iroh::SecretKey::from_bytes(&[0x5A; 32]);
            let key = DropKey::from_bytes([0x11; 32]);
            let topic = TopicId::from_bytes([0x22; 32]);
            let msg = MessageV1 {
                version: SEALED_WIRE_VERSION,
                id: [0x33; 16],
                sent_at_ms: 1_752_700_000_000,
                body: BodyEnvelopeV1 {
                    kind: crate::message::KIND_OFFER,
                    payload: postcard::to_allocvec(&OfferV1 {
                        blob_hash: BlobHash::from_bytes([0x44; 32]),
                        name: "sealed-fixture.txt".into(),
                        size: 128,
                        media_type: None,
                        created_at_ms: Some(1_752_700_000_000),
                        metadata: Default::default(),
                    })
                    .unwrap(),
                },
            };
            let inner = postcard::to_allocvec(&msg.body).unwrap();
            let frame = msg
                .encode_sealed_with_nonce(&secret, &key, &topic, NONCE, inner)
                .unwrap();
            (frame, key, topic)
        }

        fn fixture_path() -> std::path::PathBuf {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures")
                .join(FIXTURE)
        }

        #[test]
        fn conformance_sealed_frame_matches_committed_bytes() {
            let (frame, _, _) = fixture_frame();
            if std::env::var_os("IROH_DROP_BLESS").is_some() {
                std::fs::write(fixture_path(), frame).unwrap();
                return;
            }
            let committed = std::fs::read(fixture_path()).unwrap_or_else(|e| {
                panic!("missing fixture {FIXTURE} ({e}) — run: IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance")
            });
            assert_eq!(
                committed, frame,
                "sealed fixture drifted from the wire format"
            );
        }

        #[test]
        fn conformance_sealed_frame_decodes_to_exact_values() {
            if std::env::var_os("IROH_DROP_BLESS").is_some() {
                return;
            }
            let committed = std::fs::read(fixture_path()).unwrap();
            let (_, key, topic) = fixture_frame();
            let verified = MessageV1::decode_sealed(&committed, &key, &topic).unwrap();
            assert_eq!(verified.message.version, SEALED_WIRE_VERSION);
            assert_eq!(verified.message.id, [0x33; 16]);
            assert_eq!(verified.message.sent_at_ms, 1_752_700_000_000);
            let Some(MessageBodyV1::Offer(offer)) = verified.body else {
                panic!("expected offer")
            };
            assert_eq!(offer.name, "sealed-fixture.txt");
            assert_eq!(offer.size, 128);
            // The golden bytes are ciphertext: the offer name appears
            // nowhere on the wire.
            let needle = b"sealed-fixture";
            assert!(!committed.windows(needle.len()).any(|w| w == needle));
        }

        #[test]
        fn conformance_sealed_every_byte_flip_is_fatal() {
            let (frame, key, topic) = fixture_frame();
            for i in 0..frame.len() {
                for bit in [0x01u8, 0x80] {
                    let mut flipped = frame.clone();
                    flipped[i] ^= bit;
                    assert!(
                        MessageV1::decode_sealed(&flipped, &key, &topic).is_err(),
                        "flip at byte {i} bit {bit:#x} must be fatal to the sealed decoder"
                    );
                    assert!(
                        MessageV1::decode(&flipped, &topic).is_err(),
                        "flip at byte {i} bit {bit:#x} must be fatal to the family-2 decoder"
                    );
                }
            }
        }
    }

    #[test]
    fn sync_proof_is_bound_to_connection_and_request() {
        let key = DropKey::generate();
        let topic = TopicId::from_bytes([9u8; 32]);
        let requester = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let responder = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let request = b"encoded-request-bytes";
        let proof = key.sync_proof(&topic, &requester, &responder, request);
        assert!(key.verify_sync_proof(&topic, &requester, &responder, request, &proof));
        // Wrong key, wrong topic, swapped roles, different request bytes:
        // every variation must fail.
        assert!(
            !DropKey::generate().verify_sync_proof(&topic, &requester, &responder, request, &proof)
        );
        assert!(!key.verify_sync_proof(
            &TopicId::from_bytes([8u8; 32]),
            &requester,
            &responder,
            request,
            &proof
        ));
        assert!(!key.verify_sync_proof(&topic, &responder, &requester, request, &proof));
        assert!(!key.verify_sync_proof(&topic, &requester, &responder, b"other-request", &proof));
        let mut bad = proof;
        bad[0] ^= 1;
        assert!(!key.verify_sync_proof(&topic, &requester, &responder, request, &bad));
    }
}
