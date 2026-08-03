#![no_main]

use iroh_drop::message::MessageV1;
use iroh_drop::{DropKey, TopicId};
use libfuzzer_sys::fuzz_target;

// Sealed frames are the same hostile surface as plaintext frames, plus a
// decrypt step: fixed key and topic, arbitrary bytes. Decoding must never
// panic — wrong keys, garbage, and truncated ciphertexts all land on
// ordinary error paths.
fuzz_target!(|data: &[u8]| {
    let key = DropKey::from_bytes([0x11; 32]);
    let topic = TopicId::from_bytes([0x22; 32]);
    let _ = MessageV1::decode_sealed(data, &key, &topic);
});
