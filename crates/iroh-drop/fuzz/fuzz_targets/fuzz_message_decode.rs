#![no_main]

use iroh_drop::message::MessageV1;
use iroh_drop::TopicId;
use libfuzzer_sys::fuzz_target;

// Gossip frames are the most hostile bytes this crate handles: relayed
// between strangers, signature-verified, bounded-decoded. Decoding must
// never panic, never allocate beyond the message cap, and never trust.
fuzz_target!(|data: &[u8]| {
    // Fixed topic: signature verification is topic-bound, so a stable
    // topic keeps coverage on the decode paths rather than the reject path.
    let topic = TopicId::from_bytes([0xC1; 32]);
    let _ = MessageV1::decode(data, &topic);
});
