#![no_main]

use iroh_drop::wire::{
    ControlRequestV1, ControlResponseV1, HelloV1, SyncRequestV1, SyncResponseV1,
};
use libfuzzer_sys::fuzz_target;

// Control-channel envelopes are the second most hostile bytes: every peer
// we join gets to send them. Decoding must never panic. (Frames carried
// inside a SyncResponseV1 are covered separately by fuzz_message_decode.)
fuzz_target!(|data: &[u8]| {
    let _ = postcard::take_from_bytes::<ControlRequestV1>(data);
    let _ = postcard::take_from_bytes::<ControlResponseV1>(data);
    let _ = postcard::take_from_bytes::<HelloV1>(data);
    let _ = postcard::take_from_bytes::<SyncRequestV1>(data);
    let _ = postcard::take_from_bytes::<SyncResponseV1>(data);
});
