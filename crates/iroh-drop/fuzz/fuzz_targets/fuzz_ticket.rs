#![no_main]

use iroh_drop::ticket::DropTicket;
use libfuzzer_sys::fuzz_target;

// Tickets arrive as pasted text: share links, chat messages, filenames,
// shells. Parsing must never panic on any byte sequence.
fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let _ = DropTicket::from_string_prefixed(&text);
    // The bare-base32 form is a distinct parse path.
    let _ = text.parse::<DropTicket>();
});
