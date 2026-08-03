# Golden conformance fixtures

The committed bytes in this directory are the **normative examples of the
iroh-drop wire format** (`WIRE_VERSION = 2`). They exist so that wire-format
drift is caught by a test, not by a confused peer: any non-additive change
to the structs in `src/message.rs`, `src/ticket.rs`, or `src/sync.rs` fails
the conformance suite.

Every vector is deterministic — fixed author key (`[0xA5; 32]`), fixed
message ids, fixed timestamps — so the generator's output is byte-stable
forever. All addresses use documentation ranges (`203.0.113.0/24`) and a
`relay1.example.com` relay; nothing here can or should contact a network.

## Inventory

### Gossip frames (signed; author key `[0xA5; 32]`)

| File | Body | Proves |
|---|---|---|
| `offer_full.bin` | `Offer` (kind 1), all optional fields set | the full offer schema, byte-frozen |
| `provider_available.bin` | `Provider` (kind 2), `Available` | provider assertion + self-asserted clock |
| `provider_withdrawing.bin` | `Provider` (kind 2), `Withdrawing` | withdrawal ordering field |
| `request.bin` | `Request` (kind 3) | by-hash request schema |
| `unknown_kind.bin` | kind 1500 (experimental range) | unknown kinds verify, decode to no body, stay relayable |
| `trailing_additive.bin` | the offer frame + `[9, 9, 9]` | additive trailing bytes are tolerated |

### Sealed frames (private drops, wire family 3)

| File | Proves |
|---|---|
| `sealed_offer.bin` | a family-3 frame: outer `MessageV1` (version 3, kind 4) wrapping `nonce \|\| XChaCha20-Poly1305(HKDF(drop_key, topic), inner BodyEnvelopeV1)`. Fixed key `[0x11; 32]`, topic `[0x22; 32]`, nonce `[0xC3; 24]`, author key `[0x5A; 32]`. Decodes to an `Offer` for `sealed-fixture.txt`; the name appears nowhere in the bytes; every single-byte flip is fatal to both the family-3 and family-2 decoders. The family-2 decoder rejects it as `UnsupportedVersion(3)` — the old-build story. |

### Must-reject vectors

| File | Fails with |
|---|---|
| `reject_wrong_version.bin` (validly signed version 99) | `UnsupportedVersion(99)` |
| `reject_bad_signature.bin` (one payload byte flipped) | `InvalidSignature` |
| `reject_invalid_name.bin` (300-byte name) | `InvalidName` |
| `reject_metadata_limit.bin` (17 metadata entries) | `MetadataLimit` |

The suite additionally proves that flipping **any single byte** of a
canonical signed frame is detected — except bytes in the additive tail,
which are unsigned and tolerated by design (that tolerance is what makes
additive evolution within a major version possible).

### Tickets

| File | Contents |
|---|---|
| `ticket_full.txt` | full bootstrap address (relay + socket) + id-only peer, display name, auto-fetch hint |
| `ticket_short.txt` | the same drop, id-only bootstrap entries |

### Control channel (`/iroh-drop/1`)

| File | Contents |
|---|---|
| `control_hello_request.bin` | `ControlRequestV1{op: Hello}` |
| `control_hello_response.bin` | `HelloV1`: wire versions, ops, kinds, page cap |
| `control_sync_request.bin` | `SyncRequestV1`: topic, cursor 0, 16-frame hint |
| `control_sync_response.bin` | `SyncResponseV1`: caught up, three frames — byte-identical to `offer_full.bin`, `provider_available.bin`, `request.bin` |

The control envelopes are crate-private by design, so their vectors are
generated and checked by the `conformance` module inside `src/sync.rs`;
the gossip and ticket vectors live in `tests/conformance.rs` +
`tests/common/fixtures.rs`. The cross-check (embedded frames must equal the
committed gossip fixtures) pins the two generators together.

## Regenerating

Only ever regenerate after a **deliberate** wire change, and review the
resulting diff like the wire-format change it is:

```sh
IROH_DROP_BLESS=1 cargo test -p iroh-drop conformance   # rewrite this directory
cargo test -p iroh-drop conformance                     # verify everything cross-checks
git diff                                                # review every changed byte
```

Rules:

1. Never edit fixture files by hand.
2. A fixture diff in a PR is a wire-format change: treat it with the same
   ceremony as bumping `WIRE_VERSION` (and ask whether it should have been
   one).
3. New wire surface (a new kind, a new control op, ticket v2) gets new
   fixtures in the same PR, both accept and reject sides.
