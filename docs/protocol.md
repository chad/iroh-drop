# iroh-drop wire protocol (v0.4)

Reference for the on-the-wire formats. Everything is implemented in
`crates/iroh-drop`; the message definitions live in `src/message.rs`, tickets
in `src/ticket.rs`.

## Transport

One shared `iroh` endpoint per peer carries all three protocols:

| Protocol | ALPN | Role |
|---|---|---|
| `iroh-gossip` | `/iroh-gossip/1` (its own) | membership + drop coordination messages |
| `iroh-blobs` | `/iroh-blobs/1` (its own) | storage + verified content transfer |
| `iroh-drop` control | `/iroh-drop/1` | direct request/response control ops; carries **catch-up sync** (below) |

A drop is a gossip topic: `topic_id = 32 random bytes` chosen by the creator.
Coordination messages ride the topic as opaque payloads (max 64 KiB,
`MAX_MESSAGE_SIZE`); content bytes never touch gossip.

## Messages

Encoding: **postcard** over the structs below, decoded with
`take_from_bytes` so trailing additive fields from future versions are
tolerated. Readers must reject: wrong `version`, undecodable bodies, bad
signatures, oversized frames, and over-limit names/metadata — without
disturbing the session (see `tests/malformed_messages.rs`).

### Frames and bodies

```text
SignedMessageV1 {                  // the frame put on the gossip topic
    author:   [u8; 32],            // ed25519 public key == iroh EndpointId
    payload:  Vec<u8>,             // postcard(MessageV1)
    signature: [u8; 64],           // ed25519 over SIGN_DOMAIN || payload
}
SIGN_DOMAIN = b"iroh-drop/v1/message/"

MessageV1 {
    version:    u16,               // == 1 (WIRE_VERSION)
    id:         [u8; 16],          // random, for dedup (10k entries / 10 min)
    sent_at_ms: u64,               // informational only
    body:       MessageBodyV1,
}

MessageBodyV1 = Offer | Provider | Request

OfferV1 {
    blob_hash:     BlobHash,               // [u8; 32], canonical identity
    name:          String,                 // untrusted display name, ≤ 255 B
    size:          u64,                    // advisory; blobs protocol confirms
    media_type:    Option<String>,         // advisory
    created_at_ms: Option<u64>,            // informational
    metadata:      BTreeMap<String, String> // ≤ 16 entries, keys ≤ 64 B, values ≤ 512 B
}

ProviderV1 { blob_hash: BlobHash, state: Available | Withdrawing }
RequestV1  { blob_hash: BlobHash }
```

Why signatures: gossip only tells you the *delivering neighbor*, not the
author, so authorship must be cryptographic. Offers are keyed by hash:
re-offering a known hash under a new name adds the name as an alias.
Different hashes may collide on a display name — names are untrusted and
lookup by name resolves to one matching offer, so scripts should use hashes.

### Body kinds

A body is a **kind tag plus opaque bytes**, not a closed enum:

```text
BodyEnvelopeV1 {
    kind:    u16,                  // 1 = Offer, 2 = Provider, 3 = Request
    payload: Vec<u8>,              // postcard of that kind's struct
}
```

| Range | Meaning |
|---|---|
| `1..=999` | defined by this specification |
| `1000..=1999` | free for experiments |
| `2000..` | applications and vendors |

Rules for receivers:

- An unknown `kind` is **not an error**. Verify the signature, ignore the
  body, emit an observable warning, and keep the session running.
- Unknown kinds are still **relayable**: the frame stays intact and is
  retained (within a small budget) so this peer passes extensions along to
  peers that do understand them. A peer therefore does not have to
  understand an extension to help it propagate.
- Unknown kinds are *not* exempt from anything else: signature, size, and
  rate limits all apply first.

This is why wire version 2 exists: version 1 encoded the body as a postcard
enum, where an unrecognized variant made the whole frame undecodable. Body
schemas (`OfferV1`, `ProviderV1`, `RequestV1`) are unchanged.

## Catch-up sync (control ALPN)

Gossip has no history: a peer that joins after an offer was broadcast would
never hear about it. Sync closes that gap without weakening authentication.

Every control request is itself wrapped, for the same reason bodies are:

```text
ControlRequestV1  { version: u16, op: u16, payload: Vec<u8> }
ControlResponseV1 { version: u16, op: u16, payload: Vec<u8> }

op 1 = Hello        → HelloV1 { wire_versions, ops, message_kinds,
                                max_frames_per_page }
op 2 = SyncPage     → SyncRequestV1 / SyncResponseV1 (below)
op 65535            → "I do not implement that operation"
```

`Hello` is capability *discovery*: a peer states which wire versions, control
operations and message kinds it implements, so clients negotiate instead of
inferring from a version number. A version mismatch is answered rather than
dropped, so the other side learns what we speak. New operations need neither a
new ALPN nor a version bump.

A joiner opens `/iroh-drop/1` to each bootstrap peer from its ticket, says
hello, then pages through that peer's retained frames:

```text
SyncRequestV1 {                    // one bi-stream per page, EOF-delimited
    version:    u16,               // == 1
    topic_id:   [u8; 32],
    cursor:     u64,               // 0 = from the start of the retained window
    max_frames: u16,               // clamped to 256
}

SyncResponseV1 {
    version:     u16,              // == 1
    next_cursor: Option<u64>,      // None = caught up
    frames:      Vec<Vec<u8>>,     // verbatim SignedMessageV1 frames
}
```

Rules that keep this safe and bounded:

- **Frames are replayed verbatim**, then verified by the *same* code path as
  live gossip. A relaying peer cannot forge an offer it did not author, and
  authorship survives relaying (the frame carries the original author key and
  signature).
- Servers retain only `Offer` and `Provider` frames, newest-last, capped at
  `SYNC_LOG_CAP = 4096` per topic with oldest-first eviction. `Request`
  frames are transient and never retained.
- Pages are bounded twice: ≤ 256 frames and ≤ 512 KiB per response;
  requests are ≤ 4 KiB. A client stops after 8192 frames from one peer, and
  every read/write has a 15 s timeout.
- Replays hit the ordinary dedup cache, so a frame already seen through
  gossip is dropped rather than reprocessed.
- A peer with no session for the requested topic answers with an empty page:
  the control channel reveals nothing about other drops.
- Sync is best-effort. Failures are logged, never fatal, and live gossip
  keeps working regardless.

Interop: a v0.1 peer does not accept `/iroh-drop/1`, so sync simply fails
against it and the joiner falls back to `Request`-by-hash. Nothing about the
gossip wire format changed.

## Flows

**Publish.** Import bytes into the local `iroh-blobs` store → broadcast
`Offer` → broadcast `Provider(Available)` → members learn existence.

**Fetch (known offer).** Downloader pulls the blob from tracked providers
(tried in order, failures drop a provider and fall through to the next).
On completion: verify status, export if requested, broadcast
`Provider(Available)` — recipients become providers.

**Fetch (by hash, no offer).** Broadcast `Request` (after waiting ≤ 3 s for a
first neighbor). Any holder replies with `Provider(Available)` (rate-limited
per hash). Retry up to 3 rounds, re-ordering providers each time.

**Join (late).** Subscribe to the topic → in parallel, sync from bootstrap
peers (above) → the inventory arrives with names, sizes and providers intact,
so the joiner can fetch by name instead of needing a hash out of band.

**Leave.** Graceful: `Provider(Withdrawing)` for each served blob, then stop.
Abrupt: nothing — remaining providers cover the drop (see
`examples/four_peer_drop.rs`).

## Tickets

```text
drop1 + base32(postcard(DropTicketV1))   // RFC 4648 base32, lowercase, no padding

DropTicketV1 {
    version:         u16,               // == 1
    topic_id:        [u8; 32],
    bootstrap_nodes: Vec<EndpointAddr>, // ≤ 16
    options: { auto_fetch_recommended: bool, display_name: Option<String> },
}
```

Tickets are bearer capabilities and ≤ 8 KiB on the wire. A refreshed ticket
from a *live* peer is the reliable way to bring in late joiners; in offline
mode the bootstrap entry must carry full socket addresses (there is no
address lookup service).

## Ordering of self-asserted state

`Provider` carries `announced_at_ms`, the author's own clock:

- Receivers keep only the **newest assertion per (author, blob)** and ignore
  older ones. Withdrawn providers are kept as tombstones.
- This matters because relayed history has no global order: a catch-up log
  that ends before a withdrawal would otherwise resurrect a provider that has
  left. Timestamps make the outcome independent of the relay path.
- Trusting the author's clock is safe *because the claim is about the author
  itself*. A peer can lie about its own availability with or without a
  timestamp; it cannot use one to affect anybody else's state.

## Resource limits

Gossip has no admission control, so the protocol enforces its own. Every limit
below is a defence against a peer that is hostile, broken, or merely
enthusiastic:

| Limit | Value | Protects |
|---|---|---|
| frame size | 64 KiB | memory, verification cost |
| body size | frame − 1 KiB | same |
| name / metadata | 255 B / 16 entries × (64 B, 512 B) | display paths, memory |
| offers per session | 4096, least-recently-seen evicted | memory |
| offers per author | 512 | one peer crowding out the rest |
| aliases per blob | 16 | memory |
| providers per blob | 64, worst-first eviction | memory |
| known peers | 1024 | memory |
| retained history | 4096 frames (≤ 256 of unknown kinds) | memory, amplification |
| inbound messages | 64 burst, 16/s per peer | CPU (signature checks) |
| request answers | 8 burst, 1/s per peer, 10 s per blob | bandwidth amplification |
| sync pages | 32 burst, 4/s per peer, 8 concurrent servers | bandwidth amplification |

Two ordering rules make these effective: **cheap checks precede expensive
ones** (size, then rate limit, then dedup, then signature verification), and
**limits precede storage** (quotas are checked before an offer is recorded).

## Discovery records (outside the wire format)

Tickets may name bootstrap peers by id alone, with no socket addresses. Such a
ticket is only usable where the joiner can resolve an id:

| Situation | Resolver |
|---|---|
| online (`presets::N0`) | pkarr publish + pkarr/DNS resolve |
| local network (`StackOptions::mdns`) | mDNS via `iroh-mdns-address-lookup` |
| offline, no mDNS | nothing — the ticket must carry full addresses |

Independently, iroh address records can carry a short untrusted string
(`UserData`, ≤ 245 bytes). `iroh-drop` neither writes nor interprets it by
itself; `DropStack::advertise` exposes it so higher layers can, and the SDK's
`nearby` module puts a **short ticket** there to advertise a drop on a LAN.

Consequences worth stating plainly: a discovery record is a broadcast to
whoever can see it (the local network for mDNS, the pkarr service when online),
and a ticket is a bearer capability. Advertising one therefore grants the drop
to that audience. It is opt-in, per session, and revoked by stopping the
advertisement — there is no member removal in v1.

## Metadata is for higher layers

`Offer.metadata` is a bounded, untrusted `BTreeMap<String, String>` (≤ 16
entries, keys ≤ 64 B, values ≤ 512 B). The protocol never interprets it. It
exists so conventions can evolve without wire versions — for example the SDK's
collection hints:

| Key | Meaning |
|---|---|
| `collection.members` | number of files in a collection |
| `collection.total_bytes` | total size of those files |

Consumers must treat every entry as a hint from an untrusted peer: parse
defensively, fall back to protocol-level facts (`size`, verified bytes), and
never size a buffer or a policy decision on it. Media types work the same
way; `application/vnd.iroh-drop.collection+json` marks a collection manifest
and is a *convention*, not a wire feature.

## Protocol manifest

| Field | Value |
|---|---|
| name | `iroh-drop` |
| version | `0.4` (`WIRE_VERSION = 2` on the wire) |
| topic bootstrap | out-of-band `drop1` ticket |
| message framing | postcard, additive-tolerant, kind-tagged bodies, 64 KiB cap |
| auth | ed25519 signatures over `b"iroh-drop/v2/message/" \|\| payload` |
| content identity | BLAKE3 hash via `iroh-blobs` (32 bytes) |
| control ALPN | `/iroh-drop/1` (Hello + catch-up sync, op-tagged) |
| sync framing | postcard request/response, one bi-stream per page, ≤ 256 frames and ≤ 512 KiB per page |
| default policy | manual fetch; auto-fetch opt-in ≤ 500 MiB/blob, ≤ 3 concurrent, ≤ 2 GiB total |
