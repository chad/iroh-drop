# iroh-drop wire protocol (message families 3 and 4)

Reference for the on-the-wire formats. Everything is implemented in
`crates/iroh-drop`; the message definitions live in `src/message.rs`, tickets
in `src/ticket.rs`. The **normative examples** are the golden bytes in
`crates/iroh-drop/tests/fixtures/` — conformance-tested, so they cannot
drift from what the code actually speaks.

## Terminology (five different "versions")

This protocol has several version numbers that are easy to conflate. They
are deliberately separate, and this document uses these names throughout:

| Name | Current | Meaning |
|---|---|---|
| `package_version` | crate semver | the Rust crate release; not on the wire |
| `message_family` | **3** public, **4** sealed | `MessageV1.version`; which signing domain and body rules a frame obeys |
| `control_envelope_version` | **3** | `ControlRequestV1/ControlResponseV1.version` on `/iroh-drop/1` |
| `ticket_schema_version` | **3** | `DropTicketV1.version`; the `drop2` prefix |
| `body_schema_version` | per kind | the "V1" in `OfferV1`, `SyncRequestV1`, … |

History: family 1 encoded bodies as a closed postcard enum (an unrecognized
variant made the whole frame undecodable). Family 2 (released in v0.1.x)
introduced kind-tagged body envelopes. Family 3 is family 2 with
**topic-bound signatures** — same body schemas, new signing input — and
family 4 is the sealed family. Families 3/4 reject 1/2 by version check;
the old ticket schemas are rejected likewise.

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
tolerated. Readers must reject: wrong `message_family`, undecodable bodies,
bad signatures, oversized frames, and over-limit names/metadata — without
disturbing the session (see `tests/malformed_messages.rs`).

### Frames and bodies

```text
SignedMessageV1 {                  // the frame put on the gossip topic
    author:   [u8; 32],            // ed25519 public key == iroh EndpointId
    payload:  Vec<u8>,             // postcard(MessageV1)
    signature: [u8; 64],           // ed25519 over SIGN_DOMAIN || topic_id || payload
}

// family 3 (public):  SIGN_DOMAIN = b"iroh-drop/v3/message/"
// family 4 (sealed):  SIGN_DOMAIN = b"iroh-drop/v4/message/"

MessageV1 {
    version:    u16,               // == message_family (3 or 4)
    id:         [u8; 16],          // random, for dedup (10k entries / 10 min)
    sent_at_ms: u64,               // informational only
    body:       BodyEnvelopeV1,
}

MessageBodyV1 = Offer | Provider | Request | Extension

OfferV1 {
    blob_hash:     BlobHash,               // [u8; 32], canonical identity
    name:          String,                 // untrusted display name, ≤ 255 B
    size:          u64,                    // advisory; blobs protocol confirms
    media_type:    Option<String>,         // advisory
    created_at_ms: Option<u64>,            // informational
    metadata:      BTreeMap<String, String> // ≤ 16 entries, keys ≤ 64 B, values ≤ 512 B
}

ProviderV1 {
    blob_hash:       BlobHash,
    state:           Available | Withdrawing,
    announced_at_ms: Option<u64>,  // author's own clock; see "Ordering"
}

RequestV1  { blob_hash: BlobHash }

ExtensionV1 {                      // body kind 5
    namespace:      [u8; 16],      // which application protocol this is
    local_kind:     u32,           // that protocol's own message number
    schema_version: u16,           // that protocol's payload schema version
    payload:        Vec<u8>,       // opaque, ≤ MAX_BODY_SIZE
}
```

Why signatures: gossip only tells you the *delivering neighbor*, not the
author, so authorship must be cryptographic. Why the **topic in the signing
input**: without it, a validly signed frame can be replayed into any other
drop whose members include the author (cross-topic replay). The domain
separates families; the topic separates drops. A frame valid in one drop is
invalid everywhere else, full stop.

Offers are keyed by hash: re-offering a known hash under a new name adds the
name as an alias. Different hashes may share a display name — names are
untrusted, and lookup by name is **ambiguous-is-an-error**: the resolver
returns the candidate hashes and never picks one.

### Body kinds

A body is a **kind tag plus opaque bytes**, not a closed enum:

```text
BodyEnvelopeV1 {
    kind:    u16,   // 1 = Offer, 2 = Provider, 3 = Request, 4 = SEALED (family 4),
                    // 5 = Extension
    payload: Vec<u8>,              // postcard of that kind's struct
}
```

Kind numbers are **globally assigned by this specification** (`1..=999`).
Applications never mint one: everything application-specific rides kind 5,
the `ExtensionV1` envelope, where a 16-byte `namespace` (a UUID, or the
truncated hash of a name you own) makes collisions impossible, and
`local_kind` + `schema_version` belong entirely to the application protocol.

Rules for receivers:

- An unknown numeric `kind` is **not an error**. Verify the signature,
  ignore the body, emit an observable warning, and keep the session running.
- Unknown kinds are still **relayable**: the frame stays intact and is
  retained (within the small extension budget) so this peer passes them
  along to peers that do understand them — this is how future core versions
  deploy through a mixed swarm. Application frames (kind 5) are retained
  under the same budget whether or not the peer serves the namespace.
- Unknown numeric kinds are **never delivered** to extension subscribers:
  delivery is namespace-addressed (`on_extension(namespace)`), so a bare
  number cannot squat on a subscriber.
- Unknown kinds are *not* exempt from anything else: signature, size, and
  rate limits all apply first.

### Verification and acceptance order

Every incoming frame — live gossip or replayed sync page — goes through the
same path, in this order:

1. **Size** (≤ 64 KiB), then **signature verification** against the session
   family's domain + topic (this is also where the family/version check
   happens, since the version lives inside the signed payload).
2. **Per-peer rate limit** (64 burst, 16/s). Excess frames are dropped with
   an observable warning — before any state is touched.
3. **Dedup** on `(author, id)`: first-verified-wins. A replayed or
   duplicated frame is dropped silently.
4. Only then: body validation, retention, policy, dispatch.

The order matters: verification precedes bookkeeping so rate-limit and
dedup state is only spent on authentic frames; dedup follows verification so
an attacker cannot poison the dedup cache with forgeries that evict genuine
entries.

## Catch-up sync (control ALPN)

Gossip has no history: a peer that joins after an offer was broadcast would
never hear about it. Sync closes that gap without weakening authentication.

Every control request is itself wrapped, for the same reason bodies are:

```text
ControlRequestV1  { version: u16, op: u16, payload: Vec<u8> }   // version == 3
ControlResponseV1 { version: u16, op: u16, payload: Vec<u8> }   // version == 3

op 1 = Hello        → HelloV1 { wire_versions, ops, message_kinds,
                                max_frames_per_page }
op 2 = SyncPage     → SyncRequestV1 / SyncResponseV1 (below)
op 65535            → "I do not implement that operation"
```

`Hello` is endpoint-global capability *discovery*: a peer states which wire
versions, control operations and message kinds it implements, so clients
negotiate instead of inferring from a version number. A version mismatch is
answered rather than dropped, so the other side learns what we speak. New
operations need neither a new ALPN nor a version bump.

A joiner opens `/iroh-drop/1` to each bootstrap peer from its ticket, says
hello, then pages through that peer's retained frames:

```text
SyncRequestV1 {                    // one bi-stream per page, EOF-delimited
    version:    u16,               // == 3
    topic_id:   [u8; 32],
    cursor:     u64,               // absolute; 0 = from the oldest retained
    max_frames: u16,               // clamped to 256
    key_proof:  Option<[u8; 32]>,  // required for sealed drops; see below
}

SyncResponseV1 {
    version:       u16,            // == 3
    next_cursor:   Option<u64>,    // None = caught up
    frames:        Vec<Vec<u8>>,   // verbatim SignedMessageV1 frames
    oldest_cursor: u64,            // absolute cursor of the oldest retained frame
    truncated:     bool,           // 0 < request.cursor < oldest_cursor
}
```

**Cursor semantics.** Cursors are absolute positions in the responder's
retained log, valid against that responder only. `cursor = 0` asks for
everything the responder still retains and is *not* truncation. A request
cursor in `(0, oldest_cursor)` means the frames the requester expected were
evicted: the responder sets `truncated = true` and serves from
`oldest_cursor`. Truncation is a signal, not an error — the requester has
permanently missed history *this peer* once held and should ask other peers
if it cares.

Rules that keep this safe and bounded:

- **Frames are replayed verbatim**, then verified by the *same* code path as
  live gossip (order above). A relaying peer cannot forge an offer it did
  not author, and authorship survives relaying.
- Servers retain `Offer`, `Provider`, and extension/unknown frames,
  newest-last. `Request` frames are transient and never retained. The
  retained log is bounded **twice** per topic: ≤ 4096 frames and ≤ 32 MiB,
  oldest-first eviction on whichever bound bites first.
- Pages are bounded twice: ≤ 256 frames and ≤ 512 KiB per response;
  requests are ≤ 4 KiB, and every read/write has a 15 s timeout.
- One catch-up attempt against one peer is bounded: ≤ 128 pages, ≤ 8192
  frames, ≤ 64 MiB imported, and the cursor must strictly advance. A peer
  serving trickle-sized pages cannot keep a requester talking forever; a
  bigger backlog simply takes another anti-entropy round (already-seen
  frames dedup cheaply).
- Replays hit the ordinary dedup cache, so a frame already seen through
  gossip is dropped rather than reprocessed.
- A peer with no session for the requested topic answers with an empty page:
  the control channel reveals nothing about other drops.
- Sync is best-effort. Failures are logged, never fatal, and live gossip
  keeps working regardless.

## Flows

**Publish.** Import bytes into the local `iroh-blobs` store → broadcast
`Offer` → broadcast `Provider(Available)` → members learn existence.

**Fetch (known offer).** Downloader pulls the blob from tracked providers
(tried in a deterministic order — fewest failures first, original publisher
preferred, endpoint id as the final tie-break; failures back off with a
growing cooldown and fall through to the next provider). On completion:
verify status, export if requested, broadcast `Provider(Available)` —
recipients become providers.

**Fetch (by hash, no offer).** Broadcast `Request` (after waiting ≤ 3 s for a
first neighbor). Any holder replies with `Provider(Available)` (rate-limited
per hash). Retry up to 3 rounds, re-ordering providers each time.

**Join (late).** Subscribe to the topic → in parallel, sync from bootstrap
peers (above) → the inventory arrives with names, sizes and providers intact,
so the joiner can fetch by name instead of needing a hash out of band.

**Reconnect (anti-entropy).** Join-time sync covers only the ticket's
bootstrap set and only that moment, so every neighbor-up also triggers a
pull from that neighbor (per-peer 60 s cooldown; replay is verified and
deduplicated like any sync). Both sides do this, so two members that were
never online at the same time still converge on reconnect without any
re-publish.

**Leave.** Graceful: `Provider(Withdrawing)` for each served blob, then stop.
Abrupt: nothing — remaining providers cover the drop (see
`examples/four_peer_drop.rs`).

## Tickets

```text
drop2 + base32(postcard(DropTicketV1))   // RFC 4648 base32, lowercase, no padding

DropTicketV1 {
    version:         u16,               // == ticket_schema_version (3)
    topic_id:        [u8; 32],
    bootstrap_nodes: Vec<TicketAddrV1>, // ≤ 16
    options: { auto_fetch_recommended: bool, display_name: Option<String> },
    mode:            u8,                // 0 = Public, 1 = Sealed
    drop_key:        Option<[u8; 32]>,
}

TicketAddrV1 {                          // iroh-drop-owned, additive-friendly
    endpoint_id: [u8; 32],
    relay_url:   Option<String>,        // ≤ 255 B
    direct_v4:   Vec<([u8; 4], u16)>,   // ≤ 8
    direct_v6:   Vec<([u8; 16], u16)>,  // ≤ 8
}
```

Rules:

- `version ≠ 3` is rejected (`UnsupportedVersion`). The legacy `drop1`
  prefix is still recognized — and then version-rejected — so an old ticket
  fails with a precise error instead of "not a ticket".
- `mode` is explicit and validated: `Public` must not carry a `drop_key`
  (a key on a public ticket is a construction error, `Malformed`), and
  `Sealed` may carry none.
- `Sealed` **without** a key is a *blind relay* ticket: the session verifies
  family-4 signatures, retains and relays live frames, and serves nothing
  it cannot read (sync is refused; publishing errors `NoDropKey`). This is
  how a helper peer can strengthen a private drop without holding its key.
- `TicketAddrV1` is deliberately not iroh's `EndpointAddr`: the ticket
  schema is drop-owned so iroh's address-type evolution can never silently
  change what a ticket means. Conversion happens once at the boundary.
- Tickets are bearer capabilities and ≤ 8 KiB on the wire. A refreshed
  ticket from a *live* peer is the reliable way to bring in late joiners;
  in offline mode the bootstrap entries must carry direct addresses (there
  is no address lookup service).

## Private drops (sealed message family 4)

A drop is **private** iff its ticket says `mode = Sealed`. Privacy is a
property of the whole session, selected once at join: a private session
speaks and accepts *only* family 4, a public session only family 3. There
are no mixed drops.

### Envelope

```text
frame = postcard(SignedMessageV1 { author, signature, payload })
                                     // unchanged outer shape
payload = postcard(MessageV1 {
    version: 4,
    id:        [u8; 16],             // random, dedup — plaintext
    sent_at_ms: u64,                 // informational — plaintext
    body: { kind: 4 (SEALED), payload: sealed },
})
sealed = nonce(24) || XChaCha20-Poly1305(key, nonce, inner, aad)
inner  = postcard(BodyEnvelopeV1 { kind: <real kind>, payload: <real body> })
aad    = author(32) || id(16)
key    = HKDF-SHA256(ikm = drop_key, salt = topic_id,
                     info = b"iroh-drop/v4/seal")   // 32 bytes
signature = ed25519 over b"iroh-drop/v4/message/" || topic_id || payload
```

Each family has its own signature domain, and both families bind the topic:
a sealed frame verifies under exactly one domain and exactly one drop. New
builds also *diagnose* the other direction: a family-4 session handed a
family-2/3 frame (and vice versa) reports `UnsupportedVersion(v)` with the
frame's true family, never a signature error — while still rejecting it.

The outer envelope stays signed-but-readable by design: any build parses
the frame, verifies authorship, and reads `version: 4`. Old builds reject
it as `UnsupportedVersion(4)`: clean, attributable, never misparsed. The
commitment to the plaintext is transitive through the AEAD, and a
verifiable outer signature is what makes clean version rejection possible.

- **Nonce**: 192-bit, random per frame. Never derived from the message id
  or a counter; drops are low-rate, random nonces are the boring choice.
- **AAD** binds the author and message id, so headers cannot be swapped
  between frames.
- **Topic binding** is double: the KDF salt (a frame sealed for one drop
  cannot be decrypted under another drop's key) and the outer signature
  (it cannot even verify elsewhere).
- The inner body is an ordinary `BodyEnvelopeV1`: all kinds — core and
  extension — work unchanged inside the seal, and every inner limit
  (`MAX_BODY_SIZE`, per-kind validation) is enforced after decryption
  exactly as in family 3.
- Dedup and retention operate on the **outer** frame (author, id; stored
  bytes are ciphertext). Key-less peers can relay and retain private
  frames without being able to read them — retention and flood-fill need
  no key. See *blind relay tickets* above.

### What is encrypted, what is not

Encrypted (key holders only): body kinds and payloads — offer names, sizes,
media types, blob hashes, provider state, requests, extension payloads.

Necessarily plaintext (visible to relays, gossip peers, and key-less holders
of the topic id): the topic id itself, author public keys, signatures,
message ids, timestamps, approximate frame sizes (ciphertext length ≈
plaintext length; no padding in this version), and message timing.

So, stated plainly: a private drop hides **what members say** — including
which blobs exist — not **who participates** (author keys are the member
list, visible to anyone who can see the traffic) nor that the drop exists
at all.

Content blobs are served by `iroh-blobs` under the hash-as-capability
model: the transfer is plaintext-but-hash-verified. In a private drop,
hashes appear only inside the seal, so an observer can neither learn them
from traffic nor fetch the content — blob confidentiality reduces to key
secrecy. In a public drop, hashes are public and anyone holding one can
fetch the bytes from any provider; integrity is always guaranteed by the
hash itself.

### Sync gating

Catch-up sync for a sealed drop requires proof of key possession, bound to
the connection and the request:

```text
sync_key  = HKDF-SHA256(ikm = drop_key, salt = topic_id,
                        info = b"iroh-drop/v4/sync")
key_proof = HMAC-SHA256(sync_key,
                        requester_id || responder_id
                        || postcard(SyncRequestV1 { .., key_proof: None }))
```

- The sync key is **key-separated** from the frame AEAD key (different HKDF
  info), so a sync proof can never be repurposed as a frame key or vice
  versa.
- The proof is **connection-bound**: it names both endpoints, so a proof
  captured from one connection cannot be replayed against a different
  responder or by a different requester. It is request-bound too, so it
  cannot be stripped onto a different page request.
- A sealed session's sync responder refuses a missing or invalid proof with
  an empty page (`frames: []`, `next_cursor: None`). A key-less actor
  pulling pages from a key-less *relay* receives only ciphertext either way
  — confidentiality never depended on the refusal; the refusal keeps
  membership metadata (who asks) and bandwidth spent on non-members honest.
  Blind relays therefore serve no sync at all: they cannot verify proofs,
  and they cannot read what they would serve.

### Rotation

There is no re-key in this version: **rotate = new drop**. To exclude a
key holder, create a new drop (new topic, new key) and re-offer what
should survive. `HelloV1.wire_versions` advertises `[4]` from sealed
sessions so the limitation is discoverable rather than silent.

### Threat model

Private drops protect drop content — everything inside bodies, including
the existence and hashes of blobs — against everyone without the key:
passive relays, the gossip infrastructure, network observers, and actors
who learned only the topic id. They do **not** hide participation (author
keys, timing, sizes) from those same observers. Against a **key holder**
they add nothing — any member can read everything and can copy content
out; membership is the security boundary. `--lan` with a private drop is
safe on untrusted networks in the following exact sense: mDNS advertises
the endpoint's existence and addresses (as it does for any iroh node),
never the topic id or drop content — presence leaks, content does not.

## Ordering of self-asserted state

`Provider` carries `announced_at_ms`, the author's own clock:

- Receivers keep only the **newest assertion per (author, blob)** and ignore
  older ones. Ties are broken by the frame's message id, so every peer
  converges on the same winner no matter what order its relays delivered
  the two assertions in. Withdrawn providers are kept as tombstones.
- This matters because relayed history has no global order: a catch-up log
  that ends before a withdrawal would otherwise resurrect a provider that
  has left. Timestamps make the outcome independent of the relay path.
- The guarantee is bounded by retention: if the *newer* assertion itself
  has been evicted from every peer a joiner can reach, the joiner can only
  learn the older one. Stale availability heals in practice — live peers
  re-announce, and the provider-timeout policy sweeps providers that go
  silent — but the invariant this section promises is per-author ordering,
  not completeness beyond the retained window.
- Trusting the author's clock is safe *because the claim is about the
  author itself*. A peer can lie about its own availability with or without
  a timestamp; it cannot use one to affect anybody else's state.

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
| providers per blob | 64, worst-first eviction (id tie-break) | memory |
| known peers | 1024 | memory |
| retained history per topic | 4096 frames **and** 32 MiB, oldest-first | memory, amplification |
| extension/unknown retention | 256 frames **and** 4 MiB, spent per session | memory, amplification |
| live sessions per instance | 64 (`PolicyError::TooManySessions`) | memory, one topic at a time |
| inbound messages | 64 burst, 16/s per peer | CPU (signature checks) |
| request answers | 8 burst, 1/s per peer, 10 s per blob | bandwidth amplification |
| sync pages (server) | 32 burst, 4/s per peer, 8 concurrent servers | bandwidth amplification |
| sync attempt (client) | 128 pages / 8192 frames / 64 MiB per peer | livelock, memory |
| sync wire sizes | 4 KiB request, 512 KiB / 256-frame page, 15 s IO | bandwidth, livelock |
| ticket size | 8 KiB; ≤ 16 bootstrap addrs, ≤ 8 direct addrs/family | parsing cost |

Two ordering rules make these effective: **verification precedes
bookkeeping** (rate-limit and dedup state is only spent on authentic
frames — see *Verification and acceptance order*), and **limits precede
storage** (quotas are checked before an offer is recorded). An
endpoint-global byte budget across all sessions is policy territory, not
wire format; `DropPolicy` is where such knobs belong.

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
whoever can see it (the local network for mDNS, the pkarr service when
online), and a ticket is a bearer capability. Advertising one therefore
grants the drop to that audience. It is opt-in, per session, and there is
no member removal: you stop advertising, and exclusion means rotation —
a new drop with a new ticket (see *Rotation*).

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
and is a *convention*, not a wire feature. Application protocols that need
more than hints belong in an extension namespace (kind 5), never in a
minted kind number — see `docs/extending.md`.

## Protocol manifest

| Field | Value |
|---|---|
| name | `iroh-drop` |
| message families | 3 (public), 4 (sealed) — `MessageV1.version` |
| control envelope | version 3 on `/iroh-drop/1` (Hello + catch-up sync, op-tagged) |
| ticket schema | version 3, `drop2` + base32(postcard), drop-owned `TicketAddrV1` |
| message framing | postcard, additive-tolerant, kind-tagged bodies, 64 KiB cap |
| auth | ed25519 over `b"iroh-drop/v3/message/" \|\| topic \|\| payload` (family 3) or `b"iroh-drop/v4/message/" \|\| topic \|\| payload` (family 4) |
| privacy | family 4: XChaCha20-Poly1305, key = HKDF-SHA256(drop_key, topic, `iroh-drop/v4/seal`); see *Private drops* |
| sync gating | HMAC over HKDF-separated sync key (`iroh-drop/v4/sync`), bound to both endpoint ids and the exact request |
| content identity | BLAKE3 hash via `iroh-blobs` (32 bytes) |
| sync framing | postcard request/response, one bi-stream per page, ≤ 256 frames and ≤ 512 KiB per page |
| default policy | manual fetch; auto-fetch opt-in ≤ 500 MiB/blob, ≤ 3 concurrent, ≤ 2 GiB total |
