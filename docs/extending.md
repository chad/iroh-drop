# Extending iroh-drop

Four extension points, in increasing order of commitment. Prefer the earliest
one that does the job — the later ones cost interoperability.

## 1. Offer metadata (no wire change)

`Offer.metadata` is a bounded `BTreeMap<String, String>` the protocol never
interprets. Namespace your keys and treat every value as untrusted input.

```rust
let mut metadata = BTreeMap::new();
metadata.insert("myapp.project".into(), project_id.to_string());
session.publish_bytes_with(name, bytes, media_type, metadata).await?;
```

Existing conventions: `collection.members`, `collection.total_bytes` (the SDK's
directory trees). Peers that do not know a key simply do not see it.

## 2. Media types (no wire change)

A media type is a hint about what a blob *is*. The SDK marks directory
manifests with `application/vnd.iroh-drop.collection+json`; an application can
mark anything similarly and degrade gracefully elsewhere — a peer without the
convention just sees a blob.

## 3. Namespaced extension protocols (additive, no registration)

Kind numbers belong to the core spec — applications never mint one. Instead,
every application protocol rides kind `5`, the `ExtensionV1` envelope:

```
ExtensionV1 {
    namespace: [u8; 16],     // which application protocol this is
    local_kind: u32,         // your own message number
    schema_version: u16,     // your payload's schema version
    payload: Vec<u8>,        // yours entirely; opaque to iroh-drop
}
```

Pick a namespace that cannot collide: a UUID you generated, or the first 16
bytes of a hash of your protocol's fully-qualified name (presence uses
`SHA-256("iroh-drop-presence/v1")[:16]`). Publish it in your spec. Inside it,
`local_kind` and `schema_version` are entirely yours — evolve without asking
anyone.

```rust
// Send: signed, deduped, retained, and relayed like any core frame.
session.send_extension(namespace, 1 /* your kind */, 1 /* your schema */,
                       payload_bytes).await?;

// Receive: verified frames only, replaying what sync already pulled.
let mut rx = session.on_extension(namespace);
while let Ok(frame) = rx.recv().await {
    // frame.author is cryptographically attributable;
    // frame.payload is untrusted — validate before you trust it.
}
```

What stock peers do with your frames: verify the signature, bound them
under the extension budget (256 frames / 4 MiB, spent per session), relay
them to neighbors, and serve them from retained history to late joiners —
all without understanding the namespace. Frames with *unknown numeric
kinds* (future core versions) are likewise verified, retained, and relayed,
but are never delivered to `on_extension` subscribers — delivery is
namespace-addressed, so a bare number can never squat on a subscriber.

Rules your protocol must respect, because the protocol enforces them
regardless: frames stay under the body cap (~63 KiB), signatures are
checked before your code sees anything, rate limits apply per peer, and
retention is bounded — so keep frames small and treat delivery as
at-least-once (`ExtensionFrame::id` is your idempotency key). Your payload
is never validated by iroh-drop; in a private drop it is sealed like
everything else, in a public drop it is plaintext.

### The canonical example: `iroh-drop-presence`

`crates/iroh-drop-presence` is a complete, tiny extension — presence
beacons in `PRESENCE_NAMESPACE` — built using only this public API. Its
test (`tests/relay.rs`) is the composability proof: three peers in a line,
the middle one running stock `iroh-drop` with no knowledge of presence, and
the frame still (a) crosses it, (b) keeps it healthy, and (c) is served
from that peer's retained history to a fourth peer joining late. If you are
writing an extension, copy that crate's shape.

(The lower-level `DropSession::inject_raw_message` still exists as a
hidden test hook for forging *invalid* frames — hostile-input tests need
to bypass the signing path. It is not an extension mechanism.)

## 4. New control operations (additive, needs an op number)

The `/iroh-drop/1` control channel is `{ version, op, payload }` with
`Hello` (op 1) advertising what a peer supports. Unsupported ops get a clean
`op = 65535` answer rather than a dropped connection, so a client can detect a
capability and fall back. Adding an op does not need a version bump — it needs
an entry in `HelloV1::ops`.

## Hooks that need no wire changes at all

| Want | Use |
|---|---|
| custom admission (allowlists, prompts, quotas) | `OfferDecider` — runs after verification and policy, can only be stricter |
| different limits | `DropPolicy` |
| embed in an existing iroh app | `DropStack::from_parts` + `stack.sync_handler()` on your own router |
| read/write bytes yourself | `DropSession::store`, `read_bytes`, `import_path` |
| naming, trees, config, discovery UX | the SDK layer, or your own — see `docs/roadmap.md` |

## What not to do

- **Do not** put semantics in names. They are untrusted display strings, and
  two blobs can share one.
- **Do not** rely on `sent_at_ms` or `created_at_ms` for correctness; they are
  informational. `Provider.announced_at_ms` is ordering *only* for the
  author's own claims.
- **Do not** treat a ticket as authentication. It is a bearer capability:
  everyone holding it is a full member.
- **Do not** mint a numeric kind for anything, ever — use the extension
  envelope. And do not add an extension namespace for something a metadata
  key would carry: protocols are forever; metadata is cheap.
