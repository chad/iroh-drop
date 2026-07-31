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

## 3. New message kinds (additive, needs a kind number)

Bodies are `{ kind: u16, payload }`, and unknown kinds are ignored **and
relayed**, so a new kind propagates through peers that predate it.

```rust
use iroh_drop::message::{BodyEnvelopeV1, MessageV1};

let frame = MessageV1::with_envelope(BodyEnvelopeV1 {
    kind: 2001,                       // 2000.. is yours
    payload: postcard::to_allocvec(&MyBody { .. })?,
})
.encode(&secret)?;
session.inject_raw_message(frame.into()).await?;   // today's escape hatch
```

Ranges: `1..=999` core, `1000..=1999` experiments, `2000..` applications.
Receiving side: watch `DropEvent::ProtocolWarning { warning: UnknownKind { .. } }`
today; a first-class `on_kind` subscription is the natural next step.

Rules your kind must respect, because the protocol enforces them regardless:
frames stay under 64 KiB, signatures are checked before your code sees
anything, and rate limits apply per peer.

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
- **Do not** add a message kind for something a metadata key would carry.
  Kinds are forever; metadata is cheap.
