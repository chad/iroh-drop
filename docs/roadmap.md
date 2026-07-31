# iroh-drop roadmap

North star: **the announce-fetch-replicate pattern as a minimal, embeddable
protocol** — with a hard boundary between the protocol and the UX built on it.

## The boundary rule

```text
┌────────────────────────────────────────────────┐
│ apps: cli · daemon · mcp/agents · tui/gui      │  UX layer
├────────────────────────────────────────────────┤
│ iroh-drop-sdk                                  │  UX layer (conventions,
│   collections · config · orchestration         │  no wire logic)
├────────────────────────────────────────────────┤
│ iroh-drop                                      │  PROTOCOL
│   wire · sessions · policy · providers · sync  │
├────────────────────────────────────────────────┤
│ iroh · iroh-gossip · iroh-blobs                │
└────────────────────────────────────────────────┘
```

- The **protocol crate** owns: what a message *is*, what a valid offer *is*,
  byte verification, authorship (signatures), resource-protection limits
  (frame size, metadata bounds, policy gates), and provider tracking.
- The **SDK/apps** own: naming, display, when to auto-fetch (policy *values*),
  collections (a media-type *convention*, never wire-enforced), persistence
  formats, config, notifications, address books, daemon APIs.
- Test of the boundary: a third party must be able to build the daemon, an
  MCP server, or a GUI using only the public protocol API. If a UX feature
  needs new bytes on the wire, it goes through the protocol versioning rules
  below — otherwise it does not belong in the protocol crate.

## Protocol versioning governance

- `WIRE_VERSION` stays `1`. Two extension paths, both used already:
  - **additive optional fields** on postcard structs (old readers tolerate
    trailing bytes) — preferred for v1.x features.
  - **new capabilities on the control ALPN** `/iroh-drop/1` — the sync
    protocol is the first; each control request carries its own version.
- Adding a `MessageBodyV1` variant is *breaking* for old decoders (clean
  reject): reserve for a v2 family (encrypted envelopes, offer tombstones).
- Cross-version interop is tested by fixture: golden message/ticket/sync
  vectors live in `crates/iroh-drop/tests/fixtures/` and must keep decoding.

## Tracks

### Protocol track

| # | Feature | Wire delta | Status |
|---|---|---|---|
| P1 | **Catch-up sync**: late joiners pull the signed offer/provider log from any member over the control ALPN and replay it through the standard verify path. Kills the "no gossip history" limitation. | new ALPN usage; gossip unchanged | ✅ v0.2 — `src/sync.rs`, `state.rs` retained frames, `tests/catch_up_sync.rs` |
| P2 | **Persistent identity** (builder-level; stable `EndpointId` across restarts — prerequisite for allowlists, reputation, "same peer" UX) | none | ✅ v0.2 — `StackOptions::identity_path` |
| P7 | **Kind-tagged bodies**: unknown message kinds are ignored *and relayed* instead of poisoning a frame. Wire version 2. | v2 frame layout | ✅ v0.4 — `BodyEnvelopeV1` |
| P8 | **Op-tagged control channel + `Hello`**: capability discovery, clean "unsupported operation" answers, new ops without version bumps. | additive on `/iroh-drop/1` | ✅ v0.4 |
| P9 | **Bounded state and flood control**: offer/provider/peer/alias caps with eviction, per-author quotas, per-peer token buckets, capped concurrent history serving. | none | ✅ v0.4 — `limits.rs`, `tests/hostile_peers.rs` |
| P10 | **Ordered self-asserted state**: `Provider.announced_at_ms` makes withdrawals survive stale relays. | additive field | ✅ v0.4 |
| P3 | **Swarm fetch**: split large blobs into chunk ranges pulled from multiple providers in parallel (`ChunkRanges` exists in iroh-blobs; needs orchestration over ranged `GetRequest`s). Spike first; fall back to today's sequential failover. | none (blobs-protocol usage) | planned |
| P4 | **Offer series + TTL**: optional `series`/`sequence` fields (author-scoped supersede — "new version of report.pdf") and `expires_at_ms` hint. Additive fields, v1.x-safe. | additive fields | planned |
| P5 | **Private drops** (v2): ticket v2 carries a symmetric drop key; signed-then-encrypted frames (XChaCha20-Poly1305, HKDF(drop_key, topic_id)). Rotation v2.0 = new drop; rekey later. Boring primitives only. | v2 envelope | planned |
| P6 | **Offer tombstone** (v2): author-signed withdrawal variant. | v2 variant | planned |

### UX track (SDK + apps)

| # | Feature | Status |
|---|---|---|
| U1 | **`iroh-drop-sdk` crate**: config files, collections (recursive `add`, materialized `get`), inventory picking. Zero wire logic; depends only on `iroh-drop`'s public API. | ✅ v0.2 |
| U2 | **One-shot pair**: `iroh-drop share <path>` / `iroh-drop receive <ticket> [pick]` — sendme-grade ergonomics, but N receivers and publisher-independent. Plus numbered picks, auto-listing on join, XDG config, human event log. | ✅ v0.2 |
| U3 | **Collections** convention: manifest blob + `application/vnd.iroh-drop.collection+json` media type; one offer per tree, member count and total size carried in offer metadata, fetch materializes the tree with path-traversal defence. | ✅ v0.2 |
| U4 | **Daemon + control API**: long-lived process owns endpoint/sessions; CLI becomes a thin client; JSONL event stream; progress bars; `watch` view; drop address book with auto-rejoin. | ✅ core — `iroh-dropd` + `crates/iroh-drop-daemon`: JSONL over UDS (`0700` dir), tasks, coalesced progress, replayable events, consent asks, and `iroh-drop send/get/watch/drops` as thin clients. Remaining: Windows named pipes, address book, auto-rejoin. See `docs/daemon-api.md` |
| U5 | **Agent surface**: MCP server crate over the daemon API (`list_drops`, `list_offers`, `publish`, `fetch`, `events`). | planned |
| U6 | **GUI** on the daemon API. | ✅ first cut — `iroh-drop-app` (egui): drag-to-send, link + QR, accept/decline cards, progress, "still sharing". All logic lives in `bridge.rs` and is tested headlessly against real daemons. Remaining: packaging, signing, tray/menu-bar, a TUI if anyone wants one |
| U7 | **mDNS address lookup** via `iroh-mdns-address-lookup` (0.4) — reach a peer by id on a LAN with no relays and no addresses in the ticket. | ✅ v0.3 — `StackOptions::mdns`, implied by `--offline` |
| U8 | **QR ticket** in the terminal (`share --qr`) — same room, zero typing, no protocol or security change. | ✅ v0.3 |
| U9 | **Short tickets** (now the default): name peers by id, let discovery find addresses. 116 chars vs 145–227, and immune to changing IPs. | ✅ v0.3 — `DropSession::short_ticket`, `tests/short_tickets.rs` |
| U10 | **Saved rooms**: `share --room team`, `receive --room team`, `rooms`. One ticket ever, then names forever; each join refreshes the room with a ticket that includes you. | ✅ v0.3 — `sdk::rooms` |
| U11 | **LAN drop discovery**: `share --lan` puts a short ticket in our mDNS record; `nearby` lists drops and `receive --nearby <#>` joins one. Opt-in per share, with an explicit broadcast warning. | ✅ v0.3 — `sdk::nearby` |
| U13 | **Embedding API**: `DropStack::from_parts` + `sync_handler()` so an app with its own endpoint/router/gossip/blobs can add drops. | ✅ v0.4 |
| U14 | **`OfferDecider` hook**: allowlists, prompts, per-project quotas — judgement in the app, limits in the protocol. | ✅ v0.4 |
| U15 | **`mdns` feature flag**: local discovery is opt-in at compile time, so embedders do not pay for multicast machinery. | ✅ v0.4 |
| U16 | **Real links**: `iroh-drop://receive/<ticket>` registered by the app, so what you hand someone is clickable and the word "ticket" never surfaces. An optional `--link-base` adds an `https` form with the ticket in the *fragment*, so a static page can hand it off and still learn nothing. | ✅ — `service::app_link`, `CFBundleURLTypes`, `.onOpenURL` |
| U17 | **Native macOS app**: SwiftUI over the control socket — no FFI, no bindings, helper stays a separate process so it outlives the window. | ✅ first cut — `macos/Sources`, `packaging/macos/build_native.sh` |
| U12 | **Word codes** (`7-crossover-clockwork`): rendezvous over a code-derived topic, ticket delivered encrypted. | blocked — see below |

## Phasing

- **v0.2 (shipped)**: P1, P2, U1–U3. Protocol gained catch-up sync and
  persistent identity; the SDK crate established the boundary; the CLI gained
  `share`/`receive`, numbered picks, folders, and a config file. Wire format
  unchanged (`WIRE_VERSION = 1`).
- **v0.3 (shipped)**: onboarding — U7 (mDNS), U8 (QR), U9 (short tickets by
  default), U10 (rooms), U11 (LAN drop discovery). Nothing here touched the
  wire: it is all naming, discovery, and transport of one string.
- **v0.4 (shipped)**: extensibility and hardening — P7–P10, U13–U15, plus
  `#[non_exhaustive]` on public enums and `parking_lot` locks (no poisoning
  cascades). This is the last comfortable moment for a frame-layout change,
  which is why kind tagging happened now.
- **v0.4.x**: U4 daemon + control API; P4 series/TTL; `watch` UX.
  The daemon is the gate on everything decentralized: without a process that
  outlives a terminal, no peer is durable and "every recipient becomes a
  provider" stays theoretical. Contacts and per-transfer advertisement are
  polish on top of a network that already works, so they queue behind it.
- **v0.5**: P3 swarm fetch (spike first); U5 MCP server.
- **v0.6**: P5/P6 v2 family (private drops, tombstones); U6 TUI. Private
  drops are also what would make `--lan` safe on untrusted networks, and are
  a prerequisite for revisiting U12.

Each phase ships usable value; protocol work is front-loaded only where it
unblocks UX (sync unblocks late-join UX; series unblocks watch).

## Getting connected without weird strings

The ticket is long because it does three separate jobs. Naming them separately
is what makes this tractable — mDNS only does the first one:

| Job | Today | Better |
|---|---|---|
| **Address discovery** — how do I reach peer `a22999…`? | online: pkarr/DNS (an id alone is dialable, verified in `tests/online_short_ticket.rs`); offline: full socket addresses baked into the ticket | mDNS on a LAN (U7) removes the baked-in addresses; U9 then shortens every ticket to a topic + an id |
| **Drop discovery** — which drop, and what is in it? | the ticket carries the topic id; nothing else advertises drops | LAN advertisement (U11) or a rendezvous code (U12) |
| **Capability** — who is allowed in? | the ticket *is* the capability (bearer) | unchanged for U7–U10; U11/U12 must not hand the capability to the whole LAN or to a code guesser |

So mDNS keeps its place: it is the layer-1 answer, and it is the one that
fixes our offline story. It just cannot shrink the ticket by itself, because
the ticket also names the drop and grants access to it.

### How U11 was gated (shipped)

Advertising a drop on a LAN *is* handing out its capability: a ticket is a
bearer token, and mDNS is a broadcast. Rather than pretend otherwise, `--lan`
is opt-in per share, lives only as long as the process, and prints exactly what
it does ("anyone here can list and receive these files"). No PIN scheme was
added, because a PIN short enough to type is brute-forceable by anyone already
on the network — the honest framing is "this network is trusted enough", which
is true at home, in an office, or at a workshop, and false at an airport.

### Why U12 (word codes) is blocked, not just unscheduled

Two independent problems, and the second is the hard one:

1. *Crypto*: a speakable code is low-entropy, so the exchange needs a PAKE
   (SPAKE2-style, e.g. the `spake2` crate magic-wormhole uses) rather than a
   KDF — otherwise anyone who reaches the rendezvous can brute-force offline.
2. *Rendezvous*: **iroh-gossip has no way for two strangers to meet.**
   Joining a topic requires at least one bootstrap peer address, so a
   code-derived topic id is not enough — there is nobody to bootstrap from.
   Magic-wormhole solves this with a mailbox server; we would need one too, or
   a DHT (`iroh-dht-experiment` is 0.1.1 and experimental).

So U12 needs infrastructure, not just code. On a LAN, U11 already provides the
rendezvous mDNS gives us for free — which is why it shipped first.

## Success metrics

- Zero-to-received-file in < 60 s, two commands (`share` / `receive`). ✅
- Late joiner sees full inventory **by name** within seconds — no hash
  needed. ✅ (`tests/catch_up_sync.rs`, sub-second on localhost)
- 1 GiB fetch saturates the link from 2+ providers (after P3).
- An agent completes "fetch dataset X from drop Y" via MCP with no shell.
- Second and later exchanges with the same person need **no string at all**
  (U10), and first-time exchanges in one room need no typing (U8/U11).

## Non-goals

- No DHT, no global namespace, no public discovery: drops are
  ticket-scoped groups.
- No anonymity network properties (see Freenet/GNUnet for that).
- No mutable content: the hash stays the identity; series (P4) point at new
  immutable blobs.
- Policy *enforcement* never moves to the SDK; the SDK only picks values.

## What v0.2 deliberately did not do

- No new gossip message variants: everything additive rode existing fields
  (`Offer.metadata`) or the reserved control ALPN, so v0.1 peers still
  interoperate for publish/fetch.
- No daemon: the process is still the peer. `share` and `receive` are
  foreground commands, which keeps lifetime and teardown honest.
- No trust decisions moved into the SDK: policy limits stay in the protocol
  crate; the SDK only chooses values and displays results.

## Still open after v0.4

- **Transport coupling**: sessions hold `GossipSender`/`GossipReceiver`
  directly. A `DropTransport` trait (gossip as one implementation) would allow
  alternative carriers and — more usefully day to day — an in-memory transport
  that makes the suite deterministic and fast. Today's `fetch_flow` spends ~30 s
  in gossip mesh formation, not in protocol logic.
- **Conformance fixtures**: golden frames/tickets/control messages checked in,
  so format drift is caught by a test rather than by a peer. Wire version 2 is
  the right thing to freeze.
- **Streaming exports**: `FetchOutput` writes files; a caller-provided sink
  would let hosts stream into their own storage.
- **Async offer decisions**: `OfferDecider::decide` is synchronous and runs in
  the gossip receive loop, so it can never wait for a human. The daemon works
  around this correctly — record the offer, ask a UI out of band, then issue an
  ordinary manual fetch — and that is arguably the right layering anyway. But
  it means "prompt the user" is not expressible as a decider, which is worth
  saying out loud in the trait's docs.
- **First-class extension subscriptions**: unknown kinds are relayed, but an
  application still has to reach for `inject_raw_message` and event warnings to
  use them. An `on_kind(2001)` subscription is the missing sugar.

## Lessons the daemon work produced

Three bugs worth recording, because each was invisible until something forced
the issue:

1. **`Service::shutdown` left the endpoint open.** Stopping the sessions ends
   gossip participation, but the *blobs* protocol answers on the endpoint — so a
   "stopped" daemon kept serving every byte it held. Caught only by asserting
   the *provider identity* in `drop_outlives_its_publisher`; a test that merely
   checked "the file arrived" would have passed while proving nothing.
2. **One-shot commands must not register as `ui`.** Consent questions go to the
   first live UI client, so `iroh-drop send` connecting as a UI silently
   declined offers meant for somebody's `watch` in another terminal. Only
   `watch` is a UI now.
3. **`get` consenting *and* fetching downloaded everything twice**, which the
   collision-safe export names quietly hid rather than surfaced. Asking for a
   file is the consent; `get` is an observer.
4. **The GUI downloaded a file the user had declined.** Same root cause as (3),
   but with teeth: pasting a link both consented *and* issued an explicit fetch,
   so clicking "No thanks" stopped nothing. The fix is a rule worth stating
   plainly — **consent is about unsolicited offers**. Asking for a drop is
   consent for what is in it (for `RECEIVE_GRACE`, 60s), and answering the
   question is the *only* thing that ever starts a transfer.
5. **A blocking `AskHandler` froze the connection.** It ran on the client's
   reader task, so while a human deliberated, no other reply could be processed.
   Handlers now run on a blocking task, and UIs use `Client::asks()` +
   `Client::answer()` and never block at all.

6. **The "link" was never a link.** `drop.ticket` returned a literal
   `https://<host>/#...` — an unsubstituted placeholder — and every UI printed
   the bare ticket instead, so people were handed a wall of base32. Designing the
   link in a document is not the same as emitting one; the test now asserts the
   link is clickable and contains no `<`.
7. **The bundle overwrote its own app.** `CFBundleExecutable` was `iroh-drop`,
   and the CLI of the same name is installed into the same `Contents/MacOS`, so
   `install` clobbered the SwiftUI binary. Double-clicking ran the CLI, which
   printed usage and exited. The build script now refuses that name.
8. **SwiftUI ran `start()` twice**, opening two sockets and two reader threads,
   so every event arrived twice and one transfer drew two rows. One file on disk
   and two rows in the list is what told the difference between a display bug and
   a double download.

The pattern: the interesting failures were all about *who is allowed to speak
for the user*, not about bytes. None of them would have been caught by a test
that only asked "did the file arrive?" — they needed tests that asked "who
served it?", "how many copies?" and "what if they say no?".

## Risks

- **Control-ALPN abuse** (inventory flooding): sync responses are paginated,
  bounded, and per-peer; private drops (P5) will gate sync on key possession.
- **iroh-blobs ranged parallel fetch maturity**: P3 starts with a spike;
  sequential failover stays the fallback forever.
- **Scope creep into the protocol crate**: the boundary rule + a review
  checklist ("does this change bytes on the wire?") on every PR.
