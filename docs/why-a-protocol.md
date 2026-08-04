# Why iroh-drop is a protocol

iroh-drop looks like an app — AirDrop for people who don't both use Apple —
but the durable artifact is a **protocol crate** with three apps hanging off
it. This document is the "why" story: what went on the wire and what didn't,
what broke along the way, and the rules the project now runs on. It is aimed
at people who know [iroh](https://github.com/n0-computer/iroh) and are
deciding what to put in *their* protocol crate versus *their* SDK.

## The one idea

iroh gives you gossip topics, verified blob transfer, NAT traversal, and
stable identities. What it does not give you is the social contract of file
sharing: *someone announces a file, someone else chooses to take it.*

iroh-drop adds exactly that, as wire messages:

1. **Offer** — "here is a blob hash, a name, a size" ([`MessageBodyV1::Offer`](../crates/iroh-drop/src/message.rs)).
2. **Provider** — "I have the bytes for that hash" ([`MessageBodyV1::Provider`](../crates/iroh-drop/src/message.rs)).
3. **Consent-gated fetch** — nothing moves until a human (or an explicit
   one-item request) says yes, enforced in the
   [daemon's ask routing](../crates/iroh-drop-daemon/src/service.rs) and the
   [SDK policy](../crates/iroh-drop-sdk/src/lib.rs).

Everything else — gossip fanout, hole-punching, QUIC, hash-verified transfer,
endpoint identity — is iroh, used as shipped. The protocol crate
[has no networking code of its own beyond composing those](../crates/iroh-drop/src/lib.rs).

## What earned bytes on the wire

The boundary rule: **the protocol crate owns what peers must agree on; the
SDK owns what one peer can decide alone.**

Wire: message envelopes, signatures, tickets, the sync protocol, limits that
keep strangers from costing you memory. Not wire: address books and display
names ([`peer.label` is local-only](../docs/daemon-api.md)), export path
sanitization, collision-safe filenames, download directories, UIs.

The test is not philosophical. It is: **could a third party build the daemon
from the public API?** Three consumers prove it today — the
[CLI](../crates/iroh-drop-cli/src/main.rs), the
[GUI](../crates/iroh-drop-gui/src/bridge.rs) (SwiftUI, talks JSONL to the
daemon exactly like the CLI does), and an
[MCP server for agents](../crates/iroh-drop-mcp/src/lib.rs) whose entire
iroh-drop dependency list is `iroh-drop-daemon`. The agent needed zero new
daemon capabilities — when a consumer can get by with less, the boundary is
holding.

## Poisoned frames: the v1→v2 lesson

Version 1 of the wire used closed enums for message bodies: every kind known,
everything else a parse error. That is a flag day farm — any new feature
means every peer must upgrade first, because an old peer *poisons* a frame it
can't parse instead of relaying it.

v2 replaced closed enums with a **kind-tagged open envelope**
([`MessageBodyV1`](../crates/iroh-drop/src/message.rs)): known kinds decode,
unknown kinds round-trip verbatim as bytes. An old peer verifies the
signature, retains the frame under a bounded budget, and floods it onward.
Consequences, all tested:

- New features deploy to upgraded peers while old peers carry the traffic —
  the [presence extension](../crates/iroh-drop-presence/src/lib.rs) rides
  through a stock peer that doesn't understand it, proven by
  [`extension_frames_relay_through_and_are_retained_by_stock_peers`](../crates/iroh-drop-presence/tests/relay.rs).
- Applications never mint kind numbers. Core kinds are assigned by the spec;
  everything else rides the **namespaced extension envelope**
  ([`ExtensionV1`](../crates/iroh-drop/src/message.rs)): a 16-byte namespace,
  a local kind, a schema version. Two apps can't collide, and an app can't
  squat on a subscriber by picking a number first. See
  [extending.md](extending.md).

The v0.2 hardening made one more deliberate break on the same principle —
signatures now bind the topic (a frame verifies under exactly one drop), and
public/sealed frames are separate versioned families (3 and 4) so old and
new peers reject each other *by version* instead of by inscrutable signature
failures. One break, pre-freeze, rather than additive workarounds forever.

## Why every frame is signed

In gossip, **the peer delivering a message is not its author.** Your
neighbor is a relay; the frame will be re-delivered by strangers you will
never meet. So trust attaches to the frame, not the connection: every frame
is Ed25519-signed over `domain || topic || payload`
([`signing_input`](../crates/iroh-drop/src/message.rs)), and *replay is
verbatim* — sync pages serve the original signed bytes, so a late joiner
verifies the author, not the intermediary
([`sync_page`](../crates/iroh-drop/src/sync.rs)).

That decision is what makes relay-of-unknown safe: an old peer can carry
frames it cannot parse because it never has to trust them, only check a
signature and pass bytes along. For sealed (private) drops, the same logic
produced the **blind relay**: a member without the drop key verifies outer
signatures and relays ciphertext it cannot read
([`MessageV1::verify_sealed_outer`](../crates/iroh-drop/src/message.rs)).

## Cheap checks first, limits before storage

Every byte that arrives from the network is hostile until the last check
passes, and the checks are ordered by cost: size caps → signature →
per-peer rate limit → dedup → body validation → retention. The expensive
work (crypto) happens before the stateful work (memory), and nothing enters
a data structure without a bound already attached. The full order and the
limit table are normative in [protocol.md](protocol.md); the limits
themselves are one readable file,
[`limits.rs`](../crates/iroh-drop/src/limits.rs) — offers, providers, peers,
aliases, retained history, extension bytes, sessions, each capped by count
*and* bytes.

The fuzzers enforce the posture mechanically: every decode path is a
[cargo-fuzz target](../crates/iroh-drop/fuzz/fuzz_targets/) running in CI,
seeded from the golden-byte fixtures. A panic on hostile input is a red
build.

## Consent is the product

The invariant: **answering the question is the only thing that ever starts a
transfer.** Not joining a drop, not seeing an offer, not being online.
Asking for a specific thing is consent for that thing; nothing else is
consent for anything.

This sounds simple and failed eight distinct ways in practice. They are
recorded in full in [roadmap.md](roadmap.md); the short versions:

1. A "stopped" daemon kept serving bytes — the blobs protocol still answered
   on the open endpoint. Proven by asserting *provider identity* in
   [`drop_outlives_its_publisher`](../crates/iroh-drop-daemon/tests/socket_transport.rs),
   not "the file arrived".
2. One-shot CLI commands registered as `ui` and silently absorbed consent
   questions meant for a real UI. Roles exist because of this
   ([`Hello`](../crates/iroh-drop-daemon/src/frame.rs)).
3. `get` both consented *and* fetched — everything downloaded twice. Now
   `get` is an observer; asking is the consent.
4. The GUI downloaded a file the user had *declined*: pasting a link
   consented and auto-fetched, so "No thanks" stopped nothing.
5. A blocking ask handler froze the whole control connection while a human
   deliberated. Handlers run off the reader task now
   ([`Client::asks` + `Client::answer`](../crates/iroh-drop-daemon/src/client.rs)).
6. `drop.ticket` returned a URL with a literal `<host>` placeholder — the
   "link" was never a link until a test asserted it was clickable.
7. The macOS bundle overwrote its own executable with the CLI of the same
   name. The build script refuses that name now.
8. SwiftUI ran `start()` twice: two sockets, every event delivered twice.

Lesson 2's generalization is the agent rule: **an MCP client is not a UI.**
The agent connects with the control role, is never asked a consent question,
and its explicit `fetch` is the consent for that one item — pinned by
[`agent_fetches_dataset_via_mcp`](../crates/iroh-drop-mcp/tests/agent_fetch.rs),
which also proves an unsolicited offer sits unfetched while only the agent
is watching.

## Honest refusals

- **`--lan` shares the ticket with the local network.** Nearby discovery is
  convenience over a bearer capability; it is documented as such wherever
  the flag appears, not buried.
- **Member removal is rotation.** A ticket is a bearer capability; you
  cannot un-copy it. Excluding someone from a private drop means a new drop
  with a new key — the protocol says "rotate", not "revoke"
  ([protocol.md](protocol.md)).
- **Stranger-to-stranger sharing without a channel (U12) is unsolved.** It
  wants PAKE + rendezvous, and shipping a weak version would be worse than
  none. It stays on the [roadmap](roadmap.md), blocked, on purpose.

## The takeaway

If you are building on iroh and asking "protocol crate or SDK?", the
answers here were: put *agreement* on the wire (envelopes, signatures,
tickets, sync, limits) and *preference* in the SDK (names, paths, budgets,
UI); make frames carry their own trust because relays are not authors;
never let a parse failure kill a relayable frame; order checks by cost; and
treat consent as a protocol property, not a UI nicety — because the first
eight times it broke, it broke in the plumbing, not the interface.

Every claim above links to the file or test that proves it. If one rots,
that's a bug in this document.
