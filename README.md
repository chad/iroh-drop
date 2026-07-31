# iroh-drop

Hand files to people with one string. Everyone who receives them helps serve
them — so the files stay reachable even after the original sender closes their
laptop.

```sh
# you
iroh-drop share ./slides.pdf ./photos
#   → prints a ticket: drop1agxpfees...

# them
iroh-drop receive drop1agxpfees...
#   → Saved 14 file(s) to .
```

No accounts, no server, no hashes to copy. The ticket is the only string
anybody has to move, and files are picked by name or by number.

Underneath it is an **announced-blob meta-protocol** for
[Iroh](https://iroh.computer): subscribe to a drop, and when any member
publishes a blob, every member learns it exists and can retrieve it —
verified against its hash, from whoever still has it.

## How it fits together

`iroh-drop` does not reimplement gossip, byte transfer, resumable downloads,
content verification, NAT traversal, or endpoint identity. It composes two
existing protocols on one shared endpoint, and keeps the human-facing parts in
separate layers on top:

```text
   iroh-drop-gui          a window: drag files, hand over a link, accept/decline
   iroh-drop-cli          share / receive, numbered picks, event log
   iroh-drop-daemon       long-lived sessions + JSONL control API (one socket)
   iroh-drop-sdk          collections, config, inventory (no wire logic)
   ─────────────────────  public API boundary
   iroh-drop              sessions · signed messages · policy · providers
                          /                                          \
              iroh-gossip                                        iroh-blobs
        membership + offers                              storage + transfer
                          \                                          /
                                 shared Iroh endpoint
```

The boundary is a rule, not an accident: the protocol crate owns what bytes
mean and how they are verified; naming, directory trees, config, and display
belong above it. Anything the CLI does, a third-party app can do with the same
public API. See [`docs/roadmap.md`](docs/roadmap.md).

* **Announce** — publishing broadcasts a signed `Offer` (hash, name, size,
  media type).
* **Catch up** — a late joiner pulls the retained, still-signed offer log
  from any member over the control ALPN, so it sees the drop's contents *by
  name* even for offers made before it arrived.
* **Stay** — membership is the default and survives restarts: join once and
  you are in the group until you explicitly leave. Whenever a neighbor
  appears, both sides pull whatever history the other has that they missed
  (anti-entropy on neighbor-up), so two machines that are never online at
  the same time still converge.
* **Fetch** — manual by default, policy-gated, pulled from any peer that has
  the bytes and verified against the hash.
* **Replicate** — every successful recipient announces itself as a provider,
  so content survives its publisher.

## Install

```sh
cargo build --workspace   # target/debug/{iroh-drop,iroh-dropd,iroh-drop-app}
```

## The app

```sh
packaging/macos/build_native.sh   # -> dist/iroh-drop.app (+ a zip to send)
open dist/iroh-drop.app
```

A native macOS app: SwiftUI front end, the Rust helper inside the bundle. Drag
files in, get a **link**, hand it over. Clicking an `iroh-drop://` link opens the
app and pulls the files. Incoming files from someone you did not just ask show a
card with the name, size, sender and a countdown, and **nothing is written to
disk until you click Accept** — a card nobody answers is a refusal.

There is no FFI. The helper is a separate process speaking newline-delimited JSON
over a Unix socket, so Swift talks to it exactly as the CLI does: no generated
bindings, no xcframework, and a crash in the networking core cannot take the
window with it. The helper has to be a separate process anyway, because its job
is to outlive the window — that is what keeps shared files reachable.

The app never shows a hash, a peer id, or the word "ticket". The only string is a
link; the only decision is yes or no.

`crates/iroh-drop-gui` is the same idea in egui for Linux and Windows, where
there is no AppKit.

## Everyday use: run the daemon

Without a daemon the process *is* the peer, so closing the terminal withdraws
the files and you are never really a replica for anyone. Start it once:

```sh
iroh-dropd                 # relays when needed, mDNS on
iroh-dropd --lan-only      # no relay, no DNS, no pkarr: nothing leaves the network
```

Then the commands **return** instead of holding the drop open:

```sh
iroh-drop send ./scones.txt ./photos    # prints a ticket, exits; daemon keeps serving
iroh-drop get <ticket>                  # fetches, then keeps serving too
iroh-drop watch                         # incoming offers, with an accept prompt
iroh-drop drops                         # what the daemon is hosting
iroh-drop drops --ticket d1             # a fresh ticket listing you first
iroh-drop leave d1                      # leave a group (or stop hosting yours)
iroh-drop leave all                     # leave everything
```

`watch` is the receiving end: nothing is written to disk until you say yes, and
a prompt nobody answers is a refusal. `--yes` skips the prompt and is a bad
idea anywhere but a test.

The daemon speaks newline-delimited JSON over a Unix socket in a `0700`
directory — not a localhost TCP port, which any web page you visit could reach.
Every client (CLI, GUI, TUI, agent) uses the same API; see
[`docs/daemon-api.md`](docs/daemon-api.md). The foreground `share`/`receive`
commands still work with no daemon at all.

## Using it

### Send

```sh
iroh-drop share ./report.pdf ./project-folder
```

Prints a ticket, then keeps serving until Ctrl-C. Directories become a single
offer (one manifest, not one offer per file) and arrive as directories.
Publishing before anyone joins is fine — late joiners catch up.

Handy flags: `--ticket-file ./ticket` (write the ticket out), `--offline`
(LAN only, no relays), `--ephemeral` (forget identity and blobs on exit).

Handy flags: `--qr` (show a scannable code), `--lan` (advertise on the local
network — see below), `--room <name>` (reuse a saved drop), `--ticket-file`,
`--offline`, `--ephemeral`.

### Receive

```sh
iroh-drop receive <ticket>                 # everything, into the current dir
iroh-drop receive <ticket> 2 --out ./dl    # just item 2
iroh-drop receive <ticket> report.pdf      # by name
iroh-drop receive @./ticket --keep-serving # read ticket from a file, then help serve
iroh-drop receive --room laptop            # a drop you saved earlier: no ticket
iroh-drop receive --nearby 1               # something shared on this network
```

### Not typing tickets at all

Three ways to skip the string, in increasing order of laziness:

```sh
iroh-drop share ./report.pdf --qr          # they scan it with a phone
iroh-drop share ./report.pdf --lan         # they run: iroh-drop nearby
iroh-drop share ./report.pdf --room team   # you both use --room team forever
```

**QR** is just the ticket, rendered. **Rooms** are saved tickets under a name:
`share --room team` remembers the drop, `receive --room team` rejoins it, and
`iroh-drop rooms` lists what you have. Each join refreshes the room with a
ticket that includes *you*, so a room keeps working after the original sharer
goes away.

**Nearby** advertises the drop over mDNS so anyone on the network can list it:

```sh
iroh-drop nearby                # → 1  chad's laptop   be86e02084
iroh-drop receive --nearby 1
```

⚠️ `--lan` is a broadcast: everyone who can see mDNS on that network can join
the drop, read every offer, and publish into it. It is for networks where you
would happily read a filename out loud. It is opt-in per share and stops when
the process exits.

Tickets are short by default — they name peers by id and let discovery find
the addresses (pkarr/DNS online, mDNS on a LAN), which is both smaller and
immune to changing IPs. Pass `--full-ticket` to embed addresses instead, and
`--mdns` to turn on local discovery when you are not using `--offline`
(which enables it automatically).

### Stay and look around

```sh
iroh-drop open <ticket>     # join and get an interactive prompt
iroh-drop new               # start an empty drop and get a prompt
```

At the prompt (contents are listed automatically on join):

```text
  #  name                                   size  status
  1  note.txt                               12 B  file, available
  2  project                           293.0 KiB  folder, 3 files, available

> get 2 to ./downloads
> add ./another-file.txt
> who
> ticket
> quit
```

Commands: `ls`, `get <#|name|all> [to <path>]`, `add <path> [as <name>]`,
`who`, `ticket`, `help`, `quit`.

### Where things live

```sh
iroh-drop config          # show paths and limits
iroh-drop config --init   # write a config file you can edit
```

Defaults follow XDG: identity, blob store and downloads under
`$XDG_DATA_HOME/iroh-drop/`, config at `$XDG_CONFIG_HOME/iroh-drop/config.toml`.
The identity file gives you a stable peer id across restarts; the blob store
lets you keep serving what you received. `--ephemeral` opts out of both.

Other flags: `--auto` (fetch every offer automatically, within policy limits),
`--dir`, `--store`, `--identity`, `-v` (hashes and protocol chatter).

## Using it as a library

Protocol only — hashes in, hashes out:

```rust
use iroh_drop::{DropBuilder, FetchOutput, StackOptions};

let protocol = DropBuilder::from_options(StackOptions::default())
    .await?
    .build()
    .await?;

let session = protocol.create(Default::default()).await?;
println!("ticket: {}", session.ticket());

let published = session.publish_path("./slides.pdf").await?;

// Elsewhere:
//   let session = protocol.join(ticket).await?;   // catches up automatically
//   session.fetch(published.hash, FetchOutput::Directory("./dl".into())).await?;
```

Sessions emit events (`session.subscribe()`): offers, fetch progress, provider
availability, peer join/leave. `DropPolicy` gates everything automatic (size
caps, concurrency, total budget).

With the SDK layer — folders, names, numbers:

```rust
use iroh_drop_sdk::{collections::{publish_path, fetch_any}, inventory, resolve_pick};

let published = publish_path(&session, "./project", None).await?; // one offer
for item in inventory(&session) {
    println!("{:>2}  {:<24} {}", item.index, item.name, item.kind());
}
let hash = resolve_pick(&session, "2")?;             // number, name, or prefix
let files = fetch_any(&session, hash, "./downloads").await?; // tree materialized
```

## Passing it on (the point of all this)

```sh
# 1. you share, and hand out the ticket
iroh-drop share ./dataset.bin

# 2. a friend receives it and stays as a source
iroh-drop receive <ticket> --keep-serving
#    → prints a ticket of their own

# 3. you close your laptop. Someone else uses the friend's ticket:
iroh-drop receive <friend-ticket>
#    → ✔ got "dataset.bin"  (served by the friend, verified by hash)
```

The third peer learns the file *by name* even though it joined after the
original offer and the publisher is gone: it catches up from whoever is still
there, tries the publisher first, and falls through to a replica.

## The demo

The scenario that motivates the whole design — **the drop outlives its
publisher** — runs in one process, fully offline:

```sh
cargo run -p iroh-drop --example four_peer_drop
```

A creates a drop and publishes a file; B and C auto-fetch it; A exits
abruptly; D joins later and still gets the file, served by a replica.

## Tickets

A ticket (`drop1...`, RFC 4648 base32, lowercase, no padding) is a bearer
capability carrying the topic id, bootstrap addresses, and optional untrusted
metadata. Anyone holding it can join, read, and publish. Inspect one with
`iroh-drop inspect <ticket>`.

Practical notes:

- A ticket from a peer that is still running is the most reliable way to bring
  in latecomers; `ticket` at the prompt prints a fresh one, and it lists that
  peer first.
- Short tickets (the default) need the joiner to be able to *resolve* an id:
  online that is pkarr/DNS, on a LAN it is mDNS (`--mdns`, implied by
  `--offline`). With neither, use `--full-ticket`.
- Blobs fetched by hash with nobody advertising a name are stored under their
  hash — that is the one place a hash can surface.

## Security model

Messages are signed by their author's endpoint key and verified on receipt —
gossip's delivering neighbor is not the author, and catch-up sync deliberately
relays other peers' frames verbatim, so signatures are what make authorship
meaningful. The blob hash is the content identity and every transferred byte is
verified against it. Names, sizes, media types and metadata are untrusted
display hints: exports get collision-safe names, manifest paths are rejected if
they try to escape the destination, and metadata is parsed defensively. Manual
fetching is the default so that announcing an offer cannot fill someone's disk.

Tickets are capabilities: anyone with one is a full member. There is no member
removal and no private drops yet — see the roadmap.

## Development

```sh
cargo test --workspace     # 103 tests, 3 #[ignore]d (live mDNS + internet)
cargo test --workspace -- --ignored   # + live mDNS and internet tests
cargo clippy --workspace --all-targets
cargo run -p iroh-drop --example four_peer_drop
RUST_LOG=iroh_drop=debug ...
```

| Path | What |
|---|---|
| `crates/iroh-drop` | protocol: sessions, messages, sync, policy, providers, tickets |
| `crates/iroh-drop-sdk` | conventions: collections, config, inventory, rooms, nearby |
| `crates/iroh-drop-daemon` | long-lived session host + the JSONL control API |
| `crates/iroh-drop-cli` | the `iroh-drop` binary |
| `crates/iroh-drop-gui` | portable window in egui (Linux/Windows); logic in `bridge.rs` is headless-testable |
| `macos/Sources` | the native macOS app (SwiftUI over the control socket, no FFI) |
| `packaging/macos` | `build_native.sh`, the icon renderer, the bundle |
| `scripts/friend.sh` | pretend to be a second person, to test on one machine |
| `examples/four_peer_drop.rs` | publisher-disappearance demo |
| `docs/protocol.md` | wire reference (messages, sync, tickets, ALPNs) |
| `docs/daemon-api.md` | the control API every client speaks, and why |
| `docs/extending.md` | the four extension points, and which to reach for |
| `docs/roadmap.md` | what is next, and the layering rule |

Integration tests live in `crates/iroh-drop/tests/` under spec-named files
(`fetch_flow.rs`, `catch_up_sync.rs`, `publisher_exit.rs`,
`malformed_messages.rs`, `policy_gates.rs`, `provider_fallback.rs`,
`restart_persistence.rs`, `short_tickets.rs`) plus the SDK's
`collections.rs` and `nearby.rs`. Two tests need the outside world and are
`#[ignore]`d: `online_short_ticket.rs` (internet) and `nearby.rs`'s discovery
test (mDNS multicast).
They run real endpoints, gossip, and blob transfers on localhost in offline
mode — the same session logic the CLI uses online.

## Status

v0.4. `WIRE_VERSION = 2`: message bodies are kind-tagged, so unknown kinds are
ignored *and relayed* rather than breaking a frame, and the control channel is
op-tagged with a `Hello` capability exchange. State is bounded everywhere
(offers, providers, peers, aliases, history) with per-author quotas and
per-peer rate limits; see `docs/protocol.md` for the table and
`docs/extending.md` to build on it.

Known limitations: no private drops or member removal (a ticket is a bearer
capability, and `--lan` shares it with the network), no swarm (multi-provider
parallel) downloads, no long-running daemon, sessions are welded to
iroh-gossip (no pluggable transport yet), and there are no checked-in
conformance vectors — all tracked in `docs/roadmap.md`.
Next steps in [`docs/roadmap.md`](docs/roadmap.md).
