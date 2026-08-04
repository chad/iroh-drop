# The daemon control API (U4)

Status: **implemented** (`iroh-drop-daemon`, with the macOS app and the CLI
as its first two clients). Nothing here changes bytes on the wire; this is
the UX layer's API, and it is buildable by a third party against the public
protocol API (see `docs/roadmap.md`'s boundary rule).

## What the daemon is for

Today the process *is* the peer: `share` runs in the foreground and the drop
dies with the terminal. That is honest for a protocol and fatal for an app —
people close laptop lids, and a peer that only exists while a terminal is open
can never be a replica for anyone.

The daemon owns three things a CLI invocation cannot:

1. **Lifetime** — one endpoint, one blob store, N sessions, surviving sleep,
   wake, and network changes.
2. **Continued seeding** — what you received stays served, which is the entire
   point of the replication design.
3. **Presence** — you can be *offered* something, which is what makes a
   receiver-side prompt (and therefore an AirDrop-shaped UX) possible at all.

Everything else — CLI, GUI, TUI, MCP server — becomes a thin client over one
API. If a feature can't be built over this socket, the API is wrong.

## Decentralization constraints

These are requirements, not preferences:

- **No account, no cloud, no registry, no telemetry.** Identity is a local
  keypair (`StackOptions::identity_path`), already supported.
- **The only server is on your own machine**, reachable only by you.
- **The daemon is optional.** Clients must work in *embedded mode* — spawn the
  stack in-process, exactly like today's CLI — so "no background service" stays
  a supported configuration. Same API, different transport (in-memory channel).
- **LAN-only must be a first-class posture**, not a debug flag:
  `offline: true, mdns: true` means no relay, no DNS, no pkarr, no third party.
- **No autostart without consent.** Installing a background service is a
  decision the user makes once, explicitly.

## Transport

**Unix domain socket** at `$XDG_RUNTIME_DIR/iroh-drop/control.sock` (mode
`0600`), **named pipe** `\\.\pipe\iroh-drop\control` on Windows with a DACL
limited to the current user.

Deliberately **not** a localhost TCP port. A TCP port is reachable by every
process on the machine and, worse, by any web page the user visits — a
drive-by request to `127.0.0.1:PORT` could publish your files. Filesystem
permissions on a UDS are the authorization model, so the API needs no tokens,
no CSRF defence, and no bearer secrets on disk. On Linux/macOS, verify peer
credentials (`SO_PEERCRED` / `LOCAL_PEERCRED`) and reject any uid but our own.

Framing: **newline-delimited JSON**, one object per line, UTF-8. Greppable,
`socat`-able, trivially bindable from any language. Max frame 1 MiB.

## Frame types

Five, distinguished by `t`. Requests flow both ways — the daemon needs to ask
the UI questions (see *Consent*), so this is not a client-only RPC.

```jsonc
{"t":"req","id":7,"m":"offer.fetch","p":{...}}   // client → daemon
{"t":"res","id":7,"p":{...}}                     // reply (either direction)
{"t":"err","id":7,"code":"not_found","msg":"…"}  // failure reply
{"t":"ask","id":91,"q":"offer.accept","p":{...}} // daemon → client, needs res
{"t":"ev","seq":4102,"e":"fetch.progress","p":{...}}  // daemon → all clients
```

`id` is client-scoped for `req` and daemon-scoped for `ask`; they live in
separate spaces so both sides can allocate freely. Unknown `m`/`q` values get
an `err` with `code:"unsupported"` — never a dropped connection. That mirrors
the wire protocol's op-tagged control channel, which answers unsupported ops
with `op = 65535` instead of hanging up.

## Handshake

```jsonc
→ {"t":"req","id":1,"m":"hello","p":{
     "client":"iroh-drop-gui/0.1","api":1,"roles":["ui"]}}
← {"t":"res","id":1,"p":{
     "api":1,"daemon":"0.5.0","wire":2,
     "endpoint_id":"a2299f…","methods":["drop.create","offer.publish",…],
     "events_from":4102}}
```

- `api` is the *control* API version, independent of `WIRE_VERSION`. Clients
  must tolerate unknown methods and unknown event names.
- `methods` is capability discovery, so a client can feature-detect rather than
  version-sniff. Same reasoning as `HelloV1::ops`.
- `roles`: `observer` (events only), `ui` (may be sent `ask`), `control`
  (may publish/fetch/leave). A menu-bar app is `["ui","control"]`; a status
  widget is `["observer"]`; the CLI's mutating commands are `["control"]`.
  Roles are **enforced centrally** in dispatch, not advisory: mutating
  methods (`drop.create`, `drop.join`, `drop.leave`, `offer.publish`,
  `offer.fetch`, `task.cancel`) answer `forbidden` without `control`, and
  `drop.ticket` — which reveals the bearer capability — requires `ui` or
  `control`. Read methods (`daemon.status`, lists, `events.replay`) are open
  to every role. The socket itself is user-private (0600 dir on Unix), so
  this is defense in depth and a contract third-party clients can rely on.

## Persistence: drops outlive the daemon

The blob store already survives restarts; the drop *memberships* now do too.
Beside the blob store (`<store>-daemon/`) the daemon keeps, per drop:

- `drops.json` — handle, display name, `mine` (whether we created it), and
  the full ticket (bootstrap addresses possibly stale; a join does not need
  any of them reachable);
- `frames-<topic>.bin` — the drop's retained, *signed* history.

Membership-level changes (`drop.create`, `drop.join`, `offer.publish`) are
persisted immediately — a crash a hundred milliseconds after joining must
not lose the membership. Chatty session events ride a 250 ms debounce, and
a final write happens at shutdown (SIGINT *and* SIGTERM); files are `0600`,
written temp-then-renamed. On
startup the daemon rejoins every drop in the table, replays the retained
frames through the same signature verification and state transitions as live
traffic (without re-running deciders — a restart must not re-ask consent for
last week's offers), and re-announces whatever the local store still holds
complete. Handles stay stable across restarts, so scripts and UIs never see
a drop rename itself.

`drop.leave` is the opposite of persistence: it announces withdrawal to the
group, then *deletes* the persisted state — a deliberate leave is forgotten,
a crash is not.

Membership is a set, not a list of joins: `drop.join` with a ticket whose
topic is already hosted returns the existing handle (`already: true`),
re-seeds discovery with the ticket's bootstrap addresses, pulls those peers
into the swarm, and adopts the ticket's display name if the first join left
none. A joined drop is named from its ticket, so groups show up as "Holiday
photos", not as anonymous memberships.

## Methods

Namespaced `noun.verb`. All parameters and results are JSON objects — never
bare values — so fields can be added without breaking clients.

### Sessions

| Method | Params | Result |
|---|---|---|
| `drop.create` | `{name?, policy?, lan?}` | `{drop, ticket, topic}` |
| `drop.join` | `{ticket \| room \| nearby, policy?}` | `{drop, topic, already?}` |
| `drop.list` | `{}` | `{drops:[{drop,name,mine,topic,peers,offers,lan}]}` |
| `drop.ticket` | `{drop, full?}` | `{ticket, url}` |
| `drop.leave` | `{drop, forget?}` | `{}` |

`drop` is a short daemon-local handle (`d3`), not a topic id — stable for the
session's lifetime, meaningless outside it, and short enough to type. `url` is
the fragment form (below).

### Content

| Method | Params | Result |
|---|---|---|
| `offer.list` | `{drop}` | `{items:[{n,name,hash,size,kind,status,from,members?}]}` |
| `offer.publish` | `{drop, path \| bytes_b64, name?, media_type?, metadata?}` | `{task}` |
| `offer.fetch` | `{drop, pick, out?}` | `{task}` |
| `task.cancel` | `{task}` | `{}` |
| `task.list` | `{}` | `{tasks:[{task,kind,drop,hash,done,total,state}]}` |

`offer.list` returns the SDK's numbered inventory verbatim — `n` is the same
number the CLI shows, so `pick` accepts `3`, `report.pdf`, or a hash prefix
without the client reimplementing resolution.

`status` is `missing` (never fetched), `fetching`, `failed` (a fetch was
tried and did not complete — retryable, so clients show it next to
`missing`, never as complete), or `available` (the bytes are local).

**Long operations return a `task` immediately and never block the connection.**
Progress arrives as events. This is the difference between a GUI that shows a
progress bar and a GUI that hangs.

### Peers, discovery, names

| Method | Params | Result |
|---|---|---|
| `peer.list` | `{drop?}` | `{peers:[{id,label?,known,last_seen}]}` |
| `peer.label` | `{id, label}` | `{}` |
| `nearby.list` | `{}` | `{drops:[{n,label,peer,ticket}]}` |
| `nearby.advertise` | `{drop, on}` | `{}` |
| `room.list` / `room.forget` | | |

`peer.label` is the local address book — the "faces and names" that make
AirDrop legible, kept entirely on your machine, never published.

### Consent semantics for agents (MCP)

Agent clients (`iroh-drop-mcp`, see `docs/mcp.md`) connect with the
**control** role — never `ui`. Asks route to ui-role clients only, so an
agent can never absorb a consent question, and an unsolicited offer is never
auto-accepted just because an agent is connected. The agent's explicit
`offer.fetch` call *is* the ask-and-answer for that one item. This is
enforced by `crates/iroh-drop-mcp/tests/agent_fetch.rs`. Labels are
local nicknames for stable `EndpointId`s (which `identity_path` already
guarantees across restarts). A peer's *self-asserted* name is untrusted display
metadata and must be rendered differently from a label you set yourself.

### Daemon

`daemon.status`, `daemon.config.get/set`, `daemon.shutdown`,
`events.replay {from}`.

### Web links (`--link-base`)

`iroh-dropd --link-base https://iroh-drop.boxd.sh` makes every share result
include a `web_link` field next to `link` and `ticket`: a URL of the form
`<base>#<ticket>` that opens the zero-install web client straight into the
receive flow. The ticket rides in the URL **fragment**, so the static web
host never sees it — the link is the same bearer capability as the ticket,
just browser-openable. Omit the flag and the field is absent. Point it at
your own deployment of `crates/iroh-drop-web` if you self-host; the public
instance's hosting story (currently a VM, intended to become static CI
deploys) changes nothing on the wire.

## Events

`seq` is a monotonic counter. Clients persist the last `seq` they saw and call
`events.replay {from}` after a reconnect; the daemon keeps a bounded ring
(say 4096) and reports `truncated:true` if the client fell too far behind. A UI
that crashes mid-transfer should reattach and still render the truth — the same
instinct as catch-up sync, applied to the local socket.

```
peer.joined  peer.left
offer.received  offer.rejected
fetch.started  fetch.progress  fetch.completed  fetch.failed
publish.completed
provider.available
drop.joined  drop.left
task.state
net.changed        // interface/relay/reachability transition
daemon.stopping
```

Mostly a 1:1 projection of `DropEvent`, plus daemon-only lifecycle events.

Two hard rules:

- **Coalesce `fetch.progress`** — at most ~10/s per task, and always emit a
  final one. A 40 000-chunk transfer must not produce 40 000 lines.
- **Events fan out to every attached client.** Two GUIs and a CLI see the same
  stream; none is privileged.

## Consent: the interesting part

`OfferDecider::decide` is **synchronous** and runs inside the gossip receive
loop (`session.rs:1389`). Blocking it on a human tapping "Accept" would stall
message processing for that session. So the accept prompt **must not** be a
decider.

The flow that works, with no protocol change:

```
1. offer arrives  → decider returns RecordOnly   (never auto-fetches)
2. daemon → ui    {"t":"ask","id":91,"q":"offer.accept",
                   "p":{"drop":"d3","from":"a2299f…","label":"Ada's laptop",
                        "name":"holiday.zip","size":41231234,"members":12,
                        "known":true}}
3. ui    → daemon {"t":"res","id":91,"p":{"accept":true,"out":"~/Downloads"}}
4. daemon issues an ordinary manual fetch and streams fetch.* events
```

The protocol keeps its invariant (announcing can never consume your disk),
the human decision lives in the app where it belongs, and the decider stays
the fast synchronous predicate it was designed to be.

Rules:

- `ask` goes to clients with role `ui`. First answer wins; later ones get
  `err code:"already_answered"`.
- **Timeout → deny.** No `ui` client attached → deny. Silence is never consent.
- The `ask` payload carries `known` (have you ever labelled this peer?) and
  `label`, so the UI can distinguish "Ada's laptop" from a stranger on
  conference wifi. Everything else in it is untrusted display metadata and must
  be rendered as such — filenames especially.
- Auto-accept, if offered at all, is scoped to a specific labelled peer and
  a size cap, and is stored in daemon config, not in the protocol's policy.

## Links without a server

`drop.ticket` returns both the bare ticket and:

```
https://<your-host>/#drop1agxpfees…
```

The ticket sits in the **fragment**, which browsers never send to the server.
So the page can be a static file on any host — a CDN object, a GitHub Page,
your own box — that offers "Open in app" via a registered `iroh-drop://`
scheme handler, with a download link as fallback. The host learns nothing: no
logs, no database, no ticket ever reaching it, nothing to subpoena or breach.

This is what makes Product B ("send someone a link") work with **zero
infrastructure**, and it's why word codes (`U12`, blocked on a rendezvous
server plus a PAKE) are not on the critical path. The page is optional
convenience; the bare ticket always works.

## Concurrency and failure

- One writer task per client connection; per-client bounded send queue. A slow
  client gets dropped frames and a `truncated` marker, never backpressure into
  the session loops.
- The daemon is authoritative. Clients hold no state they can't rebuild from
  `drop.list` + `offer.list` + `task.list`.
- Client disconnect cancels nothing. Transfers are the daemon's, not the UI's —
  closing the window must not abort a 10 GB receive.
- `daemon.shutdown` withdraws provider announcements politely; a crash does
  not, which is exactly the case `publisher_exit.rs` already covers.

## Testing

- The in-memory transport (embedded mode) makes the whole API testable with no
  socket and no filesystem.
- Golden JSONL transcripts per method — cheap to review, and they catch schema
  drift the way the roadmap wants conformance fixtures to catch wire drift.
- One end-to-end test over a real socket, since UDS permissions and peer-cred
  checks are exactly what unit tests miss.

## Open questions

1. **Where do labels live** — daemon config, or a new `sdk::contacts` module?
   Probably the SDK, so a GUI without the daemon still has an address book.
2. **Should the API be exposable over an iroh ALPN** for controlling your own
   daemon from your own phone? Attractive (self-hosted, no cloud) and a real
   footgun (the API has no auth because UDS provided it). If ever: separate
   ALPN, explicit allowlist of your own endpoint ids, off by default.
3. **Windows/macOS service integration** — launchd/Task Scheduler wrappers are
   packaging work the roadmap doesn't cost yet.
4. **Does `nearby.advertise` belong per-drop or per-transfer?** For an
   AirDrop-shaped UX it should be per-transfer and short-lived, which may mean
   an SDK change rather than reusing `--lan` as-is.
