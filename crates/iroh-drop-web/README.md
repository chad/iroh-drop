# iroh-drop-web

`iroh-drop` in the browser: wasm-bindgen bindings plus the static site in
`www/` — both the usable web client and the project's landing page (how it
works, protocol composition, download links).

The browser node is **relay-only** (no QUIC, no mDNS) and keeps blobs in an
**in-memory store**. It is a leaf, not a swarm member: it can share and
receive, but availability still comes from desktop members. Receiving is a
fetch into memory followed by a browser download; sending is a `File` read
into memory. Very large files want the desktop app.

## Build

Requirements: `wasm32-unknown-unknown` rust target, `wasm-pack`
(`cargo binstall wasm-pack`). On macOS, `ring`'s C build needs a clang with
the wasm backend (Apple clang lacks it):

```sh
brew install llvm
export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
```

(Linux distro clang includes the wasm backend; skip the exports there.)

Then, **from this directory** (so `.cargo/config.toml` applies — it selects
getrandom's browser backend):

```sh
wasm-pack build --target web                        # dev build -> ./pkg
python3 -m http.server                              # serve this directory statically
# open http://localhost:8000/www/
```

For deployment, use the size-tuned profile (workspace-root
`[profile.wasm-release]`: `opt-level = "z"`, fat LTO, one codegen unit):

```sh
wasm-pack build --target web --profile wasm-release
```

The optimized wasm is **~3.6 MB** (a plain dev build is roughly 2.5× that).
Deploying to the live site is one script — it builds with this profile,
stages `www/` + `pkg/`, ships them to the VM, and verifies the result:

```sh
scripts/deploy-web.sh     # from the repo root; needs the `boxd` CLI
```

Open the page once for a share link; open that link anywhere (another tab, a
phone, a friend's machine) to receive. The ticket lives in the URL fragment
(`#drop2…`), so the static host never sees it. Identity persists in
`localStorage`, so a returning browser keeps its endpoint id.

## JS API

```js
import init, { WebDrop } from './pkg/iroh_drop_web.js';
await init();

const drop = await WebDrop.start(identityOrNull, relayOrNull);
// 32-byte key | null; relay URL string | null (null = n0 public relays)
drop.identity();                                   // persist this
const session = await drop.join(ticket);           // or drop.create(name)

session.on_event((ev) => { /* {kind: "offerReceived"|"fetchProgress"|…} */ });
await session.offers();                            // [{hash, name, size, …}]
const hash = await session.publish(name, u8, mime);
const u8 = await session.fetch(hash);              // verified bytes
```

## Relay configuration

`WebDrop.start(identity, relayUrl)` — pass a relay URL (e.g.
`https://relay.example.com`) to use a self-hosted relay instead of n0's
public ones. The static site's choice lives in one constant at the top of
`www/app.js` (`RELAY_URL`). **Public n0 relays rate-limit**; big transfers
may need a self-hosted relay.

## Known limits

- **Fetched files are RAM-bound.** Blobs live in an in-memory store, so
  receiving a file needs roughly its size in free tab RAM (hard cap 2 GiB
  per blob). An OPFS-backed store is tracked in
  [issue #1](https://github.com/chad/iroh-drop/issues/1); very large files
  want the desktop app.
- **Short tickets**: desktop "nearby" tickets (no bootstrap addresses)
  rely on LAN discovery a browser can't do — web joins need full tickets.
  The desktop emits full tickets in share links by default.

## Interop matrix (browser ↔ desktop)

|                            | browser joins a desktop drop | desktop joins a browser drop |
| -------------------------- | ---------------------------- | ---------------------------- |
| **full ticket** (drop2…)   | ⏳ compiles, not yet live-tested | ⏳ compiles, not yet live-tested |
| **short ticket** (nearby)  | ✖ known gap — no LAN discovery from wasm | n/a |

No cell is claimed "tested" until someone runs it in a real browser
(see below). Page-load cost: ~3.7 MB total transfer; ~3.4 s of fetching
on a fast connection, plus wasm instantiation and the relay handshake.

## First live browser test

`cargo check` proves compilation, not runtime. tokio `time`/`rt` on wasm
and iroh's relay-over-WebSocket path are exercised for real only in a
browser: share from the desktop CLI (`iroh-drop share ./file`), open the
link, accept, verify the bytes — then flip the matrix cells above to ✅.
