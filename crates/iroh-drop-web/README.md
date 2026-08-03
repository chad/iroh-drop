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
wasm-pack build --target web        # -> ./pkg
python3 -m http.server              # serve this directory statically
# open http://localhost:8000/www/
```

Open the page once for a share link; open that link anywhere (another tab, a
phone, a friend's machine) to receive. The ticket lives in the URL fragment
(`#drop2…`), so the static host never sees it. Identity persists in
`localStorage`, so a returning browser keeps its endpoint id.

## JS API

```js
import init, { WebDrop } from './pkg/iroh_drop_web.js';
await init();

const drop = await WebDrop.start(identityOrNull);  // 32-byte key | null
drop.identity();                                   // persist this
const session = await drop.join(ticket);           // or drop.create(name)

session.on_event((ev) => { /* {kind: "offerReceived"|"fetchProgress"|…} */ });
await session.offers();                            // [{hash, name, size, …}]
const hash = await session.publish(name, u8, mime);
const u8 = await session.fetch(hash);              // verified bytes
```

## Known unverifiables (until the first live browser test)

- `cargo check` proves compilation, not runtime. The first real test is:
  share from the desktop CLI (`iroh-drop share ./file`), open the link in a
  browser, accept, verify the bytes. tokio `time`/`rt` on wasm and iroh's
  relay-over-WebSocket path are exercised for real only there.
- Public n0 relays rate-limit; big transfers may need a self-hosted relay.
