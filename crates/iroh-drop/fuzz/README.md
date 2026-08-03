# Fuzz targets for iroh-drop

Three targets, one per hostile decode surface:

| Target | Input stands in for |
|---|---|
| `fuzz_message_decode` | gossip frames from any peer, relayed or direct |
| `fuzz_sync_envelopes` | control-channel envelopes from any joined peer |
| `fuzz_ticket` | pasted tickets: share links, chat text, shells |

## Running

Requires nightly and cargo-fuzz:

```sh
rustup toolchain install nightly
cargo install cargo-fuzz
```

Seed the corpora from the golden conformance fixtures (they are the best
starting point — valid frames one mutation away from interesting ones):

```sh
cd crates/iroh-drop/fuzz
mkdir -p corpus/fuzz_message_decode corpus/fuzz_sync_envelopes corpus/fuzz_ticket
cp ../tests/fixtures/*.bin corpus/fuzz_message_decode/
cp ../tests/fixtures/control_*.bin corpus/fuzz_sync_envelopes/
cp ../tests/fixtures/ticket_*.txt corpus/fuzz_ticket/
```

Then:

```sh
cargo +nightly fuzz run fuzz_message_decode    # Ctrl-C to stop
```

CI runs each target for 60 seconds per PR (`.github/workflows/ci.yml`,
`fuzz-smoke` job) and reuses the same seed step. A crash is a red build:
minimize it with `cargo fuzz tmin <target> <artifact>` and add the
minimized input as a regression fixture under `tests/fixtures/`.

## Rules

- Decoding must never panic. A panic here is a remote denial of service;
  treat every crash as a security bug.
- Do not "fix" a crash by narrowing the fuzzer. Fix the decoder.
- If a crash reveals a byte sequence that *should* decode differently, that
  is a wire-format question first — see `docs/roadmap.md` versioning rules.
