#!/usr/bin/env bash
# Build and deploy the web client to the boxd VM serving iroh-drop.boxd.sh.
#
# Reproducible deploy: wasm-pack (size-tuned `wasm-release` profile), stage
# www/ + pkg/, ship a tarball, extract over /home/boxd/site, verify live.
#
# Requirements: wasm-pack, llvm clang (macOS: brew install llvm), the `boxd`
# CLI signed in, and a running VM named $BOXD_VM serving /home/boxd/site.
#
# Usage: scripts/deploy-web.sh

set -euo pipefail
cd "$(dirname "$0")/.."

BOXD_VM="${BOXD_VM:-iroh-drop}"
SITE_DIR="${SITE_DIR:-/home/boxd/site}"
SITE_URL="${SITE_URL:-https://iroh-drop.boxd.sh}"

export CC_wasm32_unknown_unknown="${CC_wasm32_unknown_unknown:-/opt/homebrew/opt/llvm/bin/clang}"
export AR_wasm32_unknown_unknown="${AR_wasm32_unknown_unknown:-/opt/homebrew/opt/llvm/bin/llvm-ar}"

echo "==> wasm-pack build (wasm-release profile)"
(cd crates/iroh-drop-web && wasm-pack build --target web --profile wasm-release)
ls -la crates/iroh-drop-web/pkg/iroh_drop_web_bg.wasm | awk '{print "    wasm: " $5 " bytes"}'

echo "==> staging www/ + pkg/"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp crates/iroh-drop-web/www/{index.html,app.js,style.css,favicon.svg} "$STAGE/"
cp -r crates/iroh-drop-web/pkg "$STAGE/pkg"
# No AppleDouble files in the tarball (they confuse GNU tar on the VM).
COPYFILE_DISABLE=1 tar czf "$STAGE/site.tgz" -C "$STAGE" index.html app.js style.css favicon.svg pkg

echo "==> uploading to $BOXD_VM"
boxd machine cp "$STAGE/site.tgz" "$BOXD_VM:/tmp/site.tgz"

echo "==> extracting over $SITE_DIR"
boxd machine exec "$BOXD_VM" -- bash -c \
  "cd '$SITE_DIR' && tar xzf /tmp/site.tgz && rm /tmp/site.tgz && find . -name '._*' -delete"

echo "==> verifying $SITE_URL"
curl -sf "$SITE_URL/app.js" | grep -q drop2 && echo "    app.js: drop2 ok"
curl -sfI "$SITE_URL/pkg/iroh_drop_web_bg.wasm" | head -1
echo "==> deployed"
