#!/usr/bin/env bash
# Cross-build the Windows client from macOS and assemble dist/windows.
#
# Requires:
#   rustup target add x86_64-pc-windows-msvc
#   cargo install cargo-xwin
#   brew install llvm        # ring's build needs llvm-lib on PATH
#
# Produces dist/windows/iroh-drop/{Drop.exe, iroh-dropd.exe, iroh-drop.exe}
# and dist/iroh-drop-<version>-windows-x64.zip. The GUI looks for its helper
# by name next to itself, so the three binaries must ship side by side.

set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"

TARGET=x86_64-pc-windows-msvc
VERSION=$(grep -m1 '^version' crates/iroh-drop-gui/Cargo.toml | cut -d'"' -f2)

export PATH="/opt/homebrew/opt/llvm/bin:$PATH"
if ! command -v cargo-xwin >/dev/null && ! cargo xwin --version >/dev/null 2>&1; then
  echo "cargo-xwin not found: cargo install cargo-xwin" >&2
  exit 1
fi

cargo xwin build --release --target "$TARGET" \
  -p iroh-drop-daemon -p iroh-drop-cli -p iroh-drop-gui

OUT=dist/windows/iroh-drop
rm -rf dist/windows
mkdir -p "$OUT"
cp "target/$TARGET/release/iroh-drop-app.exe" "$OUT/Drop.exe"
cp "target/$TARGET/release/iroh-dropd.exe" "$OUT/iroh-dropd.exe"
cp "target/$TARGET/release/iroh-drop.exe" "$OUT/iroh-drop.exe"
cp packaging/windows/README.txt "$OUT/README.txt"
cp packaging/windows/install-url-scheme.ps1 "$OUT/install-url-scheme.ps1"

(cd dist/windows && zip -qr "../iroh-drop-$VERSION-windows-x64.zip" iroh-drop)
echo "Zipped $ROOT/dist/iroh-drop-$VERSION-windows-x64.zip"
