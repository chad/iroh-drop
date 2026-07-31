#!/usr/bin/env bash
# Build iroh-drop.app.
#
# Unsigned by Apple's reckoning, but ad-hoc signed (`codesign -s -`), which costs
# nothing and matters more than it sounds: on Apple Silicon a bundle with a
# broken or absent signature can be refused outright, and macOS ties granted
# permissions (local network, in our case) to a code identity. Without one, the
# permission can be re-prompted or silently dropped on every launch.
#
# Usage: packaging/macos/bundle.sh [--debug]

set -euo pipefail

PROFILE="release"
CARGO_FLAGS="--release"
if [[ "${1:-}" == "--debug" ]]; then
  PROFILE="debug"
  CARGO_FLAGS=""
fi

cd "$(dirname "$0")/../.."
ROOT="$PWD"
APP_NAME="iroh-drop"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' crates/iroh-drop-gui/Cargo.toml | head -1)"
DIST="$ROOT/dist"
APP="$DIST/$APP_NAME.app"
CONTENTS="$APP/Contents"

echo "==> Building ($PROFILE)"
# shellcheck disable=SC2086
cargo build $CARGO_FLAGS -p iroh-drop-gui -p iroh-drop-daemon -p iroh-drop-cli

BIN_DIR="$ROOT/target/$PROFILE"
for binary in iroh-drop-app iroh-dropd iroh-drop; do
  [[ -x "$BIN_DIR/$binary" ]] || { echo "missing $BIN_DIR/$binary" >&2; exit 1; }
done

echo "==> Assembling $APP"
rm -rf "$APP"
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"

# The window, the background helper, and the CLI all travel together: the app
# looks for the helper beside its own executable and nowhere else.
install -m 0755 "$BIN_DIR/iroh-drop-app" "$CONTENTS/MacOS/iroh-drop-app"
install -m 0755 "$BIN_DIR/iroh-dropd"    "$CONTENTS/MacOS/iroh-dropd"
install -m 0755 "$BIN_DIR/iroh-drop"     "$CONTENTS/MacOS/iroh-drop"

if [[ "$PROFILE" == "release" ]]; then
  echo "==> Stripping"
  strip -x "$CONTENTS/MacOS/"* 2>/dev/null || true
fi

echo "==> Rendering the icon"
ICONSET="$(mktemp -d)/AppIcon.iconset"
python3 packaging/macos/make_icon.py "$ICONSET"
iconutil -c icns "$ICONSET" -o "$CONTENTS/Resources/AppIcon.icns"

echo "==> Writing Info.plist"
cat > "$CONTENTS/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>              <string>Drop</string>
  <key>CFBundleDisplayName</key>       <string>Drop</string>
  <key>CFBundleIdentifier</key>        <string>computer.iroh.drop</string>
  <key>CFBundleExecutable</key>        <string>iroh-drop-app</string>
  <key>CFBundleIconFile</key>          <string>AppIcon</string>
  <key>CFBundlePackageType</key>       <string>APPL</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleVersion</key>           <string>$VERSION</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>LSMinimumSystemVersion</key>    <string>11.0</string>
  <key>LSApplicationCategoryType</key> <string>public.app-category.utilities</string>
  <key>NSHighResolutionCapable</key>   <true/>
  <key>NSSupportsAutomaticGraphicsSwitching</key><true/>
  <key>NSHumanReadableCopyright</key>  <string>MIT OR Apache-2.0</string>

  <!-- Without these two, macOS blocks Bonjour and LAN discovery fails with no
       error anywhere: peers on the same wifi simply never appear. The service
       type must match iroh-mdns-address-lookup's, which is _irohv1._udp. -->
  <key>NSLocalNetworkUsageDescription</key>
  <string>iroh-drop finds people on your network so files can go straight to them instead of across the internet.</string>
  <key>NSBonjourServices</key>
  <array>
    <string>_irohv1._udp</string>
  </array>
</dict>
</plist>
PLIST

echo "==> Ad-hoc signing"
codesign --force --deep --sign - --timestamp=none "$APP"
codesign --verify --deep --strict "$APP" && echo "    signature ok"

echo "==> Packaging for sending"
ZIP="$DIST/$APP_NAME-$VERSION-macos-$(uname -m).zip"
rm -f "$ZIP"
# ditto, not zip: it preserves the bundle's symlinks and extended attributes.
ditto -c -k --sequesterRsrc --keepParent "$APP" "$ZIP"

echo
echo "Built $APP"
du -sh "$APP" | sed 's/^/  /'
echo "Zipped $ZIP"
du -sh "$ZIP" | sed 's/^/  /'
cat <<'NOTE'

Sending it to someone: it is ad-hoc signed but not notarized, so the first
launch needs a right-click -> Open (once), or:

    xattr -dr com.apple.quarantine /Applications/iroh-drop.app

That is Gatekeeper doing its job, not a bug. Notarizing needs a paid Apple
Developer account; nothing else about the bundle would change.
NOTE
