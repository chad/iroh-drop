#!/usr/bin/env bash
# Build the native macOS app: SwiftUI front end, Rust daemon inside the bundle.
#
# No FFI. The daemon already speaks newline-delimited JSON over a Unix socket, so
# Swift talks to it exactly as the Rust CLI does. That means no generated
# bindings, no xcframework, and a crash in the networking core cannot take the
# window with it — the helper is a separate process, which it has to be anyway so
# that shared files outlive the window.
#
# Usage: packaging/macos/build_native.sh [--debug]

set -euo pipefail

cd "$(dirname "$0")/../.."
ROOT="$PWD"

PROFILE="release"
CARGO_FLAGS="--release"
SWIFT_FLAGS=(-O)
if [[ "${1:-}" == "--debug" ]]; then
  PROFILE="debug"
  CARGO_FLAGS=""
  SWIFT_FLAGS=(-Onone -g)
fi

APP_NAME="iroh-drop"
# The bundle's main executable must NOT be called `iroh-drop`: the CLI of that
# name also lives in Contents/MacOS, and `install` would silently overwrite the
# app with it. Double-clicking then runs the CLI, which prints help and exits.
EXECUTABLE="iroh-drop-app"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' crates/iroh-drop-gui/Cargo.toml | head -1)"
DIST="$ROOT/dist"
APP="$DIST/$APP_NAME.app"
CONTENTS="$APP/Contents"
DEPLOY_TARGET="13.0"   # MenuBarExtra and .dropDestination need Ventura.

echo "==> Building the helper and CLI ($PROFILE)"
# shellcheck disable=SC2086
cargo build $CARGO_FLAGS -p iroh-drop-daemon -p iroh-drop-cli
BIN_DIR="$ROOT/target/$PROFILE"

echo "==> Assembling $APP"
rm -rf "$APP"
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"

echo "==> Compiling Swift"
ARCH="$(uname -m)"
# The Command Line Tools SDK cannot see ServiceManagement's Swift interface
# (launch-at-login); the full Xcode SDK can. Prefer it, fall back to xcrun.
XCODE_SDK="/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"
if [ -d "$XCODE_SDK" ]; then SDK_PATH="$XCODE_SDK"; else SDK_PATH="$(xcrun --show-sdk-path)"; fi

swiftc "${SWIFT_FLAGS[@]}" \
  -target "${ARCH}-apple-macosx${DEPLOY_TARGET}" \
  -sdk "${SDK_PATH}" \
  -framework AppKit -framework SwiftUI -framework UserNotifications \
  -framework CoreImage -framework UniformTypeIdentifiers \
  -framework ServiceManagement \
  -o "$CONTENTS/MacOS/$EXECUTABLE" \
  macos/Sources/*.swift

install -m 0755 "$BIN_DIR/iroh-dropd" "$CONTENTS/MacOS/iroh-dropd"
install -m 0755 "$BIN_DIR/iroh-drop"  "$CONTENTS/MacOS/iroh-drop"

# Guard the collision above from coming back.
[[ "$EXECUTABLE" != "iroh-drop" && "$EXECUTABLE" != "iroh-dropd" ]] \
  || { echo "the app executable name collides with a bundled tool" >&2; exit 1; }

if [[ "$PROFILE" == "release" ]]; then
  echo "==> Stripping"
  strip -x "$CONTENTS/MacOS/iroh-dropd" "$CONTENTS/MacOS/iroh-drop" 2>/dev/null || true
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
  <key>CFBundleExecutable</key>        <string>$EXECUTABLE</string>
  <key>CFBundleIconFile</key>          <string>AppIcon</string>
  <key>CFBundlePackageType</key>       <string>APPL</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleVersion</key>           <string>$VERSION</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>LSMinimumSystemVersion</key>    <string>$DEPLOY_TARGET</string>
  <key>LSApplicationCategoryType</key> <string>public.app-category.utilities</string>
  <key>NSHighResolutionCapable</key>   <true/>
  <key>NSSupportsAutomaticGraphicsSwitching</key><true/>
  <key>NSHumanReadableCopyright</key>  <string>MIT OR Apache-2.0</string>

  <!-- What makes a link a link: clicking iroh-drop://receive/... opens the app,
       which then joins the drop. Without this, the only thing to hand someone is
       a base32 blob. -->
  <key>CFBundleURLTypes</key>
  <array>
    <dict>
      <key>CFBundleURLName</key>       <string>iroh-drop link</string>
      <key>CFBundleTypeRole</key>      <string>Viewer</string>
      <key>CFBundleURLSchemes</key>
      <array><string>iroh-drop</string></array>
    </dict>
  </array>

  <!-- Without these two, macOS blocks Bonjour and LAN discovery fails silently:
       peers on the same wifi simply never appear. The service type must match
       iroh-mdns-address-lookup's, which is _irohv1._udp. -->
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

echo "==> Registering the URL scheme with Launch Services"
# Without this the scheme is only picked up once the app has been moved or
# relaunched from Finder; nudging lsregister makes links work immediately.
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister \
  -f "$APP" 2>/dev/null || true

echo "==> Packaging for sending"
ZIP="$DIST/$APP_NAME-$VERSION-macos-$ARCH.zip"
rm -f "$ZIP"
ditto -c -k --sequesterRsrc --keepParent "$APP" "$ZIP"

echo
echo "Built $APP"
du -sh "$APP" | sed 's/^/  /'
echo "Zipped $ZIP"
du -sh "$ZIP" | sed 's/^/  /'
