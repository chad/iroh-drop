#!/usr/bin/env bash
# Pretend to be somebody else on this machine, so you can test the app alone.
#
# Two peers on one Mac only need three things kept apart: the control socket, the
# identity + blob store, and the download directory. Everything else — gossip,
# discovery, transfer — behaves exactly as it does between two laptops, because
# it is the same code talking over the same loopback-and-LAN paths.
#
#   scripts/friend.sh up                  start the friend's helper
#   scripts/friend.sh offer <file>        friend pushes a file at you  -> a card appears in the app
#   scripts/friend.sh link <file>         friend shares a file, prints a link -> paste it into the app
#   scripts/friend.sh get <link>          friend receives what you shared from the app
#   scripts/friend.sh files               what the friend has received
#   scripts/friend.sh down                stop the friend
#
# The app itself is left completely alone: it keeps using the default socket.

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"

FRIEND_DIR="${IROH_FRIEND_DIR:-/tmp/iroh-drop-friend}"
FRIEND_SOCK="$FRIEND_DIR/control.sock"
FRIEND_DL="$FRIEND_DIR/downloads"

# Prefer the bundled binaries so you are testing what you would actually send.
if [[ -x "$ROOT/dist/iroh-drop.app/Contents/MacOS/iroh-dropd" ]]; then
  BIN="$ROOT/dist/iroh-drop.app/Contents/MacOS"
elif [[ -x "$ROOT/target/release/iroh-dropd" ]]; then
  BIN="$ROOT/target/release"
else
  BIN="$ROOT/target/debug"
fi
DROPD="$BIN/iroh-dropd"
DROP="$BIN/iroh-drop"

# Your app's daemon, on the default path.
YOUR_SOCK="${XDG_RUNTIME_DIR:-$HOME/.local/share}/iroh-drop/control.sock"
[[ -S "$YOUR_SOCK" ]] || YOUR_SOCK="$HOME/.local/share/iroh-drop/control.sock"

die() { echo "error: $*" >&2; exit 1; }

# A socket file proves nothing: it outlives the process that made it. Ask.
alive() {
  [[ -S "$1" ]] && "$DROP" drops --socket "$1" >/dev/null 2>&1
}

require_friend() {
  alive "$FRIEND_SOCK" || die "the friend is not running. Try: scripts/friend.sh up"
}

require_you() {
  alive "$YOUR_SOCK" || die "your app does not seem to be running (nothing answering at $YOUR_SOCK)"
}

case "${1:-}" in
  up)
    if alive "$FRIEND_SOCK"; then
      echo "The friend is already running."
      exit 0
    fi
    # Clear a socket left behind by a crash or a kill.
    rm -f "$FRIEND_SOCK"
    mkdir -p "$FRIEND_DIR" "$FRIEND_DL"
    "$DROPD" --data-dir "$FRIEND_DIR/data" \
             --downloads "$FRIEND_DL" \
             --socket "$FRIEND_SOCK" \
             > "$FRIEND_DIR/daemon.log" 2>&1 &
    for _ in $(seq 40); do
      alive "$FRIEND_SOCK" && break
      sleep 0.25
    done
    require_friend
    echo "Friend is up."
    "$DROP" drops --socket "$FRIEND_SOCK" | head -1 | sed 's/^/  /'
    echo "  downloads: $FRIEND_DL"
    ;;

  offer)
    # The unsolicited path: the friend shares, then *your* daemon joins without
    # asking for anything, so the app has to ask you.
    [[ -n "${2:-}" ]] || die "usage: friend.sh offer <file-or-folder>"
    [[ -e "$2" ]] || die "no such file: $2"
    require_friend
    require_you
    link="$("$DROP" send "$2" --name "from your friend" --socket "$FRIEND_SOCK" \
              | grep -o 'iroh-drop://receive/[a-z0-9]*')"
    [[ -n "$link" ]] || die "the friend could not share that"
    "$DROP" join "$link" --socket "$YOUR_SOCK" > /dev/null
    echo "Sent. Look at the app: there should be a card asking about"
    echo "  $(basename "$2")"
    ;;

  link)
    # The link path: you paste it into the app's Receive box. No card, because
    # asking for a drop is itself consent.
    [[ -n "${2:-}" ]] || die "usage: friend.sh link <file-or-folder>"
    [[ -e "$2" ]] || die "no such file: $2"
    require_friend
    "$DROP" send "$2" --name "from your friend" --socket "$FRIEND_SOCK" \
      | grep -o 'iroh-drop://receive/[a-z0-9]*'
    echo
    echo "Click it, or paste it into the app's Receive box." >&2
    ;;

  get)
    # The other direction: the friend fetches what you shared from the app.
    [[ -n "${2:-}" ]] || die "usage: friend.sh get <link>"
    require_friend
    "$DROP" get "$2" --socket "$FRIEND_SOCK"
    ;;

  files)
    require_friend
    if [[ -z "$(ls -A "$FRIEND_DL" 2>/dev/null)" ]]; then
      echo "The friend has received nothing yet."
    else
      find "$FRIEND_DL" -type f | sed "s|$FRIEND_DL/|  |"
    fi
    ;;

  down)
    # Match on the friend's socket so the app's own helper is never touched.
    pkill -f "iroh-dropd.*$FRIEND_SOCK" 2>/dev/null || true
    sleep 0.5
    rm -f "$FRIEND_SOCK"
    echo "Friend is down. Files it received are still in $FRIEND_DL"
    ;;

  *)
    sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
