iroh-drop for Windows
=====================

Decentralized AirDrop: no accounts, no cloud. Peers find each other by
link or on the local network; everyone who receives helps serve.

Quick start
-----------
1. Double-click Drop.exe
   - It quietly starts the helper (iroh-dropd.exe) next to it. The helper
     is what keeps your shared files reachable after you close the window.
2. To SEND: drag files into the window (or use Share) -> you get a
     drop1... link. Send that link to the other person any way you like.
3. To RECEIVE: paste the drop1... link you were sent, or just accept the
     card that pops up when someone on your network offers you files.

The link works across the internet (encrypted relay fallback) and on the
local network (direct). Nothing is uploaded anywhere.

Command line (optional)
-----------------------
iroh-drop.exe send <file>     share through the helper, prints a link
iroh-drop.exe get <link>      receive a link
iroh-drop.exe watch           approve incoming offers in the terminal
iroh-drop.exe nearby          list drops on the local network
iroh-dropd.exe --lan-only     run the helper with no relay/DNS at all

Where things live
-----------------
%LOCALAPPDATA%\iroh-drop\   identity, blob store, drop history
Downloads\iroh-drop\        received files

Notes
-----
- First launch may trigger a firewall prompt: iroh-drop talks QUIC/UDP
  to peers (and mDNS on the LAN). Allow private networks at least.
- The helper and the app talk over a named pipe private to your user
  account (\\.\pipe\iroh-drop\control, remote clients rejected).
- Requires Windows 10+ with working graphics drivers (DirectX/Vulkan/GL).
