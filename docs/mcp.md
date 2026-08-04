# The MCP server — iroh-drop for agents

`iroh-drop-mcp` is an [MCP](https://modelcontextprotocol.io) stdio server
that lets an agent (Claude Desktop, any MCP-capable client) use iroh-drop
through a running daemon. It is the **third consumer of the daemon's control
API** — after the CLI and the GUI — and that is the point: the agent gets no
privileged hooks, because none are needed. Everything it can do is a method
any client can call.

The crate's entire iroh-drop dependency list is `iroh-drop-daemon`. It never
imports the protocol crate.

## Run it

The daemon must be running first:

```sh
iroh-dropd &                 # the peer: identity, store, sessions
iroh-drop-mcp                # the agent's hands: stdio JSON-RPC (MCP)
```

`--socket PATH` overrides the daemon socket location.

## Configure an agent

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "iroh-drop": {
      "command": "iroh-drop-mcp"
    }
  }
}
```

Generic MCP JSON (same shape, wherever your client keeps it):

```json
{
  "command": "iroh-drop-mcp",
  "args": [],
  "env": {}
}
```

## Tools

| Tool | What it does |
|---|---|
| `list_drops` | Drops this daemon is in: handle, name, peers, offers. |
| `create_drop` | New drop; returns handle, bearer ticket, shareable link. |
| `join_drop` | Join from a bare `drop2…`, an `iroh-drop://` link, an `https://…#drop2…` link, or text containing one. |
| `leave_drop` | Leave; `forget: true` also stops serving the content. |
| `list_offers` | Numbered offers with name, hash, size, status. |
| `share_files` | Publish local files into a drop; waits and reports. |
| `fetch` | Fetch one item by number, name, or hash prefix. **This call is the user's consent** for that item. |
| `drop_events` | Bounded tail of the event log (joins, offers, progress). |

The `initialize` handshake also hands the agent written instructions stating
the consent rule, so it travels with the server instead of depending on the
host's system prompt.

## The consent rule for agents (normative)

An MCP client connects with the daemon's **control** role, never the **ui**
role. Consequences, enforced structurally by the daemon:

- The daemon routes consent questions ("accept this offer?") only to ui-role
  clients. An MCP client is never asked — so an agent's presence can never
  cause an unsolicited offer to be accepted. It waits for a human.
- The agent's own `fetch` call is the ask-and-answer in one step, for that
  one item only.

`crates/iroh-drop-mcp/tests/agent_fetch.rs` pins both halves: an offer sits
at `missing` while only the agent is connected, and a `fetch` call completes
"dataset X from drop Y" end-to-end with no shell involved.

## What the agent cannot do

Anything the daemon API cannot do — by design. There is no agent-only back
door into the protocol crate, no way to bypass policy limits, and no way to
answer consent questions meant for a human.
