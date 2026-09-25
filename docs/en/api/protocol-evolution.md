---
title: Protocol evolution & versioning
tags: [api, reference, protocol]
---

# Protocol evolution & versioning

How the wire protocol changes over time, what's safe vs breaking, and how clients should detect server capabilities.

## Two version axes

The wire has **two** versions:

| Axis | Field | Today | Bumps when |
|---|---|---|---|
| **Protocol version** | `server.info.protocolVersion` (int) | `2` | A breaking change to existing methods (params/result shape, semantics, error codes) |
| **Capabilities** | `server.info.capabilities` (sorted string array) | Registered method names and semantic feature flags | A new method is added (non-breaking); an old method is removed (breaking) |

The protocol version is **the major-axis compatibility number**. Capabilities are the per-method feature flags within a protocol version.

## The strictness rule (today)

The plugin's `RpcConnection` checks `info.protocolVersion !== PROTOCOL_VERSION` (strict equality) and rejects with `ProtocolVersionTooOld (-32021)` on any mismatch. Per `plugin/src/transport/RpcConnection.ts:71`:

```typescript
if (info.protocolVersion !== PROTOCOL_VERSION) {
  // → fall through to redeploy or fail the connect
}
```

So today, **plugin and daemon must agree exactly on the protocol version**. There's no forward- or backward-compat negotiation. The `tryReuseExistingDaemon` probe relies on this: a version mismatch fails the handshake, the deploy fallback uploads the matching binary, version matches, connect succeeds. See [[en/operations/upgrading|Upgrading]] for the lifecycle view.

This is intentional pre-2.0 — the cost of a redeploy is small (~5 s) and the strictness avoids "subtly broken" runs where a client uses a method the daemon implements differently.

## What's a breaking change

Bumps `protocolVersion` (for example 1 → 2):

- Renaming an existing method
- Removing a method
- Removing or renaming a field in an existing method's params or result
- Changing the type of a field (e.g. `mtime: number` → `mtime: string`)
- Changing semantics of an existing field (e.g. "mtime is unix-ms" → "mtime is unix-seconds")
- Changing the framing format (currently LSP-style `Content-Length: <N>\r\n\r\n`)
- Adding a required field to an existing method's params

## What's a non-breaking change (capabilities-gated)

Keeps the current protocol version; new methods/fields are advertised via `capabilities`:

- Adding a new method (e.g. a future `fs.search`) — added to `capabilities`; clients feature-detect by checking `capabilities.includes("fs.search")` before calling
- Adding an optional field to an existing method's params (server defaults if absent)
- Adding an optional field to an existing method's result (clients ignore unknown fields)
- Adding a new error code (clients should map unknown codes to a generic "unrecognized error" rather than crash)

## How clients should feature-detect

```typescript
const info = await rpc.call("server.info", {});
if (info.protocolVersion !== EXPECTED_PROTOCOL_VERSION) {
  // hard reject — strict-equality regime
  throw new Error("protocol version mismatch");
}
if (info.capabilities.includes("fs.thumbnail")) {
  // safe to call rpc.call("fs.thumbnail", ...)
}
```

The plugin uses this exact pattern for `fs.thumbnail` — added partway through the 0.4.x cycle. Older daemons without it would fail the capability check; the plugin falls back to `fs.readBinary` + client-side resize.

## Conditional-write protocol version 2

Version 2 changes existing write semantics: `expectedMtime=0` now requires
absence, and a nonzero version requires an existing matching file. Version 1
interpreted zero as unconditional and allowed a missing update target to be
recreated. This is a breaking semantic change, so both TS and Go constants
advance together; a capability alone cannot protect older clients from the
changed meaning. Clients also require `fs.write.preconditions.v2` for
conditional writes, failing closed if it is missing.

Deploy the matching plugin and daemon together on all clients. The existing
strict handshake rejects version 1. The SFTP fallback is still available but
has no conflict protection and displays a persistent warning. Do not downgrade
a plugin while it has pending edits: old replayers do not understand the new
queue dependencies and still have the cancellation/data-loss bug.

## Cross-version operability matrix

Historical protocol version 1 deployment behavior:

| Client version | Daemon version | Result |
|---|---|---|
| 1.0.X | 1.0.X (same) | Match — full speed (reuse probe attaches to running daemon) |
| 1.0.X | 1.0.Y (Y < X) | Mismatch on `info.version` string — reuse probe fails, plugin redeploys to bring daemon to 1.0.X |
| 1.0.X | 1.0.Y (Y > X) | Same flow — older client redeploys older binary over the newer one |
| 1.0.X | 2.0.X (future) | `protocolVersion 2 ≠ 1` → `ProtocolVersionTooOld` → redeploy fallback uploads the bundled (1.0.X-matching) daemon |

The reuse probe is the safety net. **Users do not see protocol drift errors** — they see a one-time ~5-second redeploy on the first connect after upgrading either side.

## When we'd bump to protocol 2

The pre-1.0 line saw zero breaking proto changes, and 1.x is committed to the same. Hypothetical triggers for a `protocolVersion 2`:

- A wire-format change (e.g. moving from JSON-RPC to gRPC for performance)
- A semantics change like "mtime now means content-modification time, not metadata-modification time" — semantically incompatible at the per-message level
- A protocol-level auth change (e.g. moving from per-startup token to per-call signed nonce)

When that happens:

1. Daemon supports both protocol versions for one release (returns `protocolVersion: 2` from new daemons; old clients see this and redeploy).
2. Plugin bundle gets updated to expect protocol 2.
3. Capabilities reset / re-curated as part of the 2.0 audit.

## Mobile / WSS transport (v2.0 milestone)

The mobile-relay plan ([[en/roadmap|tracked under #151]]) introduces a WSS transport alongside SSH. The transport is independent of the JSON-RPC protocol — same `auth` / `server.info` / `fs.*` methods, different framing + connection setup. The protocol version itself does **not** need to bump just to add a new transport.

## See also

- [[en/api/overview|API overview]] — the wire format + framing
- [[en/api/authentication|Authentication]] — `auth` + `server.info` handshake details
- [[en/architecture/release-pipeline|Release & deploy pipeline]] — how version coherence is enforced from the release-machinery side
- [[en/operations/upgrading|Upgrading]] — what users see when versions don't match
- [[en/glossary|Glossary]] — protocolVersion definition
