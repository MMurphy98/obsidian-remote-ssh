---
title: Conflict handling
tags: [user-guide]
description: "How obsidian-remote-ssh detects and surfaces edit conflicts. mtime preconditions, daemon-mediated rejection, the conflict modal, what's preserved vs overwritten."
---

# Conflict handling

RPC writes compare the remote file's modification time with the version the
editor last read. A changed or deleted file produces a conflict instead of
silently accepting an old edit. Unknown offline baselines use create-only
writes, which cannot replace an existing file. The plugin and daemon must
both support protocol v2 and `fs.write.preconditions.v2`.

## Resolving a conflict

For text with an in-memory ancestor, the dialog shows ancestor, mine, and
theirs. **Keep mine** or **Save merged** writes the chosen content. **Keep
theirs** explicitly discards the corresponding local edit. **Cancel**, Escape,
or closing the dialog retains a queued edit and stops replay, including any
later rename/delete operations. Missing conflict handlers and failed writes
also leave edits pending and report an error.

After an application restart, ancestors are not available. Text without an
ancestor and binary writes use **Overwrite remote** / **Cancel** instead.
Cancellation retains the queue in this fallback too. The pending-edits status
shows retained work; it retries on the next connection. Further saves queue
behind it, so reconnect before expecting those changes to reach the remote.

There is no automatic backup of a discarded or overwritten version. Choosing
an overwrite is an explicit force-write and can replace changes that arrive
while the dialog is open. Copy any version you need to keep before accepting.

## Protection limits

SFTP has no atomic conflict check. Both an SFTP connection and a downgrade
from RPC show a persistent **SFTP — no conflict protection** warning; saves
can overwrite another writer's changes.

The RPC lock coordinates upgraded daemons serving the same vault root.
Ordinary local Obsidian, terminal editors, sync tools, SFTP, and old daemons
do not participate. Their changes can still race the final write, and edits
that preserve a timestamp can escape mtime checks. See the
[reproduction and follow-up design](../../../server/INTEGRITY.md). Avoid those
concurrent writers when relying on the RPC protection.

## Reducing conflicts

Most conflicts in practice come from:

1. **Two clients open at once** — close other Obsidians before editing.
2. **A sync tool ALSO touching the vault** — pick one sync mechanism. obsidian-remote-ssh does not play well with Syncthing pointed at the same directory.
3. **Cron / scripts writing into the vault** — schedule them to write at idle hours, or rewrite to use the daemon's [[en/api/filesystem|fs.write]] (with proper mtime tracking).

Next: [[en/user-guide/terminal-pane|Terminal pane]].
