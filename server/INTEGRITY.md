# Conditional write integrity

The wire protocol is **version 2**. This is a breaking change to the existing
`expectedMtime` semantics: version 1 daemons interpret zero as an unconditional
write and permit an old revision to recreate a deleted file. Clients and
daemons must agree on protocol version 2 at the handshake; old clients must
not reuse this daemon under their old assumptions.

As an additional check, `server.info` advertises `fs.write.preconditions.v2`
when both write methods are registered. Clients must also require this
capability for protected writes. All clients serving the same vault should
be upgraded together: an old client can otherwise redeploy its older daemon,
whose writes do not participate in the new locking and precondition rules.

| `expectedMtime` | Meaning for `fs.write` / `fs.writeBinary` |
| --- | --- |
| omitted / `null` | Explicit unconditional write, including creation. |
| `0` | Create only; an existing target produces `PreconditionFailed`. |
| non-zero | Update only; the target must exist and have this millisecond mtime. |

Every mutation (`write`, `writeBinary`, `append`, `appendBinary`, `copy`,
`rename`, `remove`, `rmdir`, `trashLocal`, `mkdir`) takes an exclusive advisory
`flock` on the vault root directory. Independent file descriptors coordinate
connections and separate daemon processes serving the same root inode.
The lock covers precondition checking, staging, committing, and obtaining
the result. Directory operations therefore cannot delete or move a write's
parent during that interval. Closing the descriptor releases the lock, also
on process exit; no lock file or stale lock cleanup is needed. A filesystem
that cannot acquire the lock returns an error instead of skipping protection.

Writes sync the staged file and publish it with rename. Create-only writes
use a hard link to publish without replacing an existing target, including
a file created by a writer that does not cooperate with the advisory lock.
The returned mtime comes from the staged inode, not a later lookup of the
destination path. Write/copy, append, and replacement rename advance a
previous destination's mtime by at least one millisecond when necessary,
so same-millisecond edits do not reuse that previous token.

## Regressions and validation

The added tests failed against release 1.1.9 before the corresponding fixes:

- Text and binary replay after remove, rename, recursive rmdir, or trash
  recreated deleted paths (eight cases).
- Two out of eight simultaneous writers accepted the same revision in the
  observed failing run.
- Zero-mtime writes overwrote an existing target in both text and binary
  cases.
- A subprocess wrote through another process's held vault lock.
- Rename over a destination with the same mtime left its stale revision
  valid; backwards-clock writes and appends did not advance revision tokens.

`fs_write_integrity_test.go` covers those content/precondition contracts.
`mutation_lock_test.go` includes a real subprocess and all ten mutation
handlers. The normal server socket tests verify the advertised capability.
All fixtures create new temporary directories; no existing vault is needed.

Run from `server/`:

```sh
go test ./...
go test -race ./...
```

## Remaining boundary: non-cooperating writers

This is not a filesystem-wide compare-and-swap. Local Obsidian, SFTP, older
daemons, and other programs do not acquire the vault lock. An edit or delete
after the final precondition check and before rename can still be overwritten
or resurrected. Rechecking after staging narrows this window but cannot
eliminate it. All participating daemons must be upgraded and use the same
root; overlapping/nested vault roots are not coordinated. Millisecond mtime
also cannot detect an outside writer that preserves/restores a timestamp.

The following Linux reproduction was run against the fixed implementation.
It pauses `renameat` at syscall entry using `strace`, then performs an ordinary
filesystem edit or deletion. In both cases the conditional write returned
success and left `stale offline edit` at the path. This uses only disposable
test directories and the regression-test subprocess helper.

From `server/`, with Go and strace on PATH:

```sh
python3 - <<'PY'
import os, pathlib, subprocess, tempfile, time

with tempfile.TemporaryDirectory(prefix="remote-ssh-integrity-") as work:
    work = pathlib.Path(work)
    binary = work / "handlers.test"
    subprocess.run(["go", "test", "-c", "-o", str(binary), "./internal/handlers"], check=True)
    for action in ("modify", "delete"):
        root = work / action
        root.mkdir()
        note = root / "note.md"
        note.write_text("base")
        os.utime(note, ns=(1700000000000000000, 1700000000000000000))
        trace = work / (action + ".trace")
        env = dict(os.environ, REMOTE_SSH_WRITE_TEST_ROOT=str(root),
                   REMOTE_SSH_WRITE_TEST_MTIME="1700000000000")
        child = subprocess.Popen([
            "strace", "-f", "-e", "trace=rename,renameat,renameat2", "-e",
            "inject=rename,renameat,renameat2:delay_enter=2s", "-o", str(trace),
            str(binary), "-test.run=^TestConditionalWriteProcess$",
        ], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert child.stdout.readline().strip() == "ready"
            deadline = time.monotonic() + 1.5
            while ".rsh-write-" not in trace.read_text():
                assert time.monotonic() < deadline, "rename entry was not observed"
                time.sleep(0.01)
            if action == "modify":
                note.write_text("external edit after final check")
            else:
                note.unlink()
            output, errors = child.communicate(timeout=5)
            print(action, "RPC error code:", output.splitlines()[0],
                  "final content:", note.read_text())
        finally:
            if child.poll() is None:
                child.kill()
            child.wait()
PY
```

## Independent follow-up tasks

1. **Coordinate local and remote writers.** Define one authoritative writer
   for a vault, and route local Obsidian/SFTP mutations through that service
   or a participating lock protocol. Ordinary POSIX rename cannot compare
   a destination revision atomically. Add the syscall-delay reproduction
   above to end-to-end acceptance tests once the chosen design promises
   to preserve both edits. Until then, avoid concurrent local/SFTP editing
   when relying on RPC conflict protection.
2. **Use durable revision and operation IDs.** Replace mtime-only tokens with
   durable generations/content identities and tombstones; include create,
   update, rename, and delete preconditions. Persist operation results so an
   acknowledged-on-server but lost-on-client response can be retried without
   duplicate append/rename/remove effects. Test same-mtime external rewrites,
   server/client crashes at each commit stage, and deleted/recreated paths.
3. **Validate durability and filesystem support.** Test directory flock and
   hard-link publication at runtime on macOS and supported remote filesystems;
   cross-compilation is not a runtime test. Specify network filesystem and
   overlapping-root behavior. The staged file is synced, but directory-entry
   durability across sudden power loss needs explicit parent-directory fsync
   and fault-injection coverage before making stronger durability claims.
