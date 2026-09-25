import { describe, it, expect, afterEach, vi } from 'vitest';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import * as os from 'node:os';
import { OfflineQueue, type QueuedOp } from '../src/offline/OfflineQueue';

// Keep real temporary-file I/O while allowing individual failure/race points
// to be controlled; native ESM fs exports themselves are non-configurable.
vi.mock('node:fs/promises', async (importOriginal) => ({
  ...await importOriginal<typeof import('node:fs/promises')>(),
}));

afterEach(() => vi.restoreAllMocks());

async function tempDir(label: string): Promise<string> {
  return await fs.mkdtemp(path.join(os.tmpdir(), `remote-ssh-queue-${label}-`));
}

function textWrite(file: string, content: string): QueuedOp {
  return {
    kind: 'write',
    path: file,
    contentBase64: Buffer.from(content, 'utf8').toString('base64'),
  };
}

describe('OfflineQueue', () => {
  // ─── basic round-trip ───────────────────────────────────────────────────

  it('starts empty', async () => {
    const q = await OfflineQueue.open(await tempDir('empty'));
    expect(q.pending()).toEqual([]);
    expect(q.stats()).toMatchObject({ entries: 0, bytes: 0 });
  });

  it('enqueue assigns increasing ids and exposes the op', async () => {
    const q = await OfflineQueue.open(await tempDir('enqueue'));
    const id1 = await q.enqueue(textWrite('a.md', 'hi'));
    const id2 = await q.enqueue(textWrite('b.md', 'bye'));
    expect(id2).toBeGreaterThan(id1);
    const pending = q.pending();
    expect(pending).toHaveLength(2);
    expect(pending[0].id).toBe(id1);
    expect(pending[1].id).toBe(id2);
    expect(pending[0].op.kind).toBe('write');
    expect((pending[0].op as { path: string }).path).toBe('a.md');
  });

  it('markCompleted removes the entry from pending', async () => {
    const q = await OfflineQueue.open(await tempDir('mark'));
    const id1 = await q.enqueue(textWrite('a.md', 'hi'));
    const id2 = await q.enqueue(textWrite('b.md', 'bye'));
    await q.markCompleted(id1);
    const remaining = q.pending();
    expect(remaining).toHaveLength(1);
    expect(remaining[0].id).toBe(id2);
  });

  it('markCompleted of an unknown id is a no-op', async () => {
    const q = await OfflineQueue.open(await tempDir('mark-unknown'));
    await q.enqueue(textWrite('a.md', 'hi'));
    await q.markCompleted(99999);
    expect(q.pending()).toHaveLength(1);
  });

  // ─── persistence across instances ───────────────────────────────────────

  it('persists pending entries across reopen', async () => {
    const dir = await tempDir('persist');
    const q1 = await OfflineQueue.open(dir);
    await q1.enqueue(textWrite('a.md', 'first'));
    await q1.enqueue(textWrite('b.md', 'second'));

    const q2 = await OfflineQueue.open(dir);
    const pending = q2.pending();
    expect(pending.map(e => (e.op as { path: string }).path)).toEqual(['a.md', 'b.md']);
  });

  it('replays past completions on reopen so already-drained ops do not resurface', async () => {
    const dir = await tempDir('persist-completed');
    const q1 = await OfflineQueue.open(dir);
    const id1 = await q1.enqueue(textWrite('a.md', 'first'));
    await q1.enqueue(textWrite('b.md', 'second'));
    await q1.markCompleted(id1);

    const q2 = await OfflineQueue.open(dir);
    const pending = q2.pending();
    expect(pending).toHaveLength(1);
    expect((pending[0].op as { path: string }).path).toBe('b.md');
  });

  it('preserves the monotonic id stream across reopen', async () => {
    const dir = await tempDir('id-stream');
    const q1 = await OfflineQueue.open(dir);
    const ids = [
      await q1.enqueue(textWrite('a.md', '1')),
      await q1.enqueue(textWrite('b.md', '2')),
      await q1.enqueue(textWrite('c.md', '3')),
    ];
    const q2 = await OfflineQueue.open(dir);
    const next = await q2.enqueue(textWrite('d.md', '4'));
    expect(next).toBeGreaterThan(Math.max(...ids));
  });

  it('chains successive text and binary saves through real committed mtimes across restart and compaction', async () => {
    const dir = await tempDir('write-chain');
    const q = await OfflineQueue.open(dir);
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    const second = await q.enqueueWrite({ kind: 'writeBinary', path: 'a.md', contentBase64: 'Yg==', expectedMtime: 9999 });
    const third = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'Yw==', expectedMtime: 10000 });
    expect(q.pending()[1].op).toMatchObject({ expectedMtime: 100, dependsOn: first });
    expect(q.pending()[2].op).toMatchObject({ expectedMtime: 100, dependsOn: second });

    await q.markCompleted(first, 200);
    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending()[0].op).toMatchObject({ expectedMtime: 200 });
    expect(reopened.pending()[0].op).not.toHaveProperty('dependsOn');
    expect(reopened.pending()[1].op).toMatchObject({ expectedMtime: 100, dependsOn: second });
    await reopened.markCompleted(second, 300);
    await reopened.compact();
    const compacted = await OfflineQueue.open(dir);
    expect(compacted.pending()).toHaveLength(1);
    expect(compacted.pending()[0]).toMatchObject({ id: third, op: { expectedMtime: 300 } });
    expect(compacted.pending()[0].op).not.toHaveProperty('dependsOn');
  });

  it('discarding a predecessor retains the original precondition instead of accepting the remote version', async () => {
    const dir = await tempDir('discard-chain');
    const q = await OfflineQueue.open(dir);
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'Yg==', expectedMtime: 9999 });

    await q.markCompleted(first);

    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending()[0].op).toMatchObject({ expectedMtime: 100 });
    expect(reopened.pending()[0].op).not.toHaveProperty('dependsOn');
  });

  it('keeps both the edit and dependency unchanged if persisting completion fails', async () => {
    const dir = await tempDir('completion-failed');
    const q = await OfflineQueue.open(dir);
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'Yg==', expectedMtime: 9999 });
    const before = q.pending();
    vi.spyOn(fs, 'appendFile').mockRejectedValueOnce(new Error('disk full'));

    await expect(q.markCompleted(first, 200)).rejects.toThrow('disk full');

    expect(q.pending()).toEqual(before);
    expect((await OfflineQueue.open(dir)).pending()).toEqual(before);
    await q.markCompleted(first, 200);
    expect(q.pending()[0].op).toMatchObject({ expectedMtime: 200 });
    expect(q.pending()[0].op).not.toHaveProperty('dependsOn');
  });

  it('includes a concurrently queued successor in the persisted completion update', async () => {
    const dir = await tempDir('completion-save-race');
    const q = await OfflineQueue.open(dir);
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    const saving = q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'Yg==', expectedMtime: 9999 });
    const completing = q.markCompleted(first, 200);
    await Promise.all([saving, completing]);

    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending()).toHaveLength(1);
    expect(reopened.pending()[0].op).toMatchObject({ expectedMtime: 200 });
    expect(reopened.pending()[0].op).not.toHaveProperty('dependsOn');
  });

  it('updates every dependent when a legacy or restored queue contains sibling dependencies', async () => {
    const dir = await tempDir('completion-all-dependents');
    const q = await OfflineQueue.open(dir);
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueue({ kind: 'write', path: 'a.md', contentBase64: 'Yg==', expectedMtime: 100, dependsOn: first });
    await q.enqueue({ kind: 'write', path: 'a.md', contentBase64: 'Yw==', expectedMtime: 100, dependsOn: first });

    await q.markCompleted(first, 200);

    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending()).toHaveLength(2);
    for (const entry of reopened.pending()) {
      expect(entry.op).toMatchObject({ expectedMtime: 200 });
      expect(entry.op).not.toHaveProperty('dependsOn');
    }
  });

  it('follows file and directory rename chains without confusing sibling paths', async () => {
    const q = await OfflineQueue.open(await tempDir('rename-chain'));
    const first = await q.enqueueWrite({ kind: 'write', path: 'old/a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueue({ kind: 'rename', oldPath: 'old', newPath: 'new' });
    await q.enqueue({ kind: 'rename', oldPath: 'new/a.md', newPath: 'new/b.md' });
    await q.enqueueWrite({ kind: 'write', path: 'new/b.md', contentBase64: 'Yg==', expectedMtime: 9999 });
    await q.enqueueWrite({ kind: 'write', path: 'older/a.md', contentBase64: 'Yw==', expectedMtime: 500 });

    expect(q.pending()[3].op).toMatchObject({ expectedMtime: 100, dependsOn: first });
    expect(q.pending()[4].op).toMatchObject({ expectedMtime: 500 });
    expect(q.pending()[4].op).not.toHaveProperty('dependsOn');
    await q.markCompleted(first, 200);
    expect(q.pending()[2].op).toMatchObject({ expectedMtime: 200 });
  });

  it.each<QueuedOp>([
    { kind: 'remove', path: 'dir/a.md' },
    { kind: 'rmdir', path: 'dir', recursive: true },
    { kind: 'trashLocal', path: 'dir/a.md' },
    { kind: 'rename', oldPath: 'dir', newPath: 'moved' },
  ])('starts a new create-only generation after $kind', async (mutation) => {
    const q = await OfflineQueue.open(await tempDir(`generation-${mutation.kind}`));
    await q.enqueueWrite({ kind: 'write', path: 'dir/a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueue(mutation);
    await q.enqueueWrite({ kind: 'write', path: 'dir/a.md', contentBase64: 'Yg==', expectedMtime: 9999 });

    expect(q.pending()[2].op).toMatchObject({ expectedMtime: 0 });
    expect(q.pending()[2].op).not.toHaveProperty('dependsOn');
  });

  it.each<QueuedOp>([
    { kind: 'append', path: 'a.md', contentBase64: 'Yg==' },
    { kind: 'appendBinary', path: 'a.md', contentBase64: 'Yg==' },
    { kind: 'copy', srcPath: 'b.md', dstPath: 'a.md' },
  ])('does not propagate a precondition across an untracked $kind mutation', async (mutation) => {
    const q = await OfflineQueue.open(await tempDir(`barrier-${mutation.kind}`));
    const first = await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'YQ==', expectedMtime: 100 });
    await q.enqueue(mutation);
    await q.enqueueWrite({ kind: 'write', path: 'a.md', contentBase64: 'Yw==', expectedMtime: 100 });
    await q.markCompleted(first, 200);

    expect(q.pending()[1].op).toMatchObject({ expectedMtime: 100 });
    expect(q.pending()[1].op).not.toHaveProperty('dependsOn');
  });

  it('treats a write with no known remote base as create-only', async () => {
    const q = await OfflineQueue.open(await tempDir('unknown-base'));
    await q.enqueueWrite({ kind: 'write', path: 'new.md', contentBase64: 'YQ==' });
    expect(q.pending()[0].op).toMatchObject({ expectedMtime: 0 });
  });

  // ─── compaction ─────────────────────────────────────────────────────────

  it('compaction shrinks the on-disk log after enough completions', async () => {
    const dir = await tempDir('compact');
    const q = await OfflineQueue.open(dir);
    // Push enough payload that the log gets fat, then complete most
    // of them so the slack triggers a rewrite.
    const big = 'x'.repeat(2000);
    const ids: number[] = [];
    for (let i = 0; i < 6; i++) {
      ids.push(await q.enqueue(textWrite(`note-${i}.md`, big)));
    }
    const before = q.stats();
    for (let i = 0; i < 5; i++) {
      await q.markCompleted(ids[i]);
    }
    const after = q.stats();
    // Compaction is triggered by the last markCompleted; after, the
    // log file should be ~ a single op's worth, much less than before.
    expect(after.entries).toBe(1);
    expect(after.logFileBytes).toBeLessThan(before.logFileBytes);
  });

  it('clear() drops every entry and writes an empty log', async () => {
    const dir = await tempDir('clear');
    const q = await OfflineQueue.open(dir);
    await q.enqueue(textWrite('a.md', 'one'));
    await q.enqueue(textWrite('b.md', 'two'));
    await q.clear();
    expect(q.pending()).toEqual([]);
    expect(q.stats().bytes).toBe(0);

    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending()).toEqual([]);
  });

  it('does not reuse completed IDs after compaction empties the queue', async () => {
    const dir = await tempDir('compacted-id-stream');
    const q = await OfflineQueue.open(dir);
    const id = await q.enqueue(textWrite('a.md', 'first'));
    await q.markCompleted(id);
    await q.compact();
    const reopened = await OfflineQueue.open(dir);
    expect(await reopened.enqueue(textWrite('a.md', 'second'))).toBeGreaterThan(id);
  });

  it('keeps a save queued while a compaction replacement is in progress', async () => {
    const dir = await tempDir('compact-save-race');
    const q = await OfflineQueue.open(dir);
    await q.enqueue(textWrite('a.md', 'first'));
    const originalRename = fs.rename;
    let release!: () => void;
    let entered!: () => void;
    const blocked = new Promise<void>(resolve => { release = resolve; });
    const started = new Promise<void>(resolve => { entered = resolve; });
    vi.spyOn(fs, 'rename').mockImplementationOnce(async (from, to) => {
      entered();
      await blocked;
      return originalRename(from, to);
    });
    const append = vi.spyOn(fs, 'appendFile');
    const compacting = q.compact();
    await started;
    const saving = q.enqueue(textWrite('b.md', 'second'));
    try {
      // No append may race with replacing the log it appends to.
      await Promise.resolve();
      expect(append).not.toHaveBeenCalled();
    } finally {
      release();
      await Promise.all([compacting, saving]);
    }
    const reopened = await OfflineQueue.open(dir);
    expect(reopened.pending().map(e => (e.op as { path: string }).path)).toEqual(['a.md', 'b.md']);
  });

  it('retains pending edits in memory when clearing fails to persist', async () => {
    const q = await OfflineQueue.open(await tempDir('clear-failed'));
    await q.enqueue(textWrite('a.md', 'first'));
    vi.spyOn(fs, 'writeFile').mockRejectedValueOnce(new Error('disk full'));

    await expect(q.clear()).rejects.toThrow('disk full');

    expect(q.pending()).toHaveLength(1);
    await q.enqueue(textWrite('b.md', 'second'));
    expect(q.pending()).toHaveLength(2);
  });

  // ─── cap enforcement ────────────────────────────────────────────────────

  it('rejects an enqueue that would exceed the byte cap', async () => {
    const dir = await tempDir('cap');
    // Cap chosen so a small write fits but a 4 KB payload doesn't.
    const q = await OfflineQueue.open(dir, { maxBytes: 1000 });
    const ok = textWrite('small.md', 'x'.repeat(20));
    await q.enqueue(ok);
    const big = textWrite('big.md', 'x'.repeat(4000));
    await expect(q.enqueue(big)).rejects.toThrow(/exceed cap/);
    // The rejected enqueue must NOT consume an id slot or appear in pending.
    const pending = q.pending();
    expect(pending).toHaveLength(1);
    // Next enqueue should still get a small id (no gap from the rejected one).
    const next = await q.enqueue(textWrite('next.md', 'tiny'));
    expect(next).toBe(pending[0].id + 1);
  });

  it('enforces the byte cap across concurrent saves', async () => {
    const q = await OfflineQueue.open(await tempDir('cap-concurrent'), { maxBytes: 250 });
    const results = await Promise.allSettled([
      q.enqueue(textWrite('a.md', 'x'.repeat(50))),
      q.enqueue(textWrite('b.md', 'x'.repeat(50))),
    ]);
    expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1);
    expect(q.pending()).toHaveLength(1);
    expect(q.stats().bytes).toBeLessThanOrEqual(250);
  });

  // ─── malformed-line tolerance ───────────────────────────────────────────

  it('skips malformed lines in the log on reopen rather than failing to load', async () => {
    const dir = await tempDir('malformed');
    const q1 = await OfflineQueue.open(dir);
    await q1.enqueue(textWrite('good.md', 'ok'));

    // Append some garbage and a partially-broken record.
    const logPath = path.join(dir, 'log.jsonl');
    await fs.appendFile(logPath, 'not-json\n{"type":"unknown"}\n', 'utf8');
    await q1.enqueue(textWrite('also-good.md', 'ok2'));

    const q2 = await OfflineQueue.open(dir);
    const pending = q2.pending();
    expect(pending.map(e => (e.op as { path: string }).path)).toEqual(['good.md', 'also-good.md']);
  });

  // ─── all op kinds ───────────────────────────────────────────────────────

  it('persists every supported op kind verbatim', async () => {
    const dir = await tempDir('kinds');
    const q = await OfflineQueue.open(dir);
    const ops: QueuedOp[] = [
      { kind: 'write',        path: 'a.md', contentBase64: 'YQ==' },
      { kind: 'writeBinary',  path: 'a.bin', contentBase64: 'YQ==' },
      { kind: 'append',       path: 'a.md', contentBase64: 'YQ==' },
      { kind: 'appendBinary', path: 'a.bin', contentBase64: 'YQ==' },
      { kind: 'mkdir',        path: 'sub' },
      { kind: 'remove',       path: 'a.md' },
      { kind: 'rmdir',        path: 'sub', recursive: true },
      { kind: 'rename',       oldPath: 'a.md', newPath: 'b.md' },
      { kind: 'copy',         srcPath: 'a.md', dstPath: 'b.md' },
      { kind: 'trashLocal',   path: 'a.md' },
    ];
    for (const op of ops) await q.enqueue(op);

    const reopened = await OfflineQueue.open(dir);
    const got = reopened.pending().map(e => e.op);
    expect(got).toEqual(ops);
  });
});
