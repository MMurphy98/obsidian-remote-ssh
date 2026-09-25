import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { SftpDataAdapter } from '../../src/adapter/SftpDataAdapter';
import { ReadCache } from '../../src/cache/ReadCache';
import { DirCache } from '../../src/cache/DirCache';
import { AncestorTracker } from '../../src/conflict/AncestorTracker';
import { ConflictResolver, type TextConflictCallback, type WriteConflictCallback } from '../../src/conflict/ConflictResolver';
import { OfflineQueue } from '../../src/offline/OfflineQueue';
import { QueueReplayer } from '../../src/offline/QueueReplayer';
import { localRpcDaemon } from './helpers/localRpcDaemon';

/** Run independently of Docker:
 * npm exec vitest run -- --config vitest.integration.config.ts tests/integration/offline-replay.rpc.test.ts
 * A built host daemon is required; every vault and queue is newly created. */
describe('integration: durable offline replay over real RPC', () => {
  let daemon: Awaited<ReturnType<typeof localRpcDaemon>>;
  let queue: OfflineQueue;
  let adapter: SftpDataAdapter;
  let queueDir: string;

  function makeAdapter(onTextConflict: TextConflictCallback | null = null, onWriteConflict: WriteConflictCallback | null = null): SftpDataAdapter {
    const cache = new ReadCache();
    const ancestor = new AncestorTracker();
    const resolver = onTextConflict || onWriteConflict
      ? new ConflictResolver(daemon.client, cache, ancestor, onTextConflict, onWriteConflict)
      : null;
    return new SftpDataAdapter(daemon.client, '', cache, new DirCache(), 'test-vault', null, null, resolver, ancestor, queue);
  }

  async function seed(name: string, content: string | Buffer, mtime = 1_600_000_000_000): Promise<void> {
    const target = path.join(daemon.vault, name);
    await fs.mkdir(path.dirname(target), { recursive: true });
    await fs.writeFile(target, content);
    const timestamp = new Date(mtime);
    await fs.utimes(target, timestamp, timestamp);
  }

  async function restart(): Promise<void> {
    // Keep synthetic offline timestamps separate from a subsequent filesystem
    // write's millisecond mtime; otherwise the old bug can pass by coincidence.
    await new Promise((resolve) => setTimeout(resolve, 10));
    queue = await OfflineQueue.open(queueDir);
    adapter = makeAdapter();
  }

  beforeEach(async () => {
    daemon = await localRpcDaemon();
    queueDir = path.join(daemon.root, 'queue');
    queue = await OfflineQueue.open(queueDir);
    adapter = makeAdapter();
  });

  afterEach(async () => { await daemon?.close(); });

  it('replays consecutive text saves after the application loses its caches', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first offline save');
    await adapter.write('note.md', 'latest offline save');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('latest offline save');
  });

  it('replays consecutive binary saves after restart', async () => {
    await seed('attachment.bin', Buffer.from([0, 1, 255]));
    await adapter.readBinary('attachment.bin');
    adapter.setReconnecting(true);
    await adapter.writeBinary('attachment.bin', new Uint8Array([1, 0, 254]).buffer);
    await adapter.writeBinary('attachment.bin', new Uint8Array([2, 0, 253]).buffer);
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'attachment.bin'))).toEqual(Buffer.from([2, 0, 253]));
  });

  it('retains Cancel and all later dependent edits on disk', async () => {
    const cancel = vi.fn(async () => false);
    adapter = makeAdapter(null, cancel);
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await adapter.write('note.md', 'latest local');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    adapter.setReconnecting(false);
    await new QueueReplayer(queue, adapter).run();
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(queue.pending()).toHaveLength(2);
    await restart();
    expect(queue.pending()).toHaveLength(2);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external edit');
  });

  it('reports a missing conflict handler and retains the pending edit', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'local edit');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toHaveLength(1);
    expect(report.errors[0].message).toMatch(/conflict|precondition/i);
    expect(queue.pending()).toHaveLength(1);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external edit');
  });

  it('persists a later online save after cancelling replay', async () => {
    const cancel = vi.fn(async () => false);
    adapter = makeAdapter(null, cancel);
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    adapter.setReconnecting(false);
    await new QueueReplayer(queue, adapter).run();
    await adapter.write('note.md', 'later local save while connected');
    expect(cancel).toHaveBeenCalledTimes(1);
    await restart();
    expect(queue.pending()).toHaveLength(2);
    expect(Buffer.from((queue.pending()[1].op as { contentBase64: string }).contentBase64, 'base64').toString()).toBe('later local save while connected');
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external edit');
  });

  it('persists a new save while replay is waiting for the conflict decision', async () => {
    let decide!: (overwrite: boolean) => void;
    const decision = new Promise<boolean>((resolve) => { decide = resolve; });
    const prompt = vi.fn(() => decision);
    adapter = makeAdapter(null, prompt);
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    adapter.setReconnecting(false);
    const replaying = new QueueReplayer(queue, adapter).run();
    await vi.waitFor(() => expect(prompt).toHaveBeenCalledTimes(1));
    const saving = adapter.write('note.md', 'saved while modal open');
    try {
      await vi.waitFor(() => expect(queue.pending()).toHaveLength(2));
      expect(prompt).toHaveBeenCalledTimes(1);
    } finally {
      decide(false);
      await Promise.allSettled([replaying, saving]);
    }
    await restart();
    expect(queue.pending()).toHaveLength(2);
    expect(Buffer.from((queue.pending()[1].op as { contentBase64: string }).contentBase64, 'base64').toString()).toBe('saved while modal open');
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external edit');
  });

  it('drains a new save queued during an accepted conflict in the same replay run', async () => {
    let decide!: (overwrite: boolean) => void;
    const decision = new Promise<boolean>((resolve) => { decide = resolve; });
    const prompt = vi.fn(() => decision);
    adapter = makeAdapter(null, prompt);
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    adapter.setReconnecting(false);
    const replaying = new QueueReplayer(queue, adapter).run();
    try {
      await vi.waitFor(() => expect(prompt).toHaveBeenCalledTimes(1));
      await adapter.write('note.md', 'newest save while modal open');
    } finally {
      decide(true);
    }
    const report = await replaying;
    expect(report.errors).toEqual([]);
    expect(report.drained).toBe(2);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('newest save while modal open');
  });

  it('completes an edit only after explicit keep-theirs', async () => {
    const keepTheirs = vi.fn(async () => ({ decision: 'keep-theirs' as const }));
    adapter = makeAdapter(keepTheirs);
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'local edit');
    await seed('note.md', 'external edit', 1_650_000_000_000);
    adapter.setReconnecting(false);
    const report = await new QueueReplayer(queue, adapter).run();
    expect(keepTheirs).toHaveBeenCalledTimes(1);
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(1);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external edit');
  });

  it('preserves a real external edit between two replayed saves', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await adapter.write('note.md', 'latest local');
    adapter.setReconnecting(false);
    let replayed = 0;
    const report = await new QueueReplayer(queue, {
      async replayQueuedOp(op) {
        const result = await adapter.replayQueuedOp(op);
        if (++replayed === 1) await seed('note.md', 'external during replay', 1_700_000_000_000);
        return result;
      },
    }).run();
    expect(report.errors).toHaveLength(1);
    expect(queue.pending()).toHaveLength(1);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external during replay');
  });

  it('resumes remaining saves after an interrupted replay and application restart', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'first local');
    await adapter.write('note.md', 'latest local');
    adapter.setReconnecting(false);
    await new Promise((resolve) => setTimeout(resolve, 10));
    let replayed = 0;
    await new QueueReplayer(queue, {
      async replayQueuedOp(op) {
        if (++replayed === 2) return { result: 'error', message: 'test disconnect before second write' };
        return adapter.replayQueuedOp(op);
      },
    }).run();
    expect(queue.pending()).toHaveLength(1);
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('latest local');
  });

  it('does not recreate a remotely deleted note from a stale offline edit', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'stale local edit');
    await fs.unlink(path.join(daemon.vault, 'note.md'));
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toHaveLength(1);
    expect(queue.pending()).toHaveLength(1);
    await expect(fs.stat(path.join(daemon.vault, 'note.md'))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('preserves queued rename ordering between saves across restart', async () => {
    await seed('old.md', 'base');
    await adapter.read('old.md');
    adapter.setReconnecting(true);
    await adapter.write('old.md', 'before rename');
    await adapter.rename('old.md', 'new.md');
    await adapter.write('new.md', 'after rename');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    await expect(fs.stat(path.join(daemon.vault, 'old.md'))).rejects.toMatchObject({ code: 'ENOENT' });
    expect(await fs.readFile(path.join(daemon.vault, 'new.md'), 'utf8')).toBe('after rename');
  });

  it('preserves queued delete/recreate ordering across restart', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.write('note.md', 'before delete');
    await adapter.remove('note.md');
    await adapter.write('note.md', 'new note after delete');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('new note after delete');
  });

  it('tracks a note through a queued directory rename', async () => {
    await seed('folder/note.md', 'base');
    await adapter.read('folder/note.md');
    adapter.setReconnecting(true);
    await adapter.write('folder/note.md', 'before directory rename');
    await adapter.rename('folder', 'renamed');
    await adapter.write('renamed/note.md', 'after directory rename');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'renamed/note.md'), 'utf8')).toBe('after directory rename');
    await expect(fs.stat(path.join(daemon.vault, 'folder/note.md'))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('treats a saved file at the old rename path as a new file', async () => {
    await seed('old.md', 'base');
    await adapter.read('old.md');
    adapter.setReconnecting(true);
    await adapter.write('old.md', 'move this version');
    await adapter.rename('old.md', 'new.md');
    await adapter.write('old.md', 'a different new note');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(report.conflicts).toBe(0);
    expect(queue.pending()).toEqual([]);
    expect(await fs.readFile(path.join(daemon.vault, 'old.md'), 'utf8')).toBe('a different new note');
    expect(await fs.readFile(path.join(daemon.vault, 'new.md'), 'utf8')).toBe('move this version');
  });

  it('does not overwrite an external edit after replaying a rename', async () => {
    await seed('old.md', 'base');
    await adapter.read('old.md');
    adapter.setReconnecting(true);
    await adapter.write('old.md', 'before rename');
    await adapter.rename('old.md', 'new.md');
    await adapter.write('new.md', 'after rename');
    await restart();
    const report = await new QueueReplayer(queue, {
      async replayQueuedOp(op) {
        const result = await adapter.replayQueuedOp(op);
        if (op.kind === 'rename') await seed('new.md', 'external after rename', 1_700_000_000_000);
        return result;
      },
    }).run();
    expect(report.errors).toHaveLength(1);
    expect(queue.pending()).toHaveLength(1);
    expect(await fs.readFile(path.join(daemon.vault, 'new.md'), 'utf8')).toBe('external after rename');
  });

  it('does not overwrite a file created externally after a queued delete', async () => {
    await seed('note.md', 'base');
    await adapter.read('note.md');
    adapter.setReconnecting(true);
    await adapter.remove('note.md');
    await adapter.write('note.md', 'local recreated note');
    await restart();
    const report = await new QueueReplayer(queue, {
      async replayQueuedOp(op) {
        const result = await adapter.replayQueuedOp(op);
        if (op.kind === 'remove') await seed('note.md', 'external recreated note', 1_700_000_000_000);
        return result;
      },
    }).run();
    expect(report.errors).toHaveLength(1);
    expect(queue.pending()).toHaveLength(1);
    expect(await fs.readFile(path.join(daemon.vault, 'note.md'), 'utf8')).toBe('external recreated note');
  });

  it('does not overwrite an existing remote file when an offline write has no cached baseline', async () => {
    await seed('unknown.md', 'unseen remote content');
    adapter.setReconnecting(true);
    await adapter.write('unknown.md', 'local content without a baseline');
    await restart();
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toHaveLength(1);
    expect(queue.pending()).toHaveLength(1);
    expect(await fs.readFile(path.join(daemon.vault, 'unknown.md'), 'utf8')).toBe('unseen remote content');
  });
});
