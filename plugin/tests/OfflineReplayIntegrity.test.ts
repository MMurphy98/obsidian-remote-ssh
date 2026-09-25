import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { SftpDataAdapter } from '../src/adapter/SftpDataAdapter';
import type { RemoteFsClient } from '../src/adapter/RemoteFsClient';
import { ReadCache } from '../src/cache/ReadCache';
import { DirCache } from '../src/cache/DirCache';
import { AncestorTracker } from '../src/conflict/AncestorTracker';
import { ConflictResolver, type TextConflictDecision } from '../src/conflict/ConflictResolver';
import { OfflineQueue } from '../src/offline/OfflineQueue';
import { QueueReplayer } from '../src/offline/QueueReplayer';

function conflict(): Error { return Object.assign(new Error('precondition failed'), { code: -32020 }); }

describe('offline replay integrity', () => {
  let dir: string;
  let queue: OfflineQueue;
  let files: Map<string, { data: Buffer; mtime: number }>;
  let client: RemoteFsClient;
  let clock: number;
  let writes: Array<{ path: string; expected?: number }>;

  beforeEach(async () => {
    dir = await fs.mkdtemp(path.join(os.tmpdir(), 'remote-ssh-integrity-'));
    queue = await OfflineQueue.open(dir);
    clock = 100;
    writes = [];
    files = new Map([['/v/note.md', { data: Buffer.from('base'), mtime: clock }]]);
    client = {
      isAlive: () => true, onClose: () => () => {}, list: async () => [], mkdirp: async () => {},
      exists: async p => files.has(p),
      stat: async p => {
        const file = files.get(p);
        if (!file) throw Object.assign(new Error('missing'), { code: -32010 });
        return { isFile: true, isDirectory: false, isSymbolicLink: false, mtime: file.mtime, size: file.data.length, mode: 0o644 };
      },
      readBinary: async p => {
        const file = files.get(p);
        if (!file) throw new Error('missing');
        return file.data;
      },
      writeBinary: async (p, data, expected) => {
        writes.push({ path: p, expected });
        const file = files.get(p);
        if (expected !== undefined && (expected === 0 ? !!file : !file || file.mtime !== expected)) throw conflict();
        const mtime = ++clock;
        files.set(p, { data, mtime });
        return mtime;
      },
      remove: async p => { files.delete(p); },
      rmdir: async p => { for (const key of files.keys()) if (key === p || key.startsWith(p + '/')) files.delete(key); },
      rename: async (a, b) => {
        for (const [key, value] of [...files]) if (key === a || key.startsWith(a + '/')) {
          files.delete(key); files.set(b + key.slice(a.length), value);
        }
      },
      copy: async (a, b) => { files.set(b, { ...files.get(a)!, mtime: ++clock }); },
    } as RemoteFsClient;
  });
  afterEach(async () => { await fs.rm(dir, { recursive: true, force: true }); });

  function adapter(decide?: () => Promise<TextConflictDecision>, legacy?: () => Promise<boolean>) {
    const cache = new ReadCache();
    const ancestors = new AncestorTracker();
    const resolver = decide || legacy ? new ConflictResolver(client, cache, ancestors, decide ?? null, legacy ?? null) : null;
    return new SftpDataAdapter(client, '/v', cache, new DirCache(), 'test', null, null, resolver, ancestors, queue);
  }
  async function saveOffline(a: SftpDataAdapter, ...contents: string[]) {
    await a.read('note.md');
    a.setReconnecting(true);
    for (const content of contents) await a.write('note.md', content);
    a.setReconnecting(false);
  }
  function external(content = 'external') { files.set('/v/note.md', { data: Buffer.from(content), mtime: ++clock }); }

  it.each(['cancel', 'missing-handler', 'callback-error', 'binary-cancel'])(
    '%s retains edits on disk and stops before a following delete', async mode => {
      const decide = mode === 'cancel' ? async () => ({ decision: 'cancel' as const })
        : mode === 'callback-error' ? async () => { throw new Error('modal failed'); } : undefined;
      const a = adapter(decide, mode === 'binary-cancel' ? async () => false : undefined);
      await saveOffline(a, 'local edit');
      a.setReconnecting(true);
      await a.remove('note.md');
      a.setReconnecting(false);
      external();
      const report = await new QueueReplayer(queue, a).run();
      expect(report.drained + report.conflicts).toBe(0);
      expect((await OfflineQueue.open(dir)).pending()).toHaveLength(2);
      expect(files.get('/v/note.md')?.data.toString()).toBe('external');
      if (mode === 'missing-handler') expect(report.errors[0]?.message).toMatch(/conflict|precondition/i);
    },
  );

  it('only an explicit keep-theirs decision discards the corresponding edit', async () => {
    const a = adapter(async () => ({ decision: 'keep-theirs' }));
    await saveOffline(a, 'local edit');
    external();
    expect((await new QueueReplayer(queue, a).run()).conflicts).toBe(1);
    expect((await OfflineQueue.open(dir)).pending()).toHaveLength(0);
    expect(files.get('/v/note.md')?.data.toString()).toBe('external');
  });

  it.each([false, true])('consecutive offline saves use real committed versions (binary=%s)', async binary => {
    const a = adapter();
    await a.read('note.md');
    a.setReconnecting(true);
    for (const text of ['one', 'two', 'three']) {
      if (binary) await a.writeBinary('note.md', new TextEncoder().encode(text).buffer);
      else await a.write('note.md', text);
    }
    queue = await OfflineQueue.open(dir);
    const report = await new QueueReplayer(queue, adapter()).run();
    expect(report).toEqual({ drained: 3, conflicts: 0, errors: [] });
    expect(files.get('/v/note.md')?.data.toString()).toBe('three');
    expect(writes.map(w => w.expected)).toEqual([100, 101, 102]);
  });

  it('persists the successor precondition when a restart follows the first completed write', async () => {
    const a = adapter();
    await saveOffline(a, 'one', 'two');
    let calls = 0;
    await new QueueReplayer(queue, { replayQueuedOp: async op => ++calls === 1
      ? a.replayQueuedOp(op) : { result: 'error', message: 'connection lost' } }).run();
    queue = await OfflineQueue.open(dir);
    expect(queue.pending()).toHaveLength(1);
    expect((await new QueueReplayer(queue, adapter()).run()).drained).toBe(1);
    expect(files.get('/v/note.md')?.data.toString()).toBe('two');
  });

  it('detects a real external edit between successive queued saves', async () => {
    const a = adapter();
    await saveOffline(a, 'one', 'two');
    let calls = 0;
    const report = await new QueueReplayer(queue, { replayQueuedOp: async op => {
      if (++calls === 2) external();
      return a.replayQueuedOp(op);
    } }).run();
    expect(report.drained).toBe(1);
    expect(report.errors).toHaveLength(1);
    expect((await OfflineQueue.open(dir)).pending()).toHaveLength(1);
    expect(files.get('/v/note.md')?.data.toString()).toBe('external');
  });

  it('carries write dependencies through directory rename without reordering operations', async () => {
    const a = adapter();
    files.set('/v/folder/note.md', files.get('/v/note.md')!);
    await a.read('folder/note.md');
    a.setReconnecting(true);
    await a.write('folder/note.md', 'one');
    await a.rename('folder', 'renamed');
    await a.write('renamed/note.md', 'two');
    queue = await OfflineQueue.open(dir);
    expect((await new QueueReplayer(queue, adapter()).run()).errors).toEqual([]);
    expect(files.get('/v/renamed/note.md')?.data.toString()).toBe('two');
    expect(writes.map(w => w.expected)).toEqual([100, 101]);
    expect(files.has('/v/folder/note.md')).toBe(false);
  });

  it('delete breaks the previous generation: recreation is create-only', async () => {
    const a = adapter();
    await a.read('note.md');
    a.setReconnecting(true);
    await a.write('note.md', 'one');
    await a.remove('note.md');
    await a.write('note.md', 'new generation');
    queue = await OfflineQueue.open(dir);
    expect((await new QueueReplayer(queue, adapter()).run()).errors).toEqual([]);
    expect(files.get('/v/note.md')?.data.toString()).toBe('new generation');
    expect(writes.map(w => w.expected)).toEqual([100, 0]);
  });

  it('a replay with no captured mtime cannot borrow the synthetic cache mtime', async () => {
    const a = adapter();
    a.setReconnecting(true);
    await a.write('new.md', 'new file');
    a.setReconnecting(false);
    expect((await new QueueReplayer(queue, a).run()).drained).toBe(1);
    expect(writes[0].expected).toBe(0);
  });
  it('a later offline snapshot cannot silently erase an earlier user merge', async () => {
    let decisions = 0;
    const decide = vi.fn(async (): Promise<TextConflictDecision> => ++decisions === 1
      ? { decision: 'merged', content: 'one + important external addition' }
      : { decision: 'cancel' });
    const a = adapter(decide);
    await saveOffline(a, 'one', 'two');
    external('important external addition');
    const report = await new QueueReplayer(queue, a).run();
    expect(report.drained).toBe(1);
    expect(decide).toHaveBeenCalledTimes(2);
    expect(files.get('/v/note.md')?.data.toString()).toBe('one + important external addition');
    expect((await OfflineQueue.open(dir)).pending()).toHaveLength(1);
  });

});
