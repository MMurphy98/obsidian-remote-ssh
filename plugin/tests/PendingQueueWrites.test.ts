import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import { SftpDataAdapter } from '../src/adapter/SftpDataAdapter';
import { ReadCache } from '../src/cache/ReadCache';
import { DirCache } from '../src/cache/DirCache';
import { OfflineQueue, type QueuedOp } from '../src/offline/OfflineQueue';
import { QueueReplayer } from '../src/offline/QueueReplayer';

describe('adapter preserves pending edits while connected', () => {
  let root: string;
  let queue: OfflineQueue;
  let adapter: SftpDataAdapter;
  let client: ReturnType<typeof makeClient>;
  let reflected: ReturnType<typeof makeReflector>;

  function makeClient() {
    return {
      isAlive: () => true, onClose: () => () => {},
      readBinary: vi.fn(async () => Buffer.from('remote version')),
      stat: vi.fn(async () => ({ isFile: true, isDirectory: false, isSymbolicLink: false, size: 14, mtime: 7, mode: 0o644 })),
      exists: vi.fn(async () => true), list: vi.fn(async () => []),
      writeBinary: vi.fn(async () => 8), mkdirp: vi.fn(async () => {}), remove: vi.fn(async () => {}),
      rmdir: vi.fn(async () => {}), rename: vi.fn(async () => {}), copy: vi.fn(async () => {}),
    };
  }
  function makeReflector() {
    return { reflectWrite: vi.fn(), reflectRename: vi.fn(), reflectRemove: vi.fn(), reflectMkdir: vi.fn() };
  }
  function setupAdapter() {
    adapter = new SftpDataAdapter(client, '/test', new ReadCache(), new DirCache(), 'test', null, null, null, null, queue);
    adapter.setWriterReflector(reflected);
  }
  async function save(name = 'note.md', content = 'pending content') {
    await queue.enqueueWrite({ kind: 'write', path: name, contentBase64: Buffer.from(content).toString('base64'), expectedMtime: 7 });
  }
  function latestContent(): string {
    const op = queue.pending().at(-1)!.op as { contentBase64: string };
    return Buffer.from(op.contentBase64, 'base64').toString();
  }

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), 'remote-ssh-pending-'));
    queue = await OfflineQueue.open(root);
    client = makeClient();
    reflected = makeReflector();
    setupAdapter();
  });
  afterEach(async () => { await fs.rm(root, { recursive: true, force: true }); });

  it.each([
    ['write', (a: SftpDataAdapter) => a.write('next.md', 'new save')],
    ['writeBinary', (a: SftpDataAdapter) => a.writeBinary('next.bin', new Uint8Array([3]).buffer)],
    ['mkdir', (a: SftpDataAdapter) => a.mkdir('folder')],
    ['remove', (a: SftpDataAdapter) => a.remove('note.md')],
    ['rmdir', (a: SftpDataAdapter) => a.rmdir('folder', true)],
    ['rename', (a: SftpDataAdapter) => a.rename('note.md', 'moved.md')],
    ['copy', (a: SftpDataAdapter) => a.copy('note.md', 'copy.md')],
    ['rename', (a: SftpDataAdapter) => a.trashLocal('note.md')],
  ] as const)('queues %s until earlier pending edits finish', async (kind, mutate) => {
    await save();
    await mutate(adapter);
    expect(queue.pending()).toHaveLength(2);
    expect(queue.pending()[1].op.kind).toBe(kind);
    for (const fn of [client.writeBinary, client.mkdirp, client.remove, client.rmdir, client.rename, client.copy]) expect(fn).not.toHaveBeenCalled();
    for (const fn of Object.values(reflected)) expect(fn).not.toHaveBeenCalled();
  });

  it.each(['append', 'appendBinary', 'process'] as const)('%s uses persisted local content after restart', async (operation) => {
    await save();
    queue = await OfflineQueue.open(root);
    setupAdapter();
    if (operation === 'append') await adapter.append('note.md', ' + local');
    else if (operation === 'appendBinary') await adapter.appendBinary('note.md', new TextEncoder().encode(' + local').buffer);
    else await adapter.process('note.md', (text) => text + ' + local');
    expect(latestContent()).toBe('pending content + local');
    expect(client.readBinary).not.toHaveBeenCalled();
    expect(client.writeBinary).not.toHaveBeenCalled();
  });

  it('appends to a queued note after a directory rename', async () => {
    await save('folder/note.md');
    await queue.enqueue({ kind: 'rename', oldPath: 'folder', newPath: 'renamed' });
    await adapter.append('renamed/note.md', ' + after rename');
    expect(latestContent()).toBe('pending content + after rename');
    expect(client.readBinary).not.toHaveBeenCalled();
  });

  it('starts an append empty only when a queued deletion proves the path is absent', async () => {
    await save();
    await queue.enqueue({ kind: 'remove', path: 'note.md' });
    await adapter.append('note.md', 'recreated');
    expect(latestContent()).toBe('recreated');
    expect(client.readBinary).not.toHaveBeenCalled();
  });

  it('refuses an append when the pending source bytes cannot be determined', async () => {
    await save('other.md');
    await expect(adapter.append('uncached.md', 'suffix')).rejects.toThrow(/pending|safely|baseline/i);
    expect(queue.pending()).toHaveLength(1);
    expect(client.writeBinary).not.toHaveBeenCalled();
  });

  it('replays queued mutations directly and reflects them only after success', async () => {
    const mutations: QueuedOp[] = [
      { kind: 'mkdir', path: 'folder' },
      { kind: 'rename', oldPath: 'note.md', newPath: 'moved.md' },
      { kind: 'copy', srcPath: 'moved.md', dstPath: 'copy.md' },
      { kind: 'remove', path: 'copy.md' },
      { kind: 'rmdir', path: 'folder', recursive: true },
      { kind: 'trashLocal', path: 'moved.md' },
    ];
    for (const op of mutations) await queue.enqueue(op);
    const report = await new QueueReplayer(queue, adapter).run();
    expect(report.errors).toEqual([]);
    expect(queue.pending()).toEqual([]);
    expect(client.rename).toHaveBeenCalledTimes(2);
    expect(reflected.reflectRename).toHaveBeenCalledTimes(2);
    expect(reflected.reflectRemove).toHaveBeenCalledTimes(2);
    expect(reflected.reflectMkdir).toHaveBeenCalledTimes(1);
  });
});
