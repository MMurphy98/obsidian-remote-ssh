import { describe, expect, it, vi } from 'vitest';
import { SftpDataAdapter } from '../src/adapter/SftpDataAdapter';
import type { RemoteFsClient } from '../src/adapter/RemoteFsClient';
import { ReadCache } from '../src/cache/ReadCache';
import { DirCache } from '../src/cache/DirCache';

describe('adapter read preconditions', () => {
  it('does not pair earlier read bytes with a concurrent edit\'s later mtime', async () => {
    let remote = { data: Buffer.from('original'), mtime: 100 };
    const client: RemoteFsClient = {
      isAlive: () => true, onClose: () => () => {},
      exists: async () => true, list: async () => [],
      stat: vi.fn(async () => ({
        isFile: true, isDirectory: false, isSymbolicLink: false,
        size: remote.data.length, mtime: remote.mtime, mode: 0o644,
      })),
      readBinary: async () => {
        const data = remote.data;
        // Another writer commits after the bytes were read but before a
        // subsequent stat could observe the old file generation.
        remote = { data: Buffer.from('concurrent edit'), mtime: 200 };
        return data;
      },
      writeBinary: async (_path, data, expectedMtime) => {
        if (expectedMtime !== remote.mtime) throw Object.assign(new Error('precondition failed'), { code: -32020 });
        remote = { data, mtime: 300 };
        return remote.mtime;
      },
      mkdirp: async () => {}, remove: async () => {}, rmdir: async () => {}, rename: async () => {}, copy: async () => {},
    };
    const adapter = new SftpDataAdapter(client, '/test', new ReadCache(), new DirCache(), 'test');
    expect(await adapter.read('note.md')).toBe('original');
    await expect(adapter.write('note.md', 'edited original')).rejects.toMatchObject({ code: -32020 });
    expect(remote.data.toString()).toBe('concurrent edit');
  });
});
