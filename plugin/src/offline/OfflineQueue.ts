import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { logger } from '../util/logger';
import { errorMessage } from "../util/errorMessage";

/**
 * Discriminated union of every adapter operation the queue can hold.
 * Mirrors `SftpDataAdapter`'s write-side surface — read-side ops are
 * intentionally NOT queueable (they'd lie about file presence to the
 * editor).
 *
 * Binary payloads are base64-encoded to keep the on-disk JSONL line-
 * delimited; the daemon's RPC schema already uses the same encoding,
 * so the queue's encoded form maps 1:1 onto a replayed `fs.write*`.
 */
export type QueuedOp =
  | { kind: 'write'; path: string; contentBase64: string; expectedMtime?: number; dependsOn?: number }
  | { kind: 'writeBinary'; path: string; contentBase64: string; expectedMtime?: number; dependsOn?: number }
  | { kind: 'append'; path: string; contentBase64: string }
  | { kind: 'appendBinary'; path: string; contentBase64: string }
  | { kind: 'mkdir'; path: string }
  | { kind: 'remove'; path: string }
  | { kind: 'rmdir'; path: string; recursive: boolean }
  | { kind: 'rename'; oldPath: string; newPath: string }
  | { kind: 'copy'; srcPath: string; dstPath: string }
  | { kind: 'trashLocal'; path: string };

export type QueuedWriteOp = Extract<QueuedOp, { kind: 'write' | 'writeBinary' }>;

export interface QueueEntry {
  /** Monotonically increasing within a queue's lifetime; survives restarts. */
  readonly id: number;
  /** Unix milliseconds when the op was enqueued. */
  readonly ts: number;
  readonly op: QueuedOp;
}

export interface QueueStats {
  /** Number of pending (un-completed) entries. */
  entries: number;
  /** Approx in-memory byte size of pending payloads (base64 included). */
  bytes: number;
  /** Configured cap. */
  maxBytes: number;
  /** Bytes the on-disk log currently occupies (pre-compaction overhead). */
  logFileBytes: number;
}

/**
 * Default cap on total in-memory payload bytes. 500 MB comfortably
 * holds a multi-day disconnect of typical text edits plus a few
 * attachments; small enough that a stuck queue doesn't quietly
 * consume the disk.
 */
export const DEFAULT_MAX_BYTES: number = 500 * 1024 * 1024;

/**
 * Append-only JSONL log of pending offline writes, persisted under
 * the plugin's queue directory so an Electron restart doesn't drop
 * the user's edits. The log holds three kinds of records:
 *
 *   {"type":"op","id":N,"ts":...,"op":{...}}     — enqueue
 *   {"type":"completed","id":N,"ts":...,"committedMtime":...} — replay/discard
 *   {"type":"watermark","nextId":N}           — ID continuity after compaction
 *
 * On `open()`, the log is replayed line-by-line; any `op` record
 * without a matching `completed` becomes a pending entry. When the
 * log file grows past 2× the live pending bytes, a compaction pass
 * rewrites it without the completed-and-tombstoned entries.
 *
 * Mutations within one owner are serialized, including appends and atomic
 * log replacements. Separate instances must still not share one directory.
 */
export class OfflineQueue {
  private readonly logFile: string;
  private readonly maxBytes: number;
  /** Pending entries in FIFO order. Mirror of the on-disk log minus completed ones. */
  private readonly pendingMap = new Map<number, QueueEntry>();
  private nextId = 1;
  /** Live byte total of pendingMap payloads (approximate). */
  private bytesUsed = 0;
  /** Bytes currently on disk in the log file (incl. tombstones, pre-compaction). */
  private logFileBytes = 0;
  private mutations: Promise<void> = Promise.resolve();

  private constructor(dir: string, maxBytes: number) {
    this.logFile = path.join(dir, 'log.jsonl');
    this.maxBytes = maxBytes;
  }

  /**
   * Open (or initialise) a queue rooted at `dir`. Replays the
   * existing log to seed the pending set.
   */
  static async open(dir: string, opts: { maxBytes?: number } = {}): Promise<OfflineQueue> {
    const maxBytes = opts.maxBytes ?? DEFAULT_MAX_BYTES;
    await fs.mkdir(dir, { recursive: true });
    const q = new OfflineQueue(dir, maxBytes);
    await q.replayLog();
    return q;
  }

  /**
   * Append `op` to the queue. Returns the assigned id. Throws if
   * adding `op`'s payload would push the in-memory byte total over
   * the configured cap (so the user gets a loud failure rather than
   * a silent disk fill).
   */
  enqueue(op: QueuedOp): Promise<number> {
    const snapshot = { ...op };
    return this.mutate(() => this.appendOp(snapshot));
  }

  /**
   * Queue a full-file save against the same remote version as its predecessor.
   * Local cache mtimes can be synthetic; only a replay's acknowledged remote
   * mtime may advance a dependent write's precondition.
   */
  enqueueWrite(op: QueuedWriteOp): Promise<number> {
    const snapshot = { ...op };
    return this.mutate(() => this.appendOp(this.linkWrite(snapshot)));
  }

  private async appendOp(op: QueuedOp): Promise<number> {
    const entry: QueueEntry = { id: this.nextId, ts: Date.now(), op };
    const line = serialiseOp(entry);
    const lineBytes = byteLengthUtf8(line);
    if (this.bytesUsed + lineBytes > this.maxBytes) {
      throw new Error(
        `OfflineQueue: enqueue would exceed cap (${this.bytesUsed + lineBytes} > ${this.maxBytes} bytes)`,
      );
    }
    await fs.appendFile(this.logFile, line + '\n', 'utf8');
    this.nextId++;
    this.pendingMap.set(entry.id, entry);
    this.bytesUsed += lineBytes;
    this.logFileBytes += lineBytes + 1; // +1 for newline
    return entry.id;
  }

  /**
   * FIFO list of un-completed entries. The replayer iterates this
   * to drain the queue against the recovered remote.
   */
  pending(): QueueEntry[] {
    return Array.from(this.pendingMap.values()).sort((a, b) => a.id - b.id);
  }

  /**
   * Mark an op as completed (replay succeeded, or user discarded it).
   * Idempotent: marking an unknown id is a no-op. Triggers
   * compaction when the on-disk log has grown past 2× the live
   * pending bytes.
   */
  markCompleted(id: number, committedMtime?: number): Promise<void> {
    return this.mutate(async () => {
      if (!this.pendingMap.has(id)) return;
      const tombstone = JSON.stringify({ type: 'completed', id, ts: Date.now(), committedMtime });
      // Completion and dependent preconditions are one record: a restart must
      // never observe the first write gone but its successor using a fake mtime.
      await fs.appendFile(this.logFile, tombstone + '\n', 'utf8');
      this.logFileBytes += byteLengthUtf8(tombstone) + 1;
      this.applyCompleted(id, committedMtime);

      if (this.shouldCompact()) await this.rewriteLog(this.pending());
    });
  }

  /** Drop everything. Atomic: a fresh log replaces the old one. */
  clear(): Promise<void> {
    return this.mutate(async () => {
      await this.rewriteLog([]);
      this.pendingMap.clear();
      this.bytesUsed = 0;
    });
  }

  /**
   * Manually trigger a compaction pass. Useful in tests; production
   * code calls this implicitly via `markCompleted`.
   */
  compact(): Promise<void> {
    return this.mutate(() => this.rewriteLog(this.pending()));
  }

  stats(): QueueStats {
    return {
      entries: this.pendingMap.size,
      bytes: this.bytesUsed,
      maxBytes: this.maxBytes,
      logFileBytes: this.logFileBytes,
    };
  }

  // ─── internals ─────────────────────────────────────────────────────────

  private mutate<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutations.then(operation);
    // A failed disk operation must surface to its caller without poisoning
    // subsequent saves or preventing the user from retrying.
    this.mutations = result.then(() => undefined, () => undefined);
    return result;
  }

  private linkWrite(op: QueuedWriteOp): QueuedWriteOp {
    const linked = { ...op, expectedMtime: op.expectedMtime ?? 0 };
    delete linked.dependsOn;
    let lookupPath = op.path;
    for (const entry of this.pending().reverse()) {
      const prior = entry.op;
      if (isWrite(prior) && prior.path === lookupPath) {
        linked.dependsOn = entry.id;
        linked.expectedMtime = prior.expectedMtime ?? 0;
        return linked;
      }
      if (prior.kind === 'rename') {
        if (containsPath(prior.newPath, lookupPath)) {
          lookupPath = prior.oldPath + lookupPath.slice(prior.newPath.length);
        } else if (containsPath(prior.oldPath, lookupPath)) {
          return { ...linked, expectedMtime: 0 };
        }
      } else if (
        (prior.kind === 'remove' || prior.kind === 'rmdir' || prior.kind === 'trashLocal')
        && containsPath(prior.path, lookupPath)
      ) {
        return { ...linked, expectedMtime: 0 };
      } else if (
        ((prior.kind === 'append' || prior.kind === 'appendBinary') && prior.path === lookupPath)
        || (prior.kind === 'copy' && containsPath(prior.dstPath, lookupPath))
      ) {
        // These operations do not return a tracked version. Keeping the old
        // precondition can prompt a conflict, but cannot waive one silently.
        return linked;
      }
    }
    return linked;
  }

  private applyCompleted(id: number, committedMtime?: number): void {
    const dropped = this.pendingMap.get(id);
    if (dropped) {
      this.pendingMap.delete(id);
      this.bytesUsed -= byteLengthUtf8(serialiseOp(dropped));
    }
    for (const entry of this.pendingMap.values()) {
      if (!isWrite(entry.op) || entry.op.dependsOn !== id) continue;
      const op = { ...entry.op };
      delete op.dependsOn;
      // No mtime means explicit discard, not a successful write. Retain the
      // original precondition so the next local edit needs its own decision.
      if (committedMtime !== undefined) op.expectedMtime = committedMtime;
      const updated = { ...entry, op };
      this.pendingMap.set(entry.id, updated);
      this.bytesUsed += byteLengthUtf8(serialiseOp(updated)) - byteLengthUtf8(serialiseOp(entry));
    }
  }

  private async replayLog(): Promise<void> {
    let raw: string;
    try {
      raw = await fs.readFile(this.logFile, 'utf8');
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code === 'ENOENT') {
        this.logFileBytes = 0;
        return;
      }
      throw e;
    }
    this.logFileBytes = byteLengthUtf8(raw);
    if (raw.length === 0) return;

    let lineNo = 0;
    for (const rawLine of raw.split('\n')) {
      lineNo++;
      const line = rawLine.trim();
      if (line === '') continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line);
      } catch (e) {
        logger.warn(
          `OfflineQueue.replay: ${this.logFile}:${lineNo} malformed JSON; skipping (${errorMessage(e)})`,
        );
        continue;
      }
      if (!isLogRecord(parsed)) {
        logger.warn(`OfflineQueue.replay: ${this.logFile}:${lineNo} unknown record shape; skipping`);
        continue;
      }
      if (parsed.type === 'op') {
        const entry: QueueEntry = { id: parsed.id, ts: parsed.ts, op: parsed.op };
        this.pendingMap.set(entry.id, entry);
        this.bytesUsed += byteLengthUtf8(serialiseOp(entry));
        if (entry.id >= this.nextId) this.nextId = entry.id + 1;
      } else if (parsed.type === 'completed') {
        this.applyCompleted(parsed.id, parsed.committedMtime);
        this.nextId = Math.max(this.nextId, parsed.id + 1);
      } else {
        this.nextId = Math.max(this.nextId, parsed.nextId);
      }
    }
  }

  /**
   * Compact iff the on-disk log is more than 2× the live pending
   * bytes — a small queue with lots of completions thrashes the
   * file otherwise.
   */
  private shouldCompact(): boolean {
    // Always compact at least 4 KiB worth of slack so a tiny queue
    // doesn't pay the rewrite cost for individual tombstones.
    const slackFloor = 4 * 1024;
    return this.logFileBytes > Math.max(slackFloor, this.bytesUsed * 2);
  }

  /**
   * Atomically rewrite the log to contain only `entries` (in order)
   * and no tombstones. Tmp + rename so a crash mid-write never
   * orphans a half-written log.
   */
  private async rewriteLog(entries: QueueEntry[]): Promise<void> {
    const tmpPath = this.logFile + '.tmp';
    const lines = [JSON.stringify({ type: 'watermark', nextId: this.nextId }), ...entries.map(e => serialiseOp(e))];
    const body = lines.join('\n') + '\n';
    await fs.writeFile(tmpPath, body, 'utf8');
    // Fail with the old log intact if atomic replacement is unavailable;
    // copying over it could destroy the only durable copy on a failed write.
    await fs.rename(tmpPath, this.logFile);
    this.logFileBytes = byteLengthUtf8(body);
  }
}

// ─── (de)serialisation ───────────────────────────────────────────────────

interface OpRecord { type: 'op'; id: number; ts: number; op: QueuedOp }
interface TombstoneRecord { type: 'completed'; id: number; ts: number; committedMtime?: number }
interface WatermarkRecord { type: 'watermark'; nextId: number }
type LogRecord = OpRecord | TombstoneRecord | WatermarkRecord;

function serialiseOp(entry: QueueEntry): string {
  return JSON.stringify({ type: 'op', id: entry.id, ts: entry.ts, op: entry.op });
}
function isLogRecord(v: unknown): v is LogRecord {
  if (!v || typeof v !== 'object') return false;
  const r = v as Partial<LogRecord>;
  if (r.type === 'op') {
    return typeof r.id === 'number'
      && typeof r.ts === 'number'
      && r.op !== undefined
      && typeof (r.op as { kind?: unknown }).kind === 'string';
  }
  if (r.type === 'completed') {
    return typeof r.id === 'number' && typeof r.ts === 'number'
      && (r.committedMtime === undefined || (typeof r.committedMtime === 'number' && Number.isFinite(r.committedMtime)));
  }
  if (r.type === 'watermark') return Number.isSafeInteger(r.nextId) && r.nextId! > 0;
  return false;
}

function isWrite(op: QueuedOp): op is QueuedWriteOp {
  return op.kind === 'write' || op.kind === 'writeBinary';
}

function containsPath(parent: string, child: string): boolean {
  return parent === child || child.startsWith(parent + '/');
}

/**
 * UTF-8 byte length without depending on `Buffer` so this module
 * stays portable to environments that prefer the Web stdlib.
 */
function byteLengthUtf8(s: string): number {
  // Browser/Node both ship TextEncoder.
  if (typeof TextEncoder !== 'undefined') {
    return new TextEncoder().encode(s).byteLength;
  }
  // Fallback: assume worst case (4 bytes/char), used only in
  // hypothetical environments without TextEncoder.
  return s.length * 4;
}
