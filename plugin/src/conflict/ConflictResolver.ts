import type { AncestorTracker } from './AncestorTracker';
import type { RemoteFsClient } from '../adapter/RemoteFsClient';
import type { ReadCache } from '../cache/ReadCache';
import { logger } from '../util/logger';
import { errorMessage } from '../util/errorMessage';

export interface ThreeWayPanes {
  ancestor: string;
  mine: string;
  theirs: string;
}

export type TextConflictDecision =
  | { decision: 'keep-mine' }
  | { decision: 'keep-theirs' }
  | { decision: 'merged'; content: string }
  | { decision: 'cancel' };

/** A decision to stop writing is distinct from a failed precondition. */
export class ConflictResolutionStopped extends Error {
  readonly code = -32020;
  readonly cause: unknown;
  constructor(readonly decision: 'cancelled' | 'discarded', originalError: unknown) {
    super(decision === 'cancelled'
      ? 'Write conflict cancelled; the local edit remains pending'
      : 'Local edit explicitly discarded in favour of the remote version');
    this.cause = originalError;
    this.name = 'ConflictResolutionStopped';
  }
}

export interface ResolvedWrite {
  data: Buffer;
  /** The version returned by the write itself, never a later stat. */
  mtime?: number;
}

export type WriteConflictCallback = (vaultPath: string) => Promise<boolean>;
export type TextConflictCallback = (
  vaultPath: string,
  panes: ThreeWayPanes,
) => Promise<TextConflictDecision>;

/**
 * Handles write-conflict resolution when the remote file changed since
 * the user's last read (PreconditionFailed).
 *
 * Two paths:
 *   1. **3-way merge** — when `onTextConflict` is wired AND an ancestor
 *      snapshot exists, the user sees (ancestor / mine / theirs) and can
 *      keep-mine, keep-theirs, hand-merge, or cancel.
 *   2. **Two-choice fallback** — binary writes, or text writes without
 *      an ancestor, surface a simple overwrite-or-cancel modal.
 */
export class ConflictResolver {
  private client: RemoteFsClient;

  constructor(
    client: RemoteFsClient,
    private readonly readCache: ReadCache,
    private readonly ancestorTracker: AncestorTracker | null,
    private readonly onTextConflict: TextConflictCallback | null,
    private readonly onWriteConflict: WriteConflictCallback | null,
  ) {
    this.client = client;
  }

  swapClient(newClient: RemoteFsClient): void {
    this.client = newClient;
  }

  /**
   * Run the conflict-resolution stack (text 3-way → legacy two-choice
   * → rethrow). Returns the bytes and version actually committed.
   * Cancellation and explicit discard have distinct typed outcomes;
   * an unavailable callback or failed write never implies user consent.
   */
  async resolve(
    normalizedPath: string,
    remote: string,
    mine: Buffer,
    isText: boolean,
    originalError: unknown,
  ): Promise<ResolvedWrite> {
    if (isText && this.ancestorTracker && this.onTextConflict) {
      const ancestor = this.ancestorTracker.get(normalizedPath);
      if (ancestor !== null) {
        let theirsBuf: Buffer;
        // Sample before reading, never after the modal: a later stat could
        // bless old displayed bytes with a version the user never saw.
        let theirsMtime = 0;
        try { theirsMtime = (await this.client.stat(remote)).mtime; }
        catch { /* Unknown version stays conservative (create-only). */ }
        try {
          theirsBuf = await this.client.readBinary(remote);
        } catch (re) {
          logger.warn(
            `ConflictResolver: re-read of "${remote}" failed (${errorMessage(re)}); ` +
            'falling back to the two-choice modal',
          );
          return await this.fallbackTwoChoice(normalizedPath, remote, mine, originalError);
        }
        const decision = await this.onTextConflict(normalizedPath, {
          ancestor: ancestor.content,
          mine:     mine.toString('utf8'),
          theirs:   theirsBuf.toString('utf8'),
        }).catch(() => ({ decision: 'cancel' as const }));

        switch (decision.decision) {
          case 'keep-mine': {
            const mtime = await this.client.writeBinary(remote, mine);
            return { data: mine, mtime: mtime ?? undefined };
          }
          case 'merged': {
            const merged = Buffer.from(decision.content, 'utf8');
            const mtime = await this.client.writeBinary(remote, merged);
            return { data: merged, mtime: mtime ?? undefined };
          }
          case 'keep-theirs': {
            this.readCache.put(remote, theirsBuf, theirsMtime);
            if (this.ancestorTracker) {
              this.ancestorTracker.remember(normalizedPath, theirsBuf.toString('utf8'), theirsMtime);
            }
            throw new ConflictResolutionStopped('discarded', originalError);
          }
          case 'cancel':
            throw new ConflictResolutionStopped('cancelled', originalError);
        }
      }
    }
    return await this.fallbackTwoChoice(normalizedPath, remote, mine, originalError);
  }

  /**
   * Two-choice (overwrite / cancel) conflict path — the binary
   * fallback and the no-ancestor fallback for text. Throws on
   * cancel; returns the committed bytes and version on overwrite.
   */
  private async fallbackTwoChoice(
    normalizedPath: string,
    remote: string,
    mine: Buffer,
    originalError: unknown,
  ): Promise<ResolvedWrite> {
    if (!this.onWriteConflict) {
      throw new Error('Write conflict cannot be resolved: no conflict handler is available; the local edit remains pending');
    }
    const overwrite = await this.onWriteConflict(normalizedPath).catch(() => false);
    if (!overwrite) throw new ConflictResolutionStopped('cancelled', originalError);
    const mtime = await this.client.writeBinary(remote, mine);
    return { data: mine, mtime: mtime ?? undefined };
  }
}
