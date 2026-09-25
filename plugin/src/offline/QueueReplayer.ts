import type { OfflineQueue, QueuedOp } from './OfflineQueue';
import { logger } from '../util/logger';
import { errorMessage } from "../util/errorMessage";

/**
 * Slim adapter surface the replayer drives. Matches
 * `SftpDataAdapter.replayQueuedOp` so a fake adapter (test fixture)
 * can satisfy the same contract.
 */
export type ReplayOutcome =
  | { result: 'ok'; mtime?: number }
  | { result: 'discarded' }
  | { result: 'cancelled' }
  | { result: 'error'; message: string };

export interface ReplayTarget {
  replayQueuedOp(op: QueuedOp): Promise<ReplayOutcome>;
}

export interface ReplayReport {
  /** Ops that landed cleanly (or whose conflict modal returned a definitive resolution). */
  drained: number;
  /** Ops explicitly discarded by picking `keep-theirs`. Cancellation never completes an entry. */
  conflicts: number;
  /** Ops that errored; queue entry stays pending for the next reconnect to retry. */
  errors: Array<{ id: number; message: string }>;
}

/**
 * Drains an `OfflineQueue` against a connected adapter once an SSH
 * session has recovered. One-shot: instantiate, call `run()`, throw
 * away.
 *
 * Behaviour:
 *
 * - Replays oldest-first so a chain of edits to the same file lands
 *   in the order the user typed them.
 * - Only a committed write (`ok`) or explicit discard (`discarded`)
 *   completes an entry. Cancellation stops the drain without removing it.
 * - On a transport error (`error`), logs and STOPS so the next
 *   reconnect retries from the same entry. We could choose to
 *   continue with later entries, but the user's mental model is
 *   "edits land in order" and skipping risks a write on a path that
 *   depends on an earlier rename / mkdir.
 */
export class QueueReplayer {
  private static readonly running = new WeakMap<OfflineQueue, Promise<ReplayReport>>();

  constructor(
    private readonly queue: OfflineQueue,
    private readonly target: ReplayTarget,
  ) {}

  run(): Promise<ReplayReport> {
    const running = QueueReplayer.running.get(this.queue);
    if (running) return running;
    const drain = this.drain().finally(() => { QueueReplayer.running.delete(this.queue); });
    QueueReplayer.running.set(this.queue, drain);
    return drain;
  }

  private async drain(): Promise<ReplayReport> {
    const report: ReplayReport = { drained: 0, conflicts: 0, errors: [] };
    const initialPending = this.queue.pending();
    if (initialPending.length === 0) return report;

    logger.info(`QueueReplayer: draining ${initialPending.length} pending entries`);

    for (;;) {
      // Re-read after each completion: versions may have advanced, and a save
      // made while a conflict modal was open belongs at the tail of this run.
      const entry = this.queue.pending()[0];
      if (!entry) break;
      let outcome: ReplayOutcome;
      try {
        outcome = await this.target.replayQueuedOp(entry.op);
      } catch (e) {
        // The target should never throw — `replayQueuedOp` returns
        // a discriminated outcome — but if a fake's behaviour
        // diverges, surface it as an error rather than crashing the
        // whole drain.
        const message = errorMessage(e);
        report.errors.push({ id: entry.id, message });
        logger.error(`QueueReplayer: target threw on entry #${entry.id} (${entry.op.kind}): ${message}`);
        break;
      }

      if (outcome.result === 'ok') {
        await this.queue.markCompleted(entry.id, outcome.mtime);
        report.drained++;
        continue;
      }

      if (outcome.result === 'discarded') {
        // Only an explicit decision to abandon this edit grants permission
        // to remove it. Later local edits retain their own preconditions.
        await this.queue.markCompleted(entry.id);
        report.conflicts++;
        continue;
      }

      if (outcome.result === 'cancelled') break;

      // outcome.result === 'error'
      report.errors.push({ id: entry.id, message: outcome.message });
      logger.warn(
        `QueueReplayer: entry #${entry.id} (${entry.op.kind}) failed: ${outcome.message}; ` +
        'leaving it queued for the next reconnect',
      );
      break;
    }

    logger.info(
      `QueueReplayer: drain complete — drained=${report.drained}, conflicts=${report.conflicts}, ` +
      `errors=${report.errors.length}, remaining=${this.queue.pending().length}`,
    );
    return report;
  }
}
