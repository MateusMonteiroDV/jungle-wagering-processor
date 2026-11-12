import { WagerTransactionStatus } from "../../domain/wager/wager-transaction";
import type { ClockPort, UnitOfWork } from "../ports";
import { ProcessWagerTransactionUseCase } from "./process-wager-transaction";

export interface ResolvePendingReferencesOptions {
  batchSize: number;
  maxAttempts: number;
  ttlMs: number;
  baseDelayMs: number;
  claimLeaseMs?: number;
}

export class ResolvePendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly processWager: ProcessWagerTransactionUseCase,
    private readonly clock: ClockPort,
    private readonly options: ResolvePendingReferencesOptions,
  ) {}

  async executeBatch(): Promise<{ claimed: number; resolved: number; rejected: number }> {
    const now = this.clock.now();
    const leaseMs = this.options.claimLeaseMs ?? 30000;
    const leaseUntil = new Date(now.getTime() + leaseMs);

    const claimed = await this.uow.runInTransaction((tx) =>
      tx.transactions.claimDuePendingReferences(
        now,
        this.options.batchSize,
        leaseUntil,
      ),
    );

    let resolved = 0;
    let rejected = 0;

    for (const item of claimed) {
      const pendingTx = await this.uow.runInTransaction((tx) =>
        tx.transactions.findById(item.id),
      );
      if (
        !pendingTx ||
        pendingTx.status !== WagerTransactionStatus.PendingReference
      ) {
        continue;
      }

      const result = await this.processWager.execute(
        {
          providerId: pendingTx.providerId,
          externalTransactionId: pendingTx.externalTransactionId,
          playerId: pendingTx.playerId,
          walletId: pendingTx.walletId,
          roundId: pendingTx.roundId,
          gameId: pendingTx.gameId,
          kind: pendingTx.kind,
          money: pendingTx.money.toJSON(),
          referenceExternalTransactionId:
            pendingTx.referenceExternalTransactionId,
        },
        {
          source: "WORKER",
          idempotencyKey: pendingTx.idempotencyKey,
          correlationId: pendingTx.correlationId ?? pendingTx.id,
          pendingConfig: {
            maxAttempts: this.options.maxAttempts,
            ttlMs: this.options.ttlMs,
            baseDelayMs: this.options.baseDelayMs,
          },
        },
      );

      if (result.status === WagerTransactionStatus.Processed) {
        resolved += 1;
      } else if (result.status === WagerTransactionStatus.Rejected) {
        rejected += 1;
      }
    }

    return {
      claimed: claimed.length,
      resolved,
      rejected,
    };
  }
}
