import { LockMode } from "@mikro-orm/core";
import { SqlEntityManager } from "@mikro-orm/postgresql";
import { WagerTransactionRepository } from "../../../application/ports";
import {
  WagerTransaction,
  WagerTransactionStatus,
} from "../../../domain/wager/wager-transaction";
import { WagerTransactionMapper } from "../mappers";
import { WagerTransactionRecord } from "../schemas";

export class PgWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    const rec = await this.em.findOne(
      WagerTransactionRecord,
      { id },
      { disableIdentityMap: true },
    );
    return rec ? WagerTransactionMapper.toDomain(rec) : null;
  }

  async findByIdForUpdate(id: string): Promise<WagerTransaction | null> {
    const rec = await this.em.findOne(
      WagerTransactionRecord,
      { id },
      {
        lockMode: LockMode.PESSIMISTIC_WRITE,
        disableIdentityMap: true,
      },
    );
    return rec ? WagerTransactionMapper.toDomain(rec) : null;
  }

  async findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<WagerTransaction | null> {
    const rec = await this.em.findOne(
      WagerTransactionRecord,
      { idempotencyKey },
      { disableIdentityMap: true },
    );
    return rec ? WagerTransactionMapper.toDomain(rec) : null;
  }

  async findByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null> {
    const rec = await this.em.findOne(
      WagerTransactionRecord,
      { providerId, externalTransactionId },
      { disableIdentityMap: true },
    );
    return rec ? WagerTransactionMapper.toDomain(rec) : null;
  }

  async findProcessedReversalForReference(
    referenceTransactionId: string,
  ): Promise<WagerTransaction | null> {
    const rows = await this.em.execute(
      `SELECT * FROM wager_transactions
       WHERE reference_transaction_id = ?
         AND kind IN ('REFUND', 'ROLLBACK')
         AND status = 'PROCESSED'
       LIMIT 1`,
      [referenceTransactionId],
    );
    const first = rows[0];
    return first ? WagerTransactionMapper.toDomain(first) : null;
  }

  async findPendingByReferenceExternalId(
    walletId: string,
    providerId: string,
    referenceExternalTransactionId: string,
  ): Promise<WagerTransaction[]> {
    const rows = await this.em.execute(
      `SELECT * FROM wager_transactions
       WHERE wallet_id = ?
         AND provider_id = ?
         AND reference_external_transaction_id = ?
         AND status = ?
       ORDER BY created_at ASC`,
      [
        walletId,
        providerId,
        referenceExternalTransactionId,
        WagerTransactionStatus.PendingReference,
      ],
    );
    return rows.map((r) => WagerTransactionMapper.toDomain(r));
  }

  async claimDuePendingReferences(
    now: Date,
    limit: number,
    leaseUntil: Date,
  ): Promise<Array<{ id: string; walletId: string }>> {
    const rows = await this.em.execute(
      `UPDATE wager_transactions
       SET next_attempt_at = ?, updated_at = ?
       WHERE id IN (
         SELECT id FROM wager_transactions
         WHERE status = 'PENDING_REFERENCE'
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
         ORDER BY next_attempt_at ASC NULLS FIRST, created_at ASC
         LIMIT ?
         FOR UPDATE SKIP LOCKED
       )
       RETURNING id, wallet_id`,
      [
        leaseUntil.toISOString(),
        now.toISOString(),
        now.toISOString(),
        limit,
      ],
    );
    return rows.map((r: Record<string, unknown>) => ({
      id: String(r.id),
      walletId: String(r.wallet_id),
    }));
  }

  async insert(tx: WagerTransaction): Promise<void> {
    await this.em.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_external_transaction_id, reference_transaction_id,
        status, failure_code, resulting_balance, attempts, next_attempt_at,
        correlation_id, created_at, updated_at, processed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tx.id,
        tx.providerId,
        tx.externalTransactionId,
        tx.idempotencyKey,
        tx.payloadHash,
        tx.walletId,
        tx.playerId,
        tx.roundId,
        tx.gameId,
        tx.kind,
        tx.money.amount,
        tx.money.currency,
        tx.referenceExternalTransactionId ?? null,
        tx.referenceTransactionId ?? null,
        tx.status,
        tx.failureCode ?? null,
        tx.resultingBalance ? tx.resultingBalance.amount : null,
        tx.attempts,
        tx.nextAttemptAt ? tx.nextAttemptAt.toISOString() : null,
        tx.correlationId ?? null,
        tx.createdAt.toISOString(),
        tx.updatedAt.toISOString(),
        tx.processedAt ? tx.processedAt.toISOString() : null,
      ],
    );
  }

  async update(tx: WagerTransaction): Promise<void> {
    await this.em.execute(
      `UPDATE wager_transactions
       SET reference_transaction_id = ?,
           status = ?,
           failure_code = ?,
           resulting_balance = ?,
           attempts = ?,
           next_attempt_at = ?,
           updated_at = ?,
           processed_at = ?
       WHERE id = ?`,
      [
        tx.referenceTransactionId ?? null,
        tx.status,
        tx.failureCode ?? null,
        tx.resultingBalance ? tx.resultingBalance.amount : null,
        tx.attempts,
        tx.nextAttemptAt ? tx.nextAttemptAt.toISOString() : null,
        tx.updatedAt.toISOString(),
        tx.processedAt ? tx.processedAt.toISOString() : null,
        tx.id,
      ],
    );
  }
}
