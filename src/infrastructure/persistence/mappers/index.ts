import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../../domain/ledger/wallet-ledger-entry";
import { InboxMessage } from "../../../domain/messaging/inbox-message";
import { OutboxMessage } from "../../../domain/messaging/outbox-message";
import { Money } from "../../../domain/money/money";
import { FailureCode } from "../../../domain/wager/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../../domain/wager/wager-transaction";
import { Wallet } from "../../../domain/wallet/wallet";
import {
  InboxMessageRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../schemas";

export class WalletMapper {
  static toDomain(row: WalletRecord | Record<string, unknown>): Wallet {
    const r = row as Record<string, unknown>;
    const currency = String(r.currency).trim();
    return Wallet.rehydrate({
      id: String(r.id),
      playerId: String(r.playerId ?? r.player_id),
      currency,
      balance: Money.fromPersistence(String(r.balance), currency),
      version: parseInt(String(r.version), 10),
      createdAt: new Date(r.createdAt as string | Date ?? r.created_at as string | Date),
      updatedAt: new Date(r.updatedAt as string | Date ?? r.updated_at as string | Date),
    });
  }
}

export class WagerTransactionMapper {
  static toDomain(
    row: WagerTransactionRecord | Record<string, unknown>,
  ): WagerTransaction {
    const r = row as Record<string, unknown>;
    const currency = String(r.currency).trim();
    const refExt =
      r.referenceExternalTransactionId ?? r.reference_external_transaction_id;
    const refTxId = r.referenceTransactionId ?? r.reference_transaction_id;
    const failCode = r.failureCode ?? r.failure_code;
    const resBalance = r.resultingBalance ?? r.resulting_balance;
    const nextAttempt = r.nextAttemptAt ?? r.next_attempt_at;
    const corrId = r.correlationId ?? r.correlation_id;
    const procAt = r.processedAt ?? r.processed_at;

    return WagerTransaction.rehydrate({
      id: String(r.id),
      providerId: String(r.providerId ?? r.provider_id),
      externalTransactionId: String(
        r.externalTransactionId ?? r.external_transaction_id,
      ),
      idempotencyKey: String(r.idempotencyKey ?? r.idempotency_key),
      payloadHash: String(r.payloadHash ?? r.payload_hash).trim(),
      walletId: String(r.walletId ?? r.wallet_id),
      playerId: String(r.playerId ?? r.player_id),
      roundId: String(r.roundId ?? r.round_id),
      gameId: String(r.gameId ?? r.game_id),
      kind: String(r.kind) as WagerTransactionKind,
      money: Money.fromPersistence(String(r.amount), currency),
      referenceExternalTransactionId:
        refExt !== null && refExt !== undefined ? String(refExt) : undefined,
      referenceTransactionId:
        refTxId !== null && refTxId !== undefined ? String(refTxId) : undefined,
      status: String(r.status) as WagerTransactionStatus,
      failureCode:
        failCode !== null && failCode !== undefined
          ? (String(failCode) as FailureCode)
          : undefined,
      resultingBalance:
        resBalance !== null && resBalance !== undefined
          ? Money.fromPersistence(String(resBalance), currency)
          : undefined,
      attempts: parseInt(String(r.attempts ?? 0), 10),
      nextAttemptAt:
        nextAttempt !== null && nextAttempt !== undefined
          ? new Date(nextAttempt as string | Date)
          : undefined,
      correlationId:
        corrId !== null && corrId !== undefined ? String(corrId) : undefined,
      createdAt: new Date(
        (r.createdAt ?? r.created_at) as string | Date,
      ),
      updatedAt: new Date(
        (r.updatedAt ?? r.updated_at) as string | Date,
      ),
      processedAt:
        procAt !== null && procAt !== undefined
          ? new Date(procAt as string | Date)
          : undefined,
    });
  }
}

export class WalletLedgerEntryMapper {
  static toDomain(
    row: WalletLedgerEntryRecord | Record<string, unknown>,
  ): WalletLedgerEntry {
    const r = row as Record<string, unknown>;
    const currency = String(r.currency).trim();
    const seqVal = r.seq;
    return WalletLedgerEntry.rehydrate({
      id: String(r.id),
      seq:
        seqVal !== null && seqVal !== undefined
          ? BigInt(String(seqVal))
          : undefined,
      walletId: String(r.walletId ?? r.wallet_id),
      transactionId: String(r.transactionId ?? r.transaction_id),
      walletVersion: parseInt(
        String(r.walletVersion ?? r.wallet_version ?? 1),
        10,
      ),
      direction: String(r.direction) as LedgerDirection,
      money: Money.fromPersistence(String(r.amount), currency),
      balanceBefore: Money.fromPersistence(
        String(r.balanceBefore ?? r.balance_before),
        currency,
      ),
      balanceAfter: Money.fromPersistence(
        String(r.balanceAfter ?? r.balance_after),
        currency,
      ),
      createdAt: new Date((r.createdAt ?? r.created_at) as string | Date),
    });
  }
}

export class InboxMessageMapper {
  static toDomain(
    row: InboxMessageRecord | Record<string, unknown>,
  ): InboxMessage {
    const r = row as Record<string, unknown>;
    const procAt = r.processedAt ?? r.processed_at;
    return InboxMessage.rehydrate({
      consumerName: String(r.consumerName ?? r.consumer_name),
      messageId: String(r.messageId ?? r.message_id),
      payloadHash: String(r.payloadHash ?? r.payload_hash).trim(),
      receivedAt: new Date((r.receivedAt ?? r.received_at) as string | Date),
      processedAt:
        procAt !== null && procAt !== undefined
          ? new Date(procAt as string | Date)
          : undefined,
    });
  }
}

export class OutboxMessageMapper {
  static toDomain(
    row: OutboxMessageRecord | Record<string, unknown>,
  ): OutboxMessage {
    const r = row as Record<string, unknown>;
    const rawPayload = r.payload;
    const parsedPayload =
      typeof rawPayload === "string"
        ? (JSON.parse(rawPayload) as Record<string, unknown>)
        : ((rawPayload ?? {}) as Record<string, unknown>);
    const nextAttempt = r.nextAttemptAt ?? r.next_attempt_at;
    const lockedUntil = r.lockedUntil ?? r.locked_until;
    const lockedBy = r.lockedBy ?? r.locked_by;
    const lastError = r.lastError ?? r.last_error;
    const pubAt = r.publishedAt ?? r.published_at;
    const seqVal = r.seq;

    return OutboxMessage.rehydrate({
      id: String(r.id),
      seq:
        seqVal !== null && seqVal !== undefined
          ? BigInt(String(seqVal))
          : undefined,
      aggregateId: String(r.aggregateId ?? r.aggregate_id),
      eventType: String(r.eventType ?? r.event_type),
      payload: parsedPayload,
      occurredAt: new Date((r.occurredAt ?? r.occurred_at) as string | Date),
      attempts: parseInt(String(r.attempts ?? 0), 10),
      nextAttemptAt:
        nextAttempt !== null && nextAttempt !== undefined
          ? new Date(nextAttempt as string | Date)
          : undefined,
      lockedUntil:
        lockedUntil !== null && lockedUntil !== undefined
          ? new Date(lockedUntil as string | Date)
          : undefined,
      lockedBy:
        lockedBy !== null && lockedBy !== undefined
          ? String(lockedBy)
          : undefined,
      lastError:
        lastError !== null && lastError !== undefined
          ? String(lastError)
          : undefined,
      publishedAt:
        pubAt !== null && pubAt !== undefined
          ? new Date(pubAt as string | Date)
          : undefined,
    });
  }
}
