import { WalletLedgerEntry } from "../../domain/ledger/wallet-ledger-entry";
import { InboxMessage } from "../../domain/messaging/inbox-message";
import { OutboxMessage } from "../../domain/messaging/outbox-message";
import { Money } from "../../domain/money/money";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/wager/wager-transaction";
import { Wallet } from "../../domain/wallet/wallet";

export interface WalletRepository {
  findById(id: string): Promise<Wallet | null>;
  findByIdForUpdate(id: string): Promise<Wallet | null>;
  findByIdForShare(id: string): Promise<Wallet | null>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | null>;
  insert(wallet: Wallet): Promise<void>;
  updateWithVersion(wallet: Wallet): Promise<void>;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | null>;
  findByIdForUpdate(id: string): Promise<WagerTransaction | null>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | null>;
  findByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null>;
  findProcessedReversalForReference(
    referenceTransactionId: string,
  ): Promise<WagerTransaction | null>;
  findPendingByReferenceExternalId(
    walletId: string,
    providerId: string,
    referenceExternalTransactionId: string,
  ): Promise<WagerTransaction[]>;
  claimDuePendingReferences(
    now: Date,
    limit: number,
    leaseUntil: Date,
  ): Promise<Array<{ id: string; walletId: string }>>;
  insert(tx: WagerTransaction): Promise<void>;
  update(tx: WagerTransaction): Promise<void>;
}

export interface WalletLedgerSummary {
  calculatedBalance: Money;
  checkedEntries: number;
  lastBalanceAfter?: Money;
  maxWalletVersion: number;
}

export interface PaginatedLedgerResult {
  items: WalletLedgerEntry[];
  nextCursor?: string;
}

export interface LedgerRepository {
  insert(entry: WalletLedgerEntry): Promise<void>;
  listByWallet(
    walletId: string,
    afterSeq: bigint | undefined,
    limit: number,
  ): Promise<PaginatedLedgerResult>;
  calculateWalletSummary(
    walletId: string,
    currency: string,
  ): Promise<WalletLedgerSummary>;
}

export interface InboxTryInsertResult {
  inserted: boolean;
  existing?: InboxMessage;
}

export interface InboxRepository {
  tryInsert(message: InboxMessage): Promise<InboxTryInsertResult>;
  markProcessed(
    consumerName: string,
    messageId: string,
    processedAt: Date,
  ): Promise<void>;
}

export interface OutboxRepository {
  insert(message: OutboxMessage): Promise<void>;
  insertMany(messages: OutboxMessage[]): Promise<void>;
  leaseDueMessages(
    now: Date,
    limit: number,
    instanceId: string,
    leaseDurationSec: number,
  ): Promise<OutboxMessage[]>;
  markPublished(id: string, publishedAt: Date): Promise<void>;
  scheduleRetry(message: OutboxMessage): Promise<void>;
  getOldestUnpublishedOccurredAt(): Promise<Date | null>;
}

export interface TransactionalContext {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  inbox: InboxRepository;
  outbox: OutboxRepository;
}

export interface UnitOfWork {
  runInTransaction<T>(work: (ctx: TransactionalContext) => Promise<T>): Promise<T>;
}

export interface ClockPort {
  now(): Date;
}

export interface IdGeneratorPort {
  generate(): string;
}

export type ProcessingSource = "HTTP" | "SQS" | "WORKER";

export interface MetricsPort {
  incrementTransaction(
    status: WagerTransactionStatus,
    kind: WagerTransactionKind,
    source: ProcessingSource,
  ): void;
  incrementIdempotentReplay(source: ProcessingSource): void;
  incrementInboxDuplicate(): void;
  incrementRetry(reason: string): void;
  incrementDlqMessage(reason: string): void;
  setDlqDepth(depth: number): void;
  incrementLockConflict(): void;
  setOutboxLagSeconds(lagSeconds: number): void;
  incrementOutboxPublished(eventType: string): void;
  incrementOutboxPublishFailure(): void;
  observeProcessingDuration(source: ProcessingSource, durationSeconds: number): void;
  incrementPendingReference(): void;
  incrementReconciliationMismatch(): void;
}

export interface EventPublisherPort {
  publish(message: OutboxMessage): Promise<void>;
}

export interface ProviderIdentity {
  providerId?: string;
  authenticated: boolean;
}

export interface ProviderIdentityPort {
  verifyRequest(headers: Record<string, string | string[] | undefined>): Promise<ProviderIdentity>;
}

export const TOKENS = {
  UnitOfWork: Symbol.for("UnitOfWork"),
  WalletRepository: Symbol.for("WalletRepository"),
  WagerTransactionRepository: Symbol.for("WagerTransactionRepository"),
  LedgerRepository: Symbol.for("LedgerRepository"),
  InboxRepository: Symbol.for("InboxRepository"),
  OutboxRepository: Symbol.for("OutboxRepository"),
  ClockPort: Symbol.for("ClockPort"),
  IdGeneratorPort: Symbol.for("IdGeneratorPort"),
  MetricsPort: Symbol.for("MetricsPort"),
  EventPublisherPort: Symbol.for("EventPublisherPort"),
  ProviderIdentityPort: Symbol.for("ProviderIdentityPort"),
} as const;
