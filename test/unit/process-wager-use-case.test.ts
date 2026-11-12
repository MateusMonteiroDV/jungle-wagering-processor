import { describe, expect, it } from "bun:test";
import { uuidv7 } from "uuidv7";
import {
  ClockPort,
  IdGeneratorPort,
  InboxRepository,
  LedgerRepository,
  MetricsPort,
  OutboxRepository,
  TransactionalContext,
  UnitOfWork,
  WagerTransactionRepository,
  WalletRepository,
} from "../../src/application/ports";
import { ProcessWagerTransactionUseCase } from "../../src/application/use-cases/process-wager-transaction";
import { IdempotencyConflictError } from "../../src/domain/errors";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../src/domain/ledger/wallet-ledger-entry";
import { InboxMessage } from "../../src/domain/messaging/inbox-message";
import { OutboxMessage } from "../../src/domain/messaging/outbox-message";
import { Money } from "../../src/domain/money/money";
import { FailureCode } from "../../src/domain/wager/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wager/wager-transaction";
import { Wallet } from "../../src/domain/wallet/wallet";

function createInMemoryHarness(initialBalance = "100.00", currency = "BRL") {
  const wallets = new Map<string, Wallet>();
  const transactions = new Map<string, WagerTransaction>();
  const ledger: WalletLedgerEntry[] = [];
  const inbox = new Map<string, InboxMessage>();
  const outbox: OutboxMessage[] = [];

  const playerId = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1";
  const walletId = "0192f291-27dd-7d3f-8071-5f8685deef37";
  const wallet = Wallet.open({
    id: walletId,
    playerId,
    initialBalance: Money.from({ amount: initialBalance, currency }),
  });
  wallets.set(walletId, wallet);

  const walletRepo: WalletRepository = {
    async findById(id) {
      return wallets.get(id) ?? null;
    },
    async findByIdForUpdate(id) {
      return wallets.get(id) ?? null;
    },
    async findByIdForShare(id) {
      return wallets.get(id) ?? null;
    },
    async findByPlayerAndCurrency(pid, curr) {
      for (const w of wallets.values()) {
        if (w.playerId === pid && w.currency === curr) return w;
      }
      return null;
    },
    async insert(w) {
      wallets.set(w.id, w);
    },
    async updateWithVersion(w) {
      w.markPersisted();
      wallets.set(w.id, w);
    },
  };

  const txRepo: WagerTransactionRepository = {
    async findById(id) {
      return transactions.get(id) ?? null;
    },
    async findByIdForUpdate(id) {
      return transactions.get(id) ?? null;
    },
    async findByIdempotencyKey(key) {
      for (const t of transactions.values()) {
        if (t.idempotencyKey === key) return t;
      }
      return null;
    },
    async findByProviderAndExternalId(providerId, extId) {
      for (const t of transactions.values()) {
        if (t.providerId === providerId && t.externalTransactionId === extId) {
          return t;
        }
      }
      return null;
    },
    async findProcessedReversalForReference(refId) {
      for (const t of transactions.values()) {
        if (
          t.referenceTransactionId === refId &&
          (t.kind === WagerTransactionKind.Refund ||
            t.kind === WagerTransactionKind.Rollback) &&
          t.status === WagerTransactionStatus.Processed
        ) {
          return t;
        }
      }
      return null;
    },
    async findPendingByReferenceExternalId(wId, providerId, refExtId) {
      const res: WagerTransaction[] = [];
      for (const t of transactions.values()) {
        if (
          t.walletId === wId &&
          t.providerId === providerId &&
          t.referenceExternalTransactionId === refExtId &&
          t.status === WagerTransactionStatus.PendingReference
        ) {
          res.push(t);
        }
      }
      return res;
    },
    async claimDuePendingReferences() {
      return [];
    },
    async insert(t) {
      transactions.set(t.id, t);
    },
    async update(t) {
      transactions.set(t.id, t);
    },
  };

  const ledgerRepo: LedgerRepository = {
    async insert(entry) {
      ledger.push(entry);
    },
    async listByWallet() {
      return { items: ledger };
    },
    async calculateWalletSummary() {
      return {
        calculatedBalance: wallet.balance,
        checkedEntries: ledger.length,
        maxWalletVersion: wallet.version,
      };
    },
  };

  const inboxRepo: InboxRepository = {
    async tryInsert(msg) {
      const k = `${msg.consumerName}:${msg.messageId}`;
      const existing = inbox.get(k);
      if (existing) {
        return { inserted: false, existing };
      }
      inbox.set(k, msg);
      return { inserted: true };
    },
    async markProcessed(consumerName, messageId, processedAt) {
      const k = `${consumerName}:${messageId}`;
      const existing = inbox.get(k);
      if (existing) existing.markProcessed(processedAt);
    },
  };

  const outboxRepo: OutboxRepository = {
    async insert(msg) {
      outbox.push(msg);
    },
    async insertMany(msgs) {
      outbox.push(...msgs);
    },
    async leaseDueMessages() {
      return [];
    },
    async markPublished() {},
    async scheduleRetry() {},
    async getOldestUnpublishedOccurredAt() {
      return null;
    },
  };

  const ctx: TransactionalContext = {
    wallets: walletRepo,
    transactions: txRepo,
    ledger: ledgerRepo,
    inbox: inboxRepo,
    outbox: outboxRepo,
  };

  const uow: UnitOfWork = {
    async runInTransaction(work) {
      return work(ctx);
    },
  };

  const clock: ClockPort = {
    now: () => new Date("2026-07-29T15:00:00.000Z"),
  };

  const idGen: IdGeneratorPort = {
    generate: () => uuidv7(),
  };

  const metrics: MetricsPort = {
    incrementTransaction() {},
    incrementIdempotentReplay() {},
    incrementInboxDuplicate() {},
    incrementRetry() {},
    incrementDlqMessage() {},
    setDlqDepth() {},
    incrementLockConflict() {},
    setOutboxLagSeconds() {},
    incrementOutboxPublished() {},
    incrementOutboxPublishFailure() {},
    observeProcessingDuration() {},
    incrementPendingReference() {},
    incrementReconciliationMismatch() {},
  };

  const useCase = new ProcessWagerTransactionUseCase(uow, clock, idGen, metrics);

  return {
    useCase,
    playerId,
    walletId,
    wallets,
    transactions,
    ledger,
    outbox,
  };
}

describe("ProcessWagerTransactionUseCase (Domain & Application Rules)", () => {
  it("processes BET, WIN, LOSS, REFUND, and ROLLBACK according to business rules", async () => {
    const h = createInMemoryHarness("100.00", "BRL");

    // 1. BET of 25.00 -> balance 75.00, 1 DEBIT ledger, 2 outbox events
    const betRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "25.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-1",
        correlationId: "corr-1",
      },
    );

    expect(betRes.status).toBe(WagerTransactionStatus.Processed);
    expect(betRes.balance).toEqual({ amount: "75.00", currency: "BRL" });
    expect(betRes.idempotentReplay).toBe(false);
    expect(h.ledger).toHaveLength(1);
    expect(h.ledger[0]?.direction).toBe(LedgerDirection.Debit);

    // 2. LOSS of 0.00 -> balance stays 75.00, no new ledger entry, 1 WagerTransactionProcessed event
    const lossRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "loss-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Loss,
        money: { amount: "0.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:loss-1",
        correlationId: "corr-2",
      },
    );

    expect(lossRes.status).toBe(WagerTransactionStatus.Processed);
    expect(lossRes.balance).toEqual({ amount: "75.00", currency: "BRL" });
    expect(h.ledger).toHaveLength(1);

    // 3. WIN of 50.00 referencing bet-1 -> balance 125.00, 1 CREDIT ledger
    const winRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "win-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Win,
        money: { amount: "50.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-1",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:win-1",
        correlationId: "corr-3",
      },
    );

    expect(winRes.status).toBe(WagerTransactionStatus.Processed);
    expect(winRes.balance).toEqual({ amount: "125.00", currency: "BRL" });
    expect(h.ledger).toHaveLength(2);

    // 4. REFUND of bet-1 (25.00) -> balance 150.00, 1 CREDIT ledger
    const refundRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "refund-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Refund,
        money: { amount: "25.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-1",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:refund-1",
        correlationId: "corr-4",
      },
    );

    expect(refundRes.status).toBe(WagerTransactionStatus.Processed);
    expect(refundRes.balance).toEqual({ amount: "150.00", currency: "BRL" });
    expect(h.ledger).toHaveLength(3);

    // 5. Second reversal of bet-1 -> REJECTED with REFERENCE_ALREADY_REVERSED
    const dupReversalRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "rollback-bet-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Rollback,
        money: { amount: "25.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-1",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:rollback-bet-1",
        correlationId: "corr-5",
      },
    );

    expect(dupReversalRes.status).toBe(WagerTransactionStatus.Rejected);
    expect(dupReversalRes.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    expect(dupReversalRes.balance).toEqual({ amount: "150.00", currency: "BRL" });
    expect(h.ledger).toHaveLength(3);

    // 6. ROLLBACK of win-1 (50.00) -> reverses CREDIT by debiting 50.00 -> balance 100.00
    const rollbackWinRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "rollback-win-1",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Rollback,
        money: { amount: "50.00", currency: "BRL" },
        referenceExternalTransactionId: "win-1",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:rollback-win-1",
        correlationId: "corr-6",
      },
    );

    expect(rollbackWinRes.status).toBe(WagerTransactionStatus.Processed);
    expect(rollbackWinRes.balance).toEqual({ amount: "100.00", currency: "BRL" });
    expect(h.ledger).toHaveLength(4);
    expect(h.ledger[3]?.direction).toBe(LedgerDirection.Debit);
  });

  it("distinguishes INSUFFICIENT_FUNDS (BET) from REVERSAL_INSUFFICIENT_FUNDS (ROLLBACK of WIN)", async () => {
    const h = createInMemoryHarness("20.00", "BRL");

    // BET 50.00 on 20.00 balance -> REJECTED INSUFFICIENT_FUNDS
    const betFail = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-big",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-2",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "50.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-big",
        correlationId: "corr-1",
      },
    );
    expect(betFail.status).toBe(WagerTransactionStatus.Rejected);
    expect(betFail.failureCode).toBe(FailureCode.InsufficientFunds);

    // WIN 30.00 -> balance becomes 50.00
    await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "win-30",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-2",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Win,
        money: { amount: "30.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:win-30",
        correlationId: "corr-2",
      },
    );

    // Player bets 40.00 -> balance becomes 10.00
    await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-40",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-3",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "40.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-40",
        correlationId: "corr-3",
      },
    );

    // Now ROLLBACK of win-30 (30.00) when balance is only 10.00 -> REJECTED REVERSAL_INSUFFICIENT_FUNDS
    const rbFail = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "rb-win-30",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-2",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Rollback,
        money: { amount: "30.00", currency: "BRL" },
        referenceExternalTransactionId: "win-30",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:rb-win-30",
        correlationId: "corr-4",
      },
    );

    expect(rbFail.status).toBe(WagerTransactionStatus.Rejected);
    expect(rbFail.failureCode).toBe(FailureCode.ReversalInsufficientFunds);
    expect(rbFail.balance).toEqual({ amount: "10.00", currency: "BRL" });
  });

  it("handles currency conflict, idempotent replay with historical balance, and divergent payload conflict", async () => {
    const h = createInMemoryHarness("100.00", "BRL");

    // Currency mismatch -> REJECTED CURRENCY_MISMATCH
    const usdBet = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-usd",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "10.00", currency: "USD" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-usd",
        correlationId: "corr-1",
      },
    );
    expect(usdBet.status).toBe(WagerTransactionStatus.Rejected);
    expect(usdBet.failureCode).toBe(FailureCode.CurrencyMismatch);

    // Valid BET of 30.00 -> balance 70.00
    const firstBet = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-30",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "30.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-30",
        correlationId: "corr-2",
      },
    );
    expect(firstBet.balance.amount).toBe("70.00");

    // Another BET of 20.00 -> balance 50.00
    await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-20",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "20.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-20",
        correlationId: "corr-3",
      },
    );

    // Replay of firstBet -> returns original balance observed at that time (70.00) and idempotentReplay: true
    const replayBet = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-30",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-1",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "30.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-30",
        correlationId: "corr-4",
      },
    );
    expect(replayBet.idempotentReplay).toBe(true);
    expect(replayBet.transactionId).toBe(firstBet.transactionId);
    expect(replayBet.balance.amount).toBe("70.00");

    // Divergent payload with same Idempotency-Key -> throws IdempotencyConflictError
    expect(
      h.useCase.execute(
        {
          providerId: "prov-a",
          externalTransactionId: "bet-30",
          playerId: h.playerId,
          walletId: h.walletId,
          roundId: "round-1",
          gameId: "chimp-slot",
          kind: WagerTransactionKind.Bet,
          money: { amount: "35.00", currency: "BRL" },
        },
        {
          source: "HTTP",
          idempotencyKey: "prov-a:bet-30",
          correlationId: "corr-5",
        },
      ),
    ).rejects.toThrow(IdempotencyConflictError);
  });

  it("parks out-of-order REFUND as PENDING_REFERENCE and resolves it once BET arrives", async () => {
    const h = createInMemoryHarness("100.00", "BRL");

    // REFUND arrives before BET
    const pendingRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "refund-early",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-99",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Refund,
        money: { amount: "40.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-late",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:refund-early",
        correlationId: "corr-1",
      },
    );

    expect(pendingRes.status).toBe(WagerTransactionStatus.PendingReference);
    expect(pendingRes.balance.amount).toBe("100.00");

    // BET arrives -> balance becomes 60.00
    const betRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-late",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-99",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "40.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-late",
        correlationId: "corr-2",
      },
    );
    expect(betRes.status).toBe(WagerTransactionStatus.Processed);
    expect(betRes.balance.amount).toBe("60.00");

    // Worker re-evaluates the pending REFUND -> now PROCESSED, balance back to 100.00
    const resolvedRes = await h.useCase.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "refund-early",
        playerId: h.playerId,
        walletId: h.walletId,
        roundId: "round-99",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Refund,
        money: { amount: "40.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-late",
      },
      {
        source: "WORKER",
        idempotencyKey: "prov-a:refund-early",
        correlationId: "corr-3",
      },
    );
    expect(resolvedRes.status).toBe(WagerTransactionStatus.Processed);
    expect(resolvedRes.balance.amount).toBe("100.00");
  });
});
