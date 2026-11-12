import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { uuidv7 } from "uuidv7";
import { ClockPort, IdGeneratorPort, MetricsPort } from "../../src/application/ports";
import { ProcessWagerTransactionUseCase } from "../../src/application/use-cases/process-wager-transaction";
import { DomainError, WalletAlreadyExistsError } from "../../src/domain/errors";
import { WalletBalanceChanged } from "../../src/domain/events";
import { OutboxMessage } from "../../src/domain/messaging/outbox-message";
import { Money } from "../../src/domain/money/money";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wager/wager-transaction";
import { Wallet } from "../../src/domain/wallet/wallet";
import { setFaultInjection } from "../../src/infrastructure/fault-injection";
import { MikroOrmUnitOfWork } from "../../src/infrastructure/persistence/unit-of-work";
import { initTestOrm, truncateAllTables } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

describe("PostgreSQL Migrations, Schema Constraints & UoW Atomicity (M2)", () => {
  let orm: MikroORM;
  let uow: MikroOrmUnitOfWork;

  beforeAll(async () => {
    orm = await initTestOrm();
    uow = new MikroOrmUnitOfWork(orm);
  });

  beforeEach(async () => {
    setFaultInjection(undefined);
    await truncateAllTables(orm);
  });

  afterAll(async () => {
    setFaultInjection(undefined);
    await orm.close(true);
  });

  async function seedWalletWithBalance(
    playerId = uuidv7(),
    amount = "100.00",
    currency = "BRL",
  ): Promise<Wallet> {
    const walletId = uuidv7();
    const txId = uuidv7();
    const entryId = uuidv7();
    const now = new Date();
    const initialMoney = Money.from({ amount, currency });
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: initialMoney,
      createdAt: now,
    });

    await uow.runInTransaction(async (tx) => {
      await tx.wallets.insert(wallet);
      if (initialMoney.isPositive()) {
        const openingTx = WagerTransaction.createOpening({
          id: txId,
          walletId,
          playerId,
          money: initialMoney,
          payloadHash: "0".repeat(64),
          createdAt: now,
        });
        const openingEntry = wallet.createOpeningEntry({
          entryId,
          transactionId: txId,
        })!;
        await tx.transactions.insert(openingTx);
        await tx.ledger.insert(openingEntry);
      }
    });

    return wallet;
  }

  it("reversibly applies migrations (up -> down -> up)", async () => {
    const migrator = orm.getMigrator();
    await migrator.down({ to: 0 });
    await migrator.up();

    const wallet = await seedWalletWithBalance(uuidv7(), "50.00", "BRL");
    await assertWalletConsistent(orm, wallet.id);
  });

  it("enforces unique wallet per (player_id, currency)", async () => {
    const playerId = uuidv7();
    await seedWalletWithBalance(playerId, "100.00", "BRL");

    expect(
      seedWalletWithBalance(playerId, "50.00", "BRL"),
    ).rejects.toThrow(WalletAlreadyExistsError);
  });

  it("enforces wallet <-> ledger consistency constraint trigger in database", async () => {
    const walletId = uuidv7();
    const playerId = uuidv7();
    const wallet = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    // Attempting to insert a wallet with positive balance WITHOUT a ledger entry must fail at commit
    expect(
      uow.runInTransaction(async (tx) => {
        await tx.wallets.insert(wallet);
      }),
    ).rejects.toThrow(DomainError);

    // Attempting to update wallet balance directly in SQL without a ledger entry must fail
    const validWallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;
    expect(
      em.transactional(async (txEm) => {
        await (txEm as SqlEntityManager).execute(
          `UPDATE wallets SET balance = 90.00, version = 2 WHERE id = ?`,
          [validWallet.id],
        );
        await (txEm as SqlEntityManager).execute("SET CONSTRAINTS ALL IMMEDIATE");
      }),
    ).rejects.toThrow();

    await assertWalletConsistent(orm, validWallet.id);
  });

  it("enforces negative balance check constraint in database", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;

    expect(
      em.execute(`UPDATE wallets SET balance = -1.00 WHERE id = ?`, [wallet.id]),
    ).rejects.toThrow();

    await assertWalletConsistent(orm, wallet.id);
  });

  it("forbids UPDATE and DELETE on wallet_ledger_entries via database trigger", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;

    expect(
      em.execute(
        `UPDATE wallet_ledger_entries SET amount = 999.00 WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).rejects.toThrow();

    expect(
      em.execute(`DELETE FROM wallet_ledger_entries WHERE wallet_id = ?`, [
        wallet.id,
      ]),
    ).rejects.toThrow();

    await assertWalletConsistent(orm, wallet.id);
  });

  it("enforces ledger arithmetic check constraint in database", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;
    const badTxId = uuidv7();

    await em.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        status, created_at, updated_at, processed_at
      ) VALUES (?, 'p1', 'ext-arith', 'p1:ext-arith', ?, ?, ?, 'r1', 'g1', 'BET', 20.00, 'BRL', 'PROCESSED', now(), now(), now())`,
      [badTxId, "a".repeat(64), wallet.id, wallet.playerId],
    );

    // 100.00 DEBIT 20.00 != 90.00 -> must fail ck_ledger_arithmetic
    expect(
      em.execute(
        `INSERT INTO wallet_ledger_entries (
          id, wallet_id, transaction_id, wallet_version, direction,
          amount, currency, balance_before, balance_after, created_at
        ) VALUES (?, ?, ?, 2, 'DEBIT', 20.00, 'BRL', 100.00, 90.00, now())`,
        [uuidv7(), wallet.id, badTxId],
      ),
    ).rejects.toThrow();
  });

  it("forbids mutating terminal wager_transactions or their immutable fields via trigger", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;

    // Opening transaction is PROCESSED (terminal) -> updating status must fail
    expect(
      em.execute(
        `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS', processed_at = NULL WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).rejects.toThrow();
  });

  it("enforces partial unique index uq_wtx_single_reversal preventing double reversal in DB", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");
    const em = orm.em.fork() as SqlEntityManager;
    const betId = uuidv7();

    await em.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        status, created_at, updated_at, processed_at
      ) VALUES (?, 'p1', 'bet-1', 'p1:bet-1', ?, ?, ?, 'r1', 'g1', 'BET', 10.00, 'BRL', 'PROCESSED', now(), now(), now())`,
      [betId, "b".repeat(64), wallet.id, wallet.playerId],
    );

    await em.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, amount, currency,
        reference_external_transaction_id, reference_transaction_id,
        status, created_at, updated_at, processed_at
      ) VALUES (?, 'p1', 'ref-1', 'p1:ref-1', ?, ?, ?, 'r1', 'g1', 'REFUND', 10.00, 'BRL', 'bet-1', ?, 'PROCESSED', now(), now(), now())`,
      [uuidv7(), "c".repeat(64), wallet.id, wallet.playerId, betId],
    );

    // Second PROCESSED reversal referencing the same betId must violate uq_wtx_single_reversal
    expect(
      em.execute(
        `INSERT INTO wager_transactions (
          id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, amount, currency,
          reference_external_transaction_id, reference_transaction_id,
          status, created_at, updated_at, processed_at
        ) VALUES (?, 'p1', 'rb-1', 'p1:rb-1', ?, ?, ?, 'r1', 'g1', 'ROLLBACK', 10.00, 'BRL', 'bet-1', ?, 'PROCESSED', now(), now(), now())`,
        [uuidv7(), "d".repeat(64), wallet.id, wallet.playerId, betId],
      ),
    ).rejects.toThrow();
  });

  it("guarantees full SQL atomicity across wallet, transaction, ledger, inbox and outbox on failure", async () => {
    const wallet = await seedWalletWithBalance(uuidv7(), "100.00", "BRL");

    const clock: ClockPort = { now: () => new Date() };
    const idGen: IdGeneratorPort = { generate: () => uuidv7() };
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

    // Inject fault right after ledger insert
    setFaultInjection("after_ledger_insert");

    expect(
      useCase.execute(
        {
          providerId: "prov-a",
          externalTransactionId: "tx-atomic-fail",
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "r-atomic",
          gameId: "g-atomic",
          kind: WagerTransactionKind.Bet,
          money: { amount: "40.00", currency: "BRL" },
        },
        {
          source: "SQS",
          idempotencyKey: "prov-a:tx-atomic-fail",
          correlationId: "corr-atomic",
          inbox: {
            consumerName: "wager-consumer",
            messageId: "msg-atomic-1",
          },
        },
      ),
    ).rejects.toThrow("FAULT_INJECTION");

    setFaultInjection(undefined);

    // Verify NOTHING from the failed attempt persisted:
    // - wallet balance is still 100.00 and version is 1
    // - wager_transactions has only the opening transaction
    // - wallet_ledger_entries has only the opening entry
    // - inbox_messages has 0 rows
    // - outbox_messages has 0 rows
    await assertWalletConsistent(orm, wallet.id);

    const em = orm.em.fork() as SqlEntityManager;
    const inboxRows = await em.execute(`SELECT * FROM inbox_messages`);
    const outboxRows = await em.execute(`SELECT * FROM outbox_messages`);
    const betRows = await em.execute(
      `SELECT * FROM wager_transactions WHERE external_transaction_id = 'tx-atomic-fail'`,
    );

    expect(inboxRows).toHaveLength(0);
    expect(outboxRows).toHaveLength(0);
    expect(betRows).toHaveLength(0);
  });
});
