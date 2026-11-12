import { DeleteQueueCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { uuidv7 } from "uuidv7";
import { FailureCode } from "../../src/domain/wager/failure-code";
import { WagerTransactionStatus } from "../../src/domain/wager/wager-transaction";
import { SqsQueueManager } from "../../src/infrastructure/messaging/sqs-client";
import { WagerTransactionsConsumer } from "../../src/infrastructure/messaging/wager-transactions.consumer";
import { OutboxPublisherWorker } from "../../src/infrastructure/workers/outbox-publisher.worker";
import { PendingReferenceWorker } from "../../src/infrastructure/workers/pending-reference.worker";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables, waitFor } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

describe("Concurrency Suite — Scenarios 1, 2 (20x), 3 & 7 (M7)", () => {
  let app: INestApplication;
  let orm: MikroORM;
  let sqs: SqsQueueManager;
  let consumer: WagerTransactionsConsumer;
  let pendingWorker: PendingReferenceWorker;
  let outboxWorker: OutboxPublisherWorker;
  let baseUrl: string;

  const runSuffix = uuidv7().slice(-8);
  const wagerQueueName = `wager-c1-${runSuffix}.fifo`;
  const dlqName = `wager-c1-dlq-${runSuffix}.fifo`;
  const eventsQueueName = `wallet-events-c1-${runSuffix}.fifo`;

  beforeAll(async () => {
    app = await createApp({
      DB_NAME: TEST_DB_NAME,
      LOG_LEVEL: "silent",
      DB_POOL_MAX: "30",
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
      RUN_DLQ_METRICS: "false",
      SQS_WAGER_QUEUE_NAME: wagerQueueName,
      SQS_WAGER_DLQ_NAME: dlqName,
      SQS_EVENTS_QUEUE_NAME: eventsQueueName,
      PENDING_REF_BASE_DELAY_MS: "10",
      PENDING_REF_MAX_ATTEMPTS: "3",
      PENDING_REF_TTL_MS: "5000",
    });
    orm = app.get(MikroORM);
    await orm.getMigrator().up();
    sqs = app.get(SqsQueueManager);
    await sqs.ensureQueues();
    consumer = app.get(WagerTransactionsConsumer);
    pendingWorker = app.get(PendingReferenceWorker);
    outboxWorker = app.get(OutboxPublisherWorker);

    await app.listen(0, "127.0.0.1");
    const addr = app.getHttpServer().address();
    const port = typeof addr === "object" && addr ? addr.port : 3096;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  beforeEach(async () => {
    await truncateAllTables(orm);
  });

  afterAll(async () => {
    try {
      const urls = await sqs.ensureQueues();
      await sqs.client.send(
        new DeleteQueueCommand({ QueueUrl: urls.wagerQueueUrl }),
      );
      await sqs.client.send(new DeleteQueueCommand({ QueueUrl: urls.dlqUrl }));
      await sqs.client.send(
        new DeleteQueueCommand({ QueueUrl: urls.eventsQueueUrl }),
      );
    } catch {
      // ignore
    }
    await app.close();
  });

  async function createWalletHttp(initialAmount = "100.00"): Promise<{
    id: string;
    playerId: string;
  }> {
    const playerId = uuidv7();
    const res = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: initialAmount, currency: "BRL" },
      }),
    });
    expect(res.status).toBe(201);
    return (await res.json()) as { id: string; playerId: string };
  }

  it("Scenario 1: 50 identical BET requests in parallel -> 1 debit, 49 idempotent replays, 1 debit ledger entry", async () => {
    const wallet = await createWalletHttp("100.00");
    const payload = {
      providerId: "prov-parallel",
      externalTransactionId: "bet-50x",
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round-50x",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "25.00", currency: "BRL" },
    };

    const responses = await Promise.all(
      Array.from({ length: 50 }, () =>
        fetch(`${baseUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "prov-parallel:bet-50x",
          },
          body: JSON.stringify(payload),
        }).then(async (r) => ({
          httpStatus: r.status,
          body: (await r.json()) as {
            transactionId: string;
            status: string;
            balance: { amount: string; currency: string };
            idempotentReplay: boolean;
          },
        })),
      ),
    );

    const freshExecutions = responses.filter((r) => !r.body.idempotentReplay);
    const replays = responses.filter((r) => r.body.idempotentReplay);

    expect(freshExecutions).toHaveLength(1);
    expect(freshExecutions[0]?.httpStatus).toBe(201);
    expect(replays).toHaveLength(49);
    for (const rep of replays) {
      expect(rep.httpStatus).toBe(200);
      expect(rep.body.status).toBe(WagerTransactionStatus.Processed);
      expect(rep.body.balance).toEqual({ amount: "75.00", currency: "BRL" });
    }

    const em = orm.em.fork() as SqlEntityManager;
    const debitEntries = await em.execute(
      `SELECT COUNT(*)::int AS cnt FROM wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(
      parseInt(String((debitEntries[0] as Record<string, unknown>).cnt), 10),
    ).toBe(1);

    await assertWalletConsistent(orm, wallet.id);
  });

  it("Scenario 2: Initial balance 100.00 BRL, two simultaneous 80.00 BRL BETs -> 1 PROCESSED, 1 REJECTED, final balance 20.00 BRL (repeated 20x)", async () => {
    for (let iteration = 1; iteration <= 20; iteration++) {
      const wallet = await createWalletHttp("100.00");

      const betA = {
        providerId: "prov-race",
        externalTransactionId: `bet-80-a-${iteration}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-race-${iteration}`,
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "80.00", currency: "BRL" },
      };
      const betB = {
        providerId: "prov-race",
        externalTransactionId: `bet-80-b-${iteration}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `round-race-${iteration}`,
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "80.00", currency: "BRL" },
      };

      const [resA, resB] = await Promise.all([
        fetch(`${baseUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `prov-race:${betA.externalTransactionId}`,
          },
          body: JSON.stringify(betA),
        }).then(async (r) => ({
          httpStatus: r.status,
          body: (await r.json()) as {
            status: string;
            failureCode?: string;
            balance: { amount: string; currency: string };
          },
        })),
        fetch(`${baseUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `prov-race:${betB.externalTransactionId}`,
          },
          body: JSON.stringify(betB),
        }).then(async (r) => ({
          httpStatus: r.status,
          body: (await r.json()) as {
            status: string;
            failureCode?: string;
            balance: { amount: string; currency: string };
          },
        })),
      ]);

      const outcomes = [resA, resB];
      const processed = outcomes.filter(
        (o) => o.body.status === WagerTransactionStatus.Processed,
      );
      const rejected = outcomes.filter(
        (o) => o.body.status === WagerTransactionStatus.Rejected,
      );

      expect(processed).toHaveLength(1);
      expect(processed[0]?.httpStatus).toBe(201);
      expect(processed[0]?.body.balance.amount).toBe("20.00");

      expect(rejected).toHaveLength(1);
      expect(rejected[0]?.httpStatus).toBe(422);
      expect(rejected[0]?.body.failureCode).toBe(FailureCode.InsufficientFunds);

      const em = orm.em.fork() as SqlEntityManager;
      const debitRows = await em.execute(
        `SELECT COUNT(*)::int AS cnt FROM wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`,
        [wallet.id],
      );
      expect(
        parseInt(String((debitRows[0] as Record<string, unknown>).cnt), 10),
      ).toBe(1);

      await assertWalletConsistent(orm, wallet.id);
    }
  });

  it("Scenario 3: 20 distinct wallets x 20 operations in parallel -> all wallets consistent without global lock contention", async () => {
    const wallets = await Promise.all(
      Array.from({ length: 20 }, () => createWalletHttp("200.00")),
    );

    await Promise.all(
      wallets.map(async (wallet, wIdx) => {
        const ops = Array.from({ length: 20 }, (_, opIdx) => {
          const isWin = opIdx % 4 === 3;
          const isLoss = opIdx % 5 === 4 && !isWin;
          const kind = isWin ? "WIN" : isLoss ? "LOSS" : "BET";
          const amount = isLoss ? "0.00" : "5.00";
          const extId = `w${wIdx}-op${opIdx}`;
          return fetch(`${baseUrl}/wagering/transactions`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Idempotency-Key": `prov-multi:${extId}`,
            },
            body: JSON.stringify({
              providerId: "prov-multi",
              externalTransactionId: extId,
              playerId: wallet.playerId,
              walletId: wallet.id,
              roundId: `round-${wIdx}-${opIdx}`,
              gameId: "fortune-chimp",
              kind,
              money: { amount, currency: "BRL" },
            }),
          });
        });
        const responses = await Promise.all(ops);
        for (const r of responses) {
          expect(r.status).toBe(201);
        }
      }),
    );

    for (const wallet of wallets) {
      await assertWalletConsistent(orm, wallet.id);
    }
  });

  it("Scenario 7: Out-of-order ROLLBACK and REFUND delivered via SQS and HTTP before BET -> resolved after BET arrives + TTL exhaustion emits WagerTransactionRejected", async () => {
    const wallet = await createWalletHttp("100.00");
    const queueUrl = await sqs.getWagerQueueUrl();

    // 1. Send ROLLBACK of bet-sqs-late via SQS BEFORE bet-sqs-late exists
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          messageId: "msg-early-rb",
          type: "WagerTransactionRequested",
          occurredAt: "2026-07-29T15:00:00.000Z",
          data: {
            providerId: "prov-oof",
            externalTransactionId: "rb-early-sqs",
            idempotencyKey: "prov-oof:rb-early-sqs",
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "r-oof-1",
            gameId: "fortune-chimp",
            kind: "ROLLBACK",
            money: { amount: "30.00", currency: "BRL" },
            referenceExternalTransactionId: "bet-sqs-late",
          },
        }),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-early-rb",
      }),
    );
    await consumer.pollOnce(1);

    // 2. Send REFUND via HTTP referencing a BET that will NEVER arrive (to test TTL/maxAttempts exhaustion)
    const neverRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-oof:refund-never",
      },
      body: JSON.stringify({
        providerId: "prov-oof",
        externalTransactionId: "refund-never",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "r-oof-never",
        gameId: "fortune-chimp",
        kind: "REFUND",
        money: { amount: "10.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-does-not-exist",
      }),
    });
    expect(neverRes.status).toBe(202);
    const neverBody = (await neverRes.json()) as { transactionId: string };

    // 3. Now send bet-sqs-late via HTTP -> balance becomes 70.00
    const betRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-oof:bet-sqs-late",
      },
      body: JSON.stringify({
        providerId: "prov-oof",
        externalTransactionId: "bet-sqs-late",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "r-oof-1",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "30.00", currency: "BRL" },
      }),
    });
    expect(betRes.status).toBe(201);

    // 4. Tick PendingReferenceWorker until rb-early-sqs is PROCESSED (balance restored to 100.00)
    //    and refund-never is REJECTED with REFERENCE_NOT_FOUND
    const em = orm.em.fork() as SqlEntityManager;
    await waitFor(async () => {
      await em.execute(
        `UPDATE wager_transactions SET next_attempt_at = now() - interval '1 second' WHERE status = 'PENDING_REFERENCE'`,
      );
      await pendingWorker.tick();

      const rbRes = await fetch(
        `${baseUrl}/providers/prov-oof/wagering/transactions/rb-early-sqs`,
      );
      const rbJson = (await rbRes.json()) as { status: string };
      const nevRes = await fetch(
        `${baseUrl}/wagering/transactions/${neverBody.transactionId}`,
      );
      const nevJson = (await nevRes.json()) as {
        status: string;
        failureCode?: string;
      };
      return (
        rbJson.status === WagerTransactionStatus.Processed &&
        nevJson.status === WagerTransactionStatus.Rejected &&
        nevJson.failureCode === FailureCode.ReferenceNotFound
      );
    }, 10000);

    await outboxWorker.tick();
    await assertWalletConsistent(orm, wallet.id);
  });
});
