import {
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { uuidv7 } from "uuidv7";
import { loadEnv } from "../../src/infrastructure/config/env";
import { SqsQueueManager } from "../../src/infrastructure/messaging/sqs-client";
import {
  TEST_DB_NAME,
  initTestOrm,
  truncateAllTables,
  waitFor,
} from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";
import { SpawnedInstance, spawnAppInstance } from "../helpers/spawn-cluster";

describe("Multi-Process Concurrency, Crash Recovery & SIGKILL Resilience — Scenarios 4, 5, 6 & 8 (M7)", () => {
  let orm: MikroORM;
  let sqs: SqsQueueManager;

  const runSuffix = uuidv7().slice(-8);
  const wagerQueueName = `wager-mp-${runSuffix}.fifo`;
  const dlqName = `wager-mp-dlq-${runSuffix}.fifo`;
  const eventsQueueName = `wallet-events-mp-${runSuffix}.fifo`;

  const baseClusterEnv: Record<string, string> = {
    DB_NAME: TEST_DB_NAME,
    LOG_LEVEL: "silent",
    SQS_WAGER_QUEUE_NAME: wagerQueueName,
    SQS_WAGER_DLQ_NAME: dlqName,
    SQS_EVENTS_QUEUE_NAME: eventsQueueName,
    SQS_VISIBILITY_TIMEOUT_SEC: "2",
    SQS_WAIT_TIME_SEC: "1",
    SQS_CONSUMER_POLL_INTERVAL_MS: "50",
    OUTBOX_POLL_INTERVAL_MS: "100",
    OUTBOX_LEASE_SEC: "2",
    PENDING_REF_POLL_INTERVAL_MS: "100",
    PENDING_REF_BASE_DELAY_MS: "50",
    DLQ_METRICS_POLL_INTERVAL_MS: "5000",
  };

  beforeAll(async () => {
    orm = await initTestOrm(TEST_DB_NAME);
    sqs = new SqsQueueManager(loadEnv(baseClusterEnv));
    await sqs.ensureQueues();
  });

  beforeEach(async () => {
    await truncateAllTables(orm);
    await drainQueue(await sqs.getWagerQueueUrl());
    await drainQueue(await sqs.getEventsQueueUrl());
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
    await orm.close(true);
  });

  async function drainQueue(queueUrl: string): Promise<void> {
    for (let i = 0; i < 10; i++) {
      const res = await sqs.client.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        }),
      );
      if (!res.Messages || res.Messages.length === 0) break;
      for (const m of res.Messages) {
        await sqs.client.send(
          new DeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: m.ReceiptHandle!,
          }),
        );
      }
    }
  }

  it("Scenario 4: 3 simultaneous OS processes receiving round-robin HTTP + SQS messages on shared wallets", async () => {
    const nodes: SpawnedInstance[] = [];
    try {
      const [n1, n2, n3] = await Promise.all([
        spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "node-1" }),
        spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "node-2" }),
        spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "node-3" }),
      ]);
      nodes.push(n1, n2, n3);

      // Create 3 wallets across the 3 instances
      const wallets = await Promise.all(
        nodes.map(async (node) => {
          const res = await fetch(`${node.baseUrl}/wallets`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              playerId: uuidv7(),
              initialBalance: { amount: "200.00", currency: "BRL" },
            }),
          });
          expect(res.status).toBe(201);
          return (await res.json()) as { id: string; playerId: string };
        }),
      );

      // Send 15 HTTP transactions round-robin across the 3 nodes for the 3 wallets
      const httpPromises = Array.from({ length: 15 }, (_, idx) => {
        const node = nodes[idx % nodes.length]!;
        const wallet = wallets[idx % wallets.length]!;
        const extId = `http-mp-${idx}`;
        return fetch(`${node.baseUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `prov-mp:${extId}`,
          },
          body: JSON.stringify({
            providerId: "prov-mp",
            externalTransactionId: extId,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `round-mp-${idx}`,
            gameId: "fortune-chimp",
            kind: idx % 3 === 0 ? "WIN" : "BET",
            money: { amount: "10.00", currency: "BRL" },
          }),
        });
      });

      // Simultaneously enqueue 9 SQS messages for the same 3 wallets
      const wagerQueueUrl = await sqs.getWagerQueueUrl();
      const sqsPromises = Array.from({ length: 9 }, (_, idx) => {
        const wallet = wallets[idx % wallets.length]!;
        const extId = `sqs-mp-${idx}`;
        const msgId = `msg-mp-${idx}`;
        return sqs.client.send(
          new SendMessageCommand({
            QueueUrl: wagerQueueUrl,
            MessageBody: JSON.stringify({
              messageId: msgId,
              type: "WagerTransactionRequested",
              occurredAt: new Date().toISOString(),
              data: {
                providerId: "prov-mp",
                externalTransactionId: extId,
                idempotencyKey: `prov-mp:${extId}`,
                playerId: wallet.playerId,
                walletId: wallet.id,
                roundId: `round-sqs-${idx}`,
                gameId: "fortune-chimp",
                kind: "BET",
                money: { amount: "5.00", currency: "BRL" },
              },
            }),
            MessageGroupId: wallet.id,
            MessageDeduplicationId: msgId,
          }),
        );
      });

      const httpResponses = await Promise.all(httpPromises);
      for (const r of httpResponses) {
        expect(r.status).toBe(201);
      }
      await Promise.all(sqsPromises);

      const em = orm.em.fork() as SqlEntityManager;
      // Wait for all 3 (OPENING) + 15 (HTTP) + 9 (SQS) = 27 transactions to be PROCESSED
      // and all outbox messages to be published
      await waitFor(async () => {
        const txRows = await em.execute(
          `SELECT COUNT(*)::int AS cnt FROM wager_transactions WHERE status = 'PROCESSED'`,
        );
        const outboxRows = await em.execute(
          `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
        );
        const processedCnt = parseInt(
          String((txRows[0] as Record<string, unknown>).cnt),
          10,
        );
        const unpublishedCnt = parseInt(
          String((outboxRows[0] as Record<string, unknown>).cnt),
          10,
        );
        return processedCnt === 27 && unpublishedCnt === 0;
      }, 15000);

      for (const w of wallets) {
        await assertWalletConsistent(orm, w.id);
      }
    } finally {
      await Promise.all(nodes.map((n) => n.kill("SIGTERM")));
    }
  }, 30000);

  it("Scenario 5: Worker killed after SQL commit and before SQS ack -> redelivery deduplicated via inbox with single debit", async () => {
    const setupNode = await spawnAppInstance({
      ...baseClusterEnv,
      INSTANCE_ID: "setup-node",
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
    });

    let wallet: { id: string; playerId: string };
    try {
      const res = await fetch(`${setupNode.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: uuidv7(),
          initialBalance: { amount: "100.00", currency: "BRL" },
        }),
      });
      wallet = (await res.json()) as { id: string; playerId: string };
    } finally {
      await setupNode.kill("SIGTERM");
    }

    // Send SQS message
    const wagerQueueUrl = await sqs.getWagerQueueUrl();
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: wagerQueueUrl,
        MessageBody: JSON.stringify({
          messageId: "msg-crash-after-commit",
          type: "WagerTransactionRequested",
          occurredAt: new Date().toISOString(),
          data: {
            providerId: "prov-crash",
            externalTransactionId: "bet-crash-1",
            idempotencyKey: "prov-crash:bet-crash-1",
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "round-crash",
            gameId: "fortune-chimp",
            kind: "BET",
            money: { amount: "35.00", currency: "BRL" },
          },
        }),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-crash-1",
      }),
    );

    // Spawn worker with FAULT_INJECTION=crash_after_commit -> exits with 137 after DB commit and before DeleteMessage
    const crashingWorker = await spawnAppInstance(
      {
        ...baseClusterEnv,
        INSTANCE_ID: "worker-crash-commit",
        RUN_HTTP: "false",
        RUN_CONSUMER: "true",
        RUN_OUTBOX: "false",
        RUN_PENDING: "false",
        FAULT_INJECTION: "crash_after_commit",
      },
      { waitForReady: false },
    );

    const exitCode = await crashingWorker.proc.exited;
    expect(exitCode).toBe(137);

    // Verify DB already committed the 35.00 debit (balance = 65.00)
    await assertWalletConsistent(orm, wallet.id);

    // Now spawn a healthy worker that will receive the SQS redelivery after the 2s visibility timeout
    const recoveryWorker = await spawnAppInstance({
      ...baseClusterEnv,
      INSTANCE_ID: "worker-recovery",
      RUN_HTTP: "true",
      RUN_CONSUMER: "true",
      RUN_OUTBOX: "true",
      RUN_PENDING: "false",
    });

    try {
      await waitFor(async () => {
        const attr = await sqs.client.send(
          new GetQueueAttributesCommand({
            QueueUrl: wagerQueueUrl,
            AttributeNames: [
              "ApproximateNumberOfMessages",
              "ApproximateNumberOfMessagesNotVisible",
            ],
          }),
        );
        const vis = parseInt(
          attr.Attributes?.ApproximateNumberOfMessages ?? "0",
          10,
        );
        const notVis = parseInt(
          attr.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "0",
          10,
        );
        return vis === 0 && notVis === 0;
      }, 15000);

      // Verify wallet balance is still 65.00 and only 1 DEBIT entry exists
      const em = orm.em.fork() as SqlEntityManager;
      const wRows = await em.execute(
        `SELECT balance FROM wallets WHERE id = ?`,
        [wallet.id],
      );
      expect(String((wRows[0] as Record<string, unknown>).balance)).toBe("65.00");

      const debitRows = await em.execute(
        `SELECT COUNT(*)::int AS cnt FROM wallet_ledger_entries WHERE wallet_id = ? AND direction = 'DEBIT'`,
        [wallet.id],
      );
      expect(
        parseInt(String((debitRows[0] as Record<string, unknown>).cnt), 10),
      ).toBe(1);

      await assertWalletConsistent(orm, wallet.id);
    } finally {
      await recoveryWorker.kill("SIGTERM");
    }
  }, 30000);

  it("Scenario 6: Two outbox publisher processes with crash_after_lease in the first -> second process publishes after lease expiration", async () => {
    const httpOnlyNode = await spawnAppInstance({
      ...baseClusterEnv,
      INSTANCE_ID: "http-only-node",
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
    });

    let walletId: string;
    try {
      const wRes = await fetch(`${httpOnlyNode.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: uuidv7(),
          initialBalance: { amount: "100.00", currency: "BRL" },
        }),
      });
      const wallet = (await wRes.json()) as { id: string; playerId: string };
      walletId = wallet.id;

      await fetch(`${httpOnlyNode.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "prov-lease:bet-1",
        },
        body: JSON.stringify({
          providerId: "prov-lease",
          externalTransactionId: "bet-1",
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "r-lease",
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "20.00", currency: "BRL" },
        }),
      });
    } finally {
      await httpOnlyNode.kill("SIGTERM");
    }

    // Spawn first outbox publisher with FAULT_INJECTION=crash_after_lease and OUTBOX_LEASE_SEC=1
    const crashingPub = await spawnAppInstance(
      {
        ...baseClusterEnv,
        INSTANCE_ID: "pub-crash-lease",
        RUN_HTTP: "false",
        RUN_CONSUMER: "false",
        RUN_OUTBOX: "true",
        RUN_PENDING: "false",
        OUTBOX_LEASE_SEC: "1",
        FAULT_INJECTION: "crash_after_lease",
      },
      { waitForReady: false },
    );

    const exitCode = await crashingPub.proc.exited;
    expect(exitCode).toBe(137);

    // Verify rows are leased by pub-crash-lease and not yet published
    const em = orm.em.fork() as SqlEntityManager;
    const leasedRows = await em.execute(
      `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE locked_by = 'pub-crash-lease' AND published_at IS NULL`,
    );
    expect(
      parseInt(String((leasedRows[0] as Record<string, unknown>).cnt), 10),
    ).toBe(3);

    // Spawn second healthy outbox publisher -> waits for the 1s lease to expire and publishes all 3 messages
    const healthyPub = await spawnAppInstance(
      {
        ...baseClusterEnv,
        INSTANCE_ID: "pub-healthy-2",
        RUN_HTTP: "false",
        RUN_CONSUMER: "false",
        RUN_OUTBOX: "true",
        RUN_PENDING: "false",
      },
      { waitForReady: false },
    );

    try {
      await waitFor(async () => {
        const rows = await em.execute(
          `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
        );
        return (
          parseInt(String((rows[0] as Record<string, unknown>).cnt), 10) === 0
        );
      }, 10000);

      await assertWalletConsistent(orm, walletId);
    } finally {
      await healthyPub.kill("SIGTERM");
    }
  }, 30000);

  it("Scenario 8: 3 instances under load killed with SIGKILL mid-flight -> restarted cluster drains queues/outbox/pending and reconciles all wallets", async () => {
    const wave1 = await Promise.all([
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w1-1" }),
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w1-2" }),
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w1-3" }),
    ]);

    const wallets: Array<{ id: string; playerId: string }> = [];
    for (let i = 0; i < 5; i++) {
      const node = wave1[i % wave1.length]!;
      const res = await fetch(`${node.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: uuidv7(),
          initialBalance: { amount: "500.00", currency: "BRL" },
        }),
      });
      wallets.push((await res.json()) as { id: string; playerId: string });
    }

    // Prepare 20 operations (including out-of-order REFUNDs before BETs)
    const operations: Array<{
      idempotencyKey: string;
      payload: Record<string, unknown>;
    }> = [];

    for (let wIdx = 0; wIdx < wallets.length; wIdx++) {
      const w = wallets[wIdx]!;
      // Out-of-order REFUND before BET
      operations.push({
        idempotencyKey: `prov-kill:refund-${wIdx}`,
        payload: {
          providerId: "prov-kill",
          externalTransactionId: `refund-${wIdx}`,
          playerId: w.playerId,
          walletId: w.id,
          roundId: `round-kill-${wIdx}`,
          gameId: "fortune-chimp",
          kind: "REFUND",
          money: { amount: "20.00", currency: "BRL" },
          referenceExternalTransactionId: `bet-ref-${wIdx}`,
        },
      });
      // Referenced BET
      operations.push({
        idempotencyKey: `prov-kill:bet-ref-${wIdx}`,
        payload: {
          providerId: "prov-kill",
          externalTransactionId: `bet-ref-${wIdx}`,
          playerId: w.playerId,
          walletId: w.id,
          roundId: `round-kill-${wIdx}`,
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "20.00", currency: "BRL" },
        },
      });
      // Regular BET + WIN
      operations.push({
        idempotencyKey: `prov-kill:bet-reg-${wIdx}`,
        payload: {
          providerId: "prov-kill",
          externalTransactionId: `bet-reg-${wIdx}`,
          playerId: w.playerId,
          walletId: w.id,
          roundId: `round-reg-${wIdx}`,
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "50.00", currency: "BRL" },
        },
      });
      operations.push({
        idempotencyKey: `prov-kill:win-reg-${wIdx}`,
        payload: {
          providerId: "prov-kill",
          externalTransactionId: `win-reg-${wIdx}`,
          playerId: w.playerId,
          walletId: w.id,
          roundId: `round-reg-${wIdx}`,
          gameId: "fortune-chimp",
          kind: "WIN",
          money: { amount: "30.00", currency: "BRL" },
        },
      });
    }

    // Also enqueue 5 SQS messages
    const wagerQueueUrl = await sqs.getWagerQueueUrl();
    for (let wIdx = 0; wIdx < wallets.length; wIdx++) {
      const w = wallets[wIdx]!;
      await sqs.client.send(
        new SendMessageCommand({
          QueueUrl: wagerQueueUrl,
          MessageBody: JSON.stringify({
            messageId: `msg-kill-sqs-${wIdx}`,
            type: "WagerTransactionRequested",
            occurredAt: new Date().toISOString(),
            data: {
              providerId: "prov-kill",
              externalTransactionId: `sqs-bet-${wIdx}`,
              idempotencyKey: `prov-kill:sqs-bet-${wIdx}`,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `round-sqs-${wIdx}`,
              gameId: "fortune-chimp",
              kind: "BET",
              money: { amount: "15.00", currency: "BRL" },
            },
          }),
          MessageGroupId: w.id,
          MessageDeduplicationId: `dedup-kill-${wIdx}`,
        }),
      );
    }

    // Fire HTTP requests without awaiting completion, then SIGKILL all 3 nodes mid-flight!
    const inFlightHttp = operations.map((op, idx) => {
      const node = wave1[idx % wave1.length]!;
      return fetch(`${node.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": op.idempotencyKey,
        },
        body: JSON.stringify(op.payload),
      }).catch(() => null);
    });

    await Bun.sleep(40);
    await Promise.all(wave1.map((n) => n.kill("SIGKILL")));
    await Promise.allSettled(inFlightHttp);

    // Now start 3 replacement instances and replay all HTTP operations idempotently
    const wave2 = await Promise.all([
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w2-1" }),
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w2-2" }),
      spawnAppInstance({ ...baseClusterEnv, INSTANCE_ID: "w2-3" }),
    ]);

    try {
      for (let idx = 0; idx < operations.length; idx++) {
        const op = operations[idx]!;
        const node = wave2[idx % wave2.length]!;
        const res = await fetch(`${node.baseUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": op.idempotencyKey,
          },
          body: JSON.stringify(op.payload),
        });
        expect([200, 201, 202]).toContain(res.status);
      }

      const em = orm.em.fork() as SqlEntityManager;
      // Wait for all 5 (OPENING) + 20 (HTTP) + 5 (SQS) = 30 transactions to be PROCESSED
      // and all outbox messages to be published
      await waitFor(async () => {
        await em.execute(
          `UPDATE wager_transactions SET next_attempt_at = now() - interval '1 second' WHERE status = 'PENDING_REFERENCE'`,
        );
        const txRows = await em.execute(
          `SELECT COUNT(*)::int AS cnt FROM wager_transactions WHERE status = 'PROCESSED'`,
        );
        const outboxRows = await em.execute(
          `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
        );
        const processedCnt = parseInt(
          String((txRows[0] as Record<string, unknown>).cnt),
          10,
        );
        const unpublishedCnt = parseInt(
          String((outboxRows[0] as Record<string, unknown>).cnt),
          10,
        );
        return processedCnt === 30 && unpublishedCnt === 0;
      }, 20000);

      // Verify invariant & reconciliation endpoint on all 5 wallets
      for (const w of wallets) {
        await assertWalletConsistent(orm, w.id);
        const recRes = await fetch(
          `${wave2[0]!.baseUrl}/wallets/${w.id}/reconciliation`,
          { method: "POST" },
        );
        expect(recRes.status).toBe(200);
        const rec = (await recRes.json()) as {
          consistent: boolean;
          storedBalance: { amount: string };
          calculatedBalance: { amount: string };
          difference: { amount: string };
        };
        expect(rec.consistent).toBe(true);
        expect(rec.difference.amount).toBe("0.00");
        // Initial 500 - 20 (bet-ref) + 20 (refund) - 50 (bet-reg) + 30 (win-reg) - 15 (sqs-bet) = 465.00
        expect(rec.storedBalance.amount).toBe("465.00");
        expect(rec.calculatedBalance.amount).toBe("465.00");
      }
    } finally {
      await Promise.all(wave2.map((n) => n.kill("SIGTERM")));
    }
  }, 30000);
});
