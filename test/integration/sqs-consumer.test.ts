import {
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { uuidv7 } from "uuidv7";
import { CreateWalletUseCase } from "../../src/application/use-cases/create-wallet";
import { setFaultInjection } from "../../src/infrastructure/fault-injection";
import { SqsQueueManager } from "../../src/infrastructure/messaging/sqs-client";
import { WagerTransactionsConsumer } from "../../src/infrastructure/messaging/wager-transactions.consumer";
import { PrometheusMetrics } from "../../src/infrastructure/observability/metrics";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

describe("SQS Consumer, Persistent Inbox Deduplication, Retry & DLQ (M5)", () => {
  let app: INestApplication;
  let orm: MikroORM;
  let sqs: SqsQueueManager;
  let consumer: WagerTransactionsConsumer;
  let createWallet: CreateWalletUseCase;
  let metrics: PrometheusMetrics;

  const runSuffix = uuidv7().slice(-8);
  const wagerQueueName = `wager-m5-${runSuffix}.fifo`;
  const dlqName = `wager-m5-dlq-${runSuffix}.fifo`;
  const eventsQueueName = `wallet-events-m5-${runSuffix}.fifo`;

  beforeAll(async () => {
    app = await createApp({
      DB_NAME: TEST_DB_NAME,
      LOG_LEVEL: "silent",
      RUN_HTTP: "false",
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
      RUN_DLQ_METRICS: "false",
      SQS_WAGER_QUEUE_NAME: wagerQueueName,
      SQS_WAGER_DLQ_NAME: dlqName,
      SQS_EVENTS_QUEUE_NAME: eventsQueueName,
      SQS_VISIBILITY_TIMEOUT_SEC: "2",
      SQS_MAX_RECEIVE_COUNT: "3",
      SQS_RETRY_BASE_SEC: "0",
      SQS_WAIT_TIME_SEC: "0",
    });
    await app.init();

    orm = app.get(MikroORM);
    await orm.getMigrator().up();
    sqs = app.get(SqsQueueManager);
    await sqs.ensureQueues();

    consumer = app.get(WagerTransactionsConsumer);
    createWallet = app.get(CreateWalletUseCase);
    metrics = app.get(PrometheusMetrics);
  });

  beforeEach(async () => {
    setFaultInjection(undefined);
    await truncateAllTables(orm);
  });

  afterAll(async () => {
    setFaultInjection(undefined);
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
      // ignore cleanup errors
    }
    await app.close();
  });

  async function drainDlq(): Promise<
    Array<{ body: string; failureReason?: string }>
  > {
    const dlqUrl = await sqs.getDlqUrl();
    const out: Array<{ body: string; failureReason?: string }> = [];
    for (let i = 0; i < 5; i++) {
      const res = await sqs.client.send(
        new ReceiveMessageCommand({
          QueueUrl: dlqUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
          MessageAttributeNames: ["All"],
        }),
      );
      if (!res.Messages || res.Messages.length === 0) break;
      for (const m of res.Messages) {
        out.push({
          body: m.Body ?? "",
          failureReason: m.MessageAttributes?.failureReason?.StringValue,
        });
        await sqs.client.send(
          new DeleteMessageCommand({
            QueueUrl: dlqUrl,
            ReceiptHandle: m.ReceiptHandle!,
          }),
        );
      }
    }
    return out;
  }

  it("consumes SQS message, deduplicates redelivery via persistent inbox, and sends conflicting messageId to DLQ", async () => {
    const wallet = await createWallet.execute({
      playerId: uuidv7(),
      initialBalance: { amount: "100.00", currency: "BRL" },
      correlationId: "corr-sqs-1",
    });

    const queueUrl = await sqs.getWagerQueueUrl();
    const messagePayload = {
      messageId: "msg-sqs-bet-1",
      type: "WagerTransactionRequested",
      occurredAt: "2026-07-29T15:00:00.000Z",
      data: {
        providerId: "provider-a",
        externalTransactionId: "sqs-bet-1",
        idempotencyKey: "provider-a:sqs-bet-1",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-sqs-1",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "25.00", currency: "BRL" },
      },
    };

    // 1. Send message to SQS
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(messagePayload),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-1",
      }),
    );

    const processedCount = await consumer.pollOnce(1);
    expect(processedCount).toBe(1);

    // Check DB: wallet balance = 75.00, 1 inbox row with processed_at != null
    await assertWalletConsistent(orm, wallet.id);
    const em = orm.em.fork() as SqlEntityManager;
    const inboxRows = await em.execute(
      `SELECT * FROM inbox_messages WHERE message_id = 'msg-sqs-bet-1'`,
    );
    expect(inboxRows).toHaveLength(1);
    expect((inboxRows[0] as Record<string, unknown>).processed_at).not.toBeNull();

    // 2. Simulate SQS redelivery of the exact same messageId (with new SQS dedup id)
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(messagePayload),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-2-redelivery",
      }),
    );

    const redeliveryCount = await consumer.pollOnce(1);
    expect(redeliveryCount).toBe(1);

    // Balance must still be 75.00 (no double debit!)
    const walletRows = await em.execute(
      `SELECT balance FROM wallets WHERE id = ?`,
      [wallet.id],
    );
    expect(String((walletRows[0] as Record<string, unknown>).balance)).toBe(
      "75.00",
    );
    await assertWalletConsistent(orm, wallet.id);

    // 3. Send same messageId with a DIFFERENT payload -> must go to DLQ with INBOX_PAYLOAD_CONFLICT
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          ...messagePayload,
          data: {
            ...messagePayload.data,
            money: { amount: "90.00", currency: "BRL" },
          },
        }),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-3-conflict",
      }),
    );

    await consumer.pollOnce(1);
    const dlqMsgs = await drainDlq();
    expect(dlqMsgs).toHaveLength(1);
    expect(dlqMsgs[0]?.failureReason).toBe("INBOX_PAYLOAD_CONFLICT");

    await assertWalletConsistent(orm, wallet.id);
  });

  it("routes malformed JSON and OPENING kind messages directly to DLQ, and retries transient errors up to maxReceiveCount before DLQ", async () => {
    await drainDlq();
    const wallet = await createWallet.execute({
      playerId: uuidv7(),
      initialBalance: { amount: "100.00", currency: "BRL" },
      correlationId: "corr-sqs-dlq",
    });

    const queueUrl = await sqs.getWagerQueueUrl();

    // 1. Malformed JSON -> straight to DLQ
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: "{not-valid-json",
        MessageGroupId: "group-bad-json",
        MessageDeduplicationId: "dedup-bad-json",
      }),
    );
    await consumer.pollOnce(1);

    // 2. Forbidden OPENING kind -> straight to DLQ
    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          messageId: "msg-opening-forbidden",
          type: "WagerTransactionRequested",
          occurredAt: "2026-07-29T15:00:00.000Z",
          data: {
            providerId: "provider-a",
            externalTransactionId: "ext-open",
            idempotencyKey: "provider-a:ext-open",
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "r-1",
            gameId: "g-1",
            kind: "OPENING",
            money: { amount: "50.00", currency: "BRL" },
          },
        }),
        MessageGroupId: "group-opening",
        MessageDeduplicationId: "dedup-opening",
      }),
    );
    await consumer.pollOnce(1);

    const permanentDlq = await drainDlq();
    expect(permanentDlq.map((m) => m.failureReason).sort()).toEqual([
      "MALFORMED_JSON",
      "OPENING_KIND_FORBIDDEN",
    ]);

    // 3. Transient error -> retries 3 times (SQS_MAX_RECEIVE_COUNT=3) then routes to DLQ
    setFaultInjection("transient_sqs_consume");

    await sqs.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          messageId: "msg-transient-dlq",
          type: "WagerTransactionRequested",
          occurredAt: "2026-07-29T15:00:00.000Z",
          data: {
            providerId: "provider-a",
            externalTransactionId: "ext-transient",
            idempotencyKey: "provider-a:ext-transient",
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "r-1",
            gameId: "g-1",
            kind: "BET",
            money: { amount: "10.00", currency: "BRL" },
          },
        }),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: "dedup-transient",
      }),
    );

    // Poll 3 times (visibility timeout is set to 0s on retry because SQS_RETRY_BASE_SEC=0)
    for (let attempt = 1; attempt <= 3; attempt++) {
      await consumer.pollOnce(1);
    }

    setFaultInjection(undefined);

    const transientDlq = await drainDlq();
    expect(transientDlq).toHaveLength(1);
    expect(transientDlq[0]?.failureReason).toBe("MAX_RECEIVE_COUNT_EXCEEDED");

    const metricsText = await metrics.getMetricsText();
    expect(metricsText).toContain("wagering_dlq_messages_total");
    expect(metricsText).toContain("wagering_retries_total");

    await assertWalletConsistent(orm, wallet.id);
  });
});
