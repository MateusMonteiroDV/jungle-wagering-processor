import {
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
} from "@aws-sdk/client-sqs";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { uuidv7 } from "uuidv7";
import { TOKENS, UnitOfWork } from "../../src/application/ports";
import { CreateWalletUseCase } from "../../src/application/use-cases/create-wallet";
import { ProcessWagerTransactionUseCase } from "../../src/application/use-cases/process-wager-transaction";
import { PublishOutboxUseCase } from "../../src/application/use-cases/publish-outbox";
import { ResolvePendingReferencesUseCase } from "../../src/application/use-cases/resolve-pending-references";
import { FailureCode } from "../../src/domain/wager/failure-code";
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wager/wager-transaction";
import { SqsQueueManager } from "../../src/infrastructure/messaging/sqs-client";
import { SqsEventPublisher } from "../../src/infrastructure/messaging/sqs-event-publisher";
import { PrometheusMetrics } from "../../src/infrastructure/observability/metrics";
import { DlqMetricsWorker } from "../../src/infrastructure/workers/dlq-metrics.worker";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables, waitFor } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

describe("Transactional Outbox Publisher, Pending-Reference Worker & DLQ Metrics (M4)", () => {
  let app: INestApplication;
  let orm: MikroORM;
  let sqs: SqsQueueManager;
  let createWallet: CreateWalletUseCase;
  let processWager: ProcessWagerTransactionUseCase;
  let publishOutbox: PublishOutboxUseCase;
  let resolvePending: ResolvePendingReferencesUseCase;
  let dlqWorker: DlqMetricsWorker;
  let metrics: PrometheusMetrics;

  const runSuffix = uuidv7().slice(-8);
  const wagerQueueName = `wager-m4-${runSuffix}.fifo`;
  const dlqName = `wager-m4-dlq-${runSuffix}.fifo`;
  const eventsQueueName = `wallet-events-m4-${runSuffix}.fifo`;

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
      PENDING_REF_BASE_DELAY_MS: "20",
      PENDING_REF_MAX_ATTEMPTS: "3",
      PENDING_REF_TTL_MS: "5000",
    });
    await app.init();

    orm = app.get(MikroORM);
    await orm.getMigrator().up();
    sqs = app.get(SqsQueueManager);
    await sqs.ensureQueues();

    createWallet = app.get(CreateWalletUseCase);
    processWager = app.get(ProcessWagerTransactionUseCase);
    publishOutbox = app.get(PublishOutboxUseCase);
    resolvePending = app.get(ResolvePendingReferencesUseCase);
    dlqWorker = app.get(DlqMetricsWorker);
    metrics = app.get(PrometheusMetrics);
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
      // ignore cleanup errors
    }
    await app.close();
  });

  it("publishes outbox messages concurrently across multiple publishers and recovers expired leases", async () => {
    const wallet = await createWallet.execute({
      playerId: uuidv7(),
      initialBalance: { amount: "500.00", currency: "BRL" },
      correlationId: "corr-outbox-1",
    });

    // Generate 10 BET transactions -> 1 (opening) + 10*2 (processed + balance) = 21 outbox messages
    for (let i = 1; i <= 10; i++) {
      await processWager.execute(
        {
          providerId: "prov-a",
          externalTransactionId: `bet-outbox-${i}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `round-${i}`,
          gameId: "chimp-slot",
          kind: WagerTransactionKind.Bet,
          money: { amount: "10.00", currency: "BRL" },
        },
        {
          source: "HTTP",
          idempotencyKey: `prov-a:bet-outbox-${i}`,
          correlationId: `corr-${i}`,
        },
      );
    }

    // Simulate one message having an expired lease from a crashed instance
    const em = orm.em.fork() as SqlEntityManager;
    await em.execute(
      `UPDATE outbox_messages
       SET locked_by = 'dead-instance',
           locked_until = now() - interval '10 seconds'
       WHERE seq = 1`,
    );

    const uow = app.get<UnitOfWork>(TOKENS.UnitOfWork);
    const publisherA = new PublishOutboxUseCase(
      uow,
      new SqsEventPublisher(sqs),
      { now: () => new Date() },
      metrics,
      { instanceId: "pub-a", batchSize: 5, leaseDurationSec: 30 },
    );
    const publisherB = new PublishOutboxUseCase(
      uow,
      new SqsEventPublisher(sqs),
      { now: () => new Date() },
      metrics,
      { instanceId: "pub-b", batchSize: 5, leaseDurationSec: 30 },
    );

    // Run both publishers in parallel until all 21 outbox rows are published
    await waitFor(async () => {
      await Promise.all([publisherA.executeBatch(), publisherB.executeBatch()]);
      const rows = await em.execute(
        `SELECT COUNT(*)::int AS unpublished FROM outbox_messages WHERE published_at IS NULL`,
      );
      const unpublished = parseInt(
        String((rows[0] as Record<string, unknown>)?.unpublished ?? 0),
        10,
      );
      return unpublished === 0;
    }, 10000);

    // Verify all 21 outbox rows have published_at set and locked_by cleared
    const publishedRows = await em.execute(
      `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NOT NULL AND locked_by IS NULL`,
    );
    expect(
      parseInt(String((publishedRows[0] as Record<string, unknown>)?.cnt ?? 0), 10),
    ).toBe(21);

    // Receive from SQS events queue to confirm real SQS delivery
    const eventsUrl = await sqs.getEventsQueueUrl();
    const receivedEventIds = new Set<string>();
    for (let i = 0; i < 6 && receivedEventIds.size < 21; i++) {
      const res = await sqs.client.send(
        new ReceiveMessageCommand({
          QueueUrl: eventsUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
        }),
      );
      for (const m of res.Messages ?? []) {
        const body = JSON.parse(m.Body!) as { eventId: string; eventType: string };
        expect(body.eventId).toBeTruthy();
        expect(body.eventType).toBeTruthy();
        receivedEventIds.add(body.eventId);
        await sqs.client.send(
          new DeleteMessageCommand({
            QueueUrl: eventsUrl,
            ReceiptHandle: m.ReceiptHandle!,
          }),
        );
      }
    }
    expect(receivedEventIds.size).toBe(21);

    await assertWalletConsistent(orm, wallet.id);
  });

  it("resolves out-of-order REFUND via PendingReferenceWorker when BET arrives, and rejects with REFERENCE_NOT_FOUND after maxAttempts", async () => {
    const wallet = await createWallet.execute({
      playerId: uuidv7(),
      initialBalance: { amount: "100.00", currency: "BRL" },
      correlationId: "corr-pending-1",
    });

    // 1. Out-of-order REFUND arrives before BET -> PENDING_REFERENCE
    const earlyRefund = await processWager.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "refund-before-bet",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-oof",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Refund,
        money: { amount: "30.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-delayed",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:refund-before-bet",
        correlationId: "corr-oof-1",
      },
    );
    expect(earlyRefund.status).toBe(WagerTransactionStatus.PendingReference);

    // 2. Another REFUND referencing a BET that will NEVER arrive
    const orphanRefund = await processWager.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "refund-orphan",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-orphan",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Refund,
        money: { amount: "15.00", currency: "BRL" },
        referenceExternalTransactionId: "bet-never-sent",
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:refund-orphan",
        correlationId: "corr-orphan",
      },
    );
    expect(orphanRefund.status).toBe(WagerTransactionStatus.PendingReference);

    // 3. Now the delayed BET arrives -> wallet balance becomes 70.00
    const delayedBet = await processWager.execute(
      {
        providerId: "prov-a",
        externalTransactionId: "bet-delayed",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-oof",
        gameId: "chimp-slot",
        kind: WagerTransactionKind.Bet,
        money: { amount: "30.00", currency: "BRL" },
      },
      {
        source: "HTTP",
        idempotencyKey: "prov-a:bet-delayed",
        correlationId: "corr-oof-2",
      },
    );
    expect(delayedBet.status).toBe(WagerTransactionStatus.Processed);
    expect(delayedBet.balance.amount).toBe("70.00");

    // 4. Run PendingReferenceWorker batches until earlyRefund is PROCESSED and orphanRefund is REJECTED
    const em = orm.em.fork() as SqlEntityManager;
    await waitFor(async () => {
      // Force next_attempt_at to now so we don't wait on backoff delays in test
      await em.execute(
        `UPDATE wager_transactions SET next_attempt_at = now() - interval '1 second' WHERE status = 'PENDING_REFERENCE'`,
      );
      await resolvePending.executeBatch();

      const rows = await em.execute(
        `SELECT id, status, failure_code FROM wager_transactions WHERE id IN (?, ?)`,
        [earlyRefund.transactionId, orphanRefund.transactionId],
      );
      const map = new Map(
        rows.map((r: Record<string, unknown>) => [String(r.id), r]),
      );
      const earlyRow = map.get(earlyRefund.transactionId);
      const orphanRow = map.get(orphanRefund.transactionId);
      return (
        earlyRow?.status === WagerTransactionStatus.Processed &&
        orphanRow?.status === WagerTransactionStatus.Rejected &&
        orphanRow?.failure_code === FailureCode.ReferenceNotFound
      );
    }, 10000);

    // Verify DLQ metrics worker tick works cleanly
    await dlqWorker.tick();
    const metricsText = await metrics.getMetricsText();
    expect(metricsText).toContain("wagering_dlq_depth");

    await assertWalletConsistent(orm, wallet.id);
  });
});
