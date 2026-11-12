import { Writable } from "stream";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { uuidv7 } from "uuidv7";
import {
  runWithLogContext,
  updateLogContext,
} from "../../src/infrastructure/observability/context";
import { AppLogger } from "../../src/infrastructure/observability/logger";
import { PrometheusMetrics } from "../../src/infrastructure/observability/metrics";
import { WagerTransactionKind, WagerTransactionStatus } from "../../src/domain/wager/wager-transaction";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables } from "../helpers/db";

describe("Observability: Structured JSON Logs, Prometheus Metrics & Health Probes (M6)", () => {
  let app: INestApplication;
  let orm: MikroORM;
  let baseUrl: string;

  beforeAll(async () => {
    app = await createApp({
      DB_NAME: TEST_DB_NAME,
      LOG_LEVEL: "silent",
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
      RUN_DLQ_METRICS: "false",
    });
    orm = app.get(MikroORM);
    await orm.getMigrator().up();
    await app.listen(0, "127.0.0.1");
    const addr = app.getHttpServer().address();
    const port = typeof addr === "object" && addr ? addr.port : 3098;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  beforeEach(async () => {
    await truncateAllTables(orm);
  });

  afterAll(async () => {
    await app.close();
  });

  it("emits structured JSON logs with correlationId/messageId/transactionId/walletId/providerId and redacts sensitive financial fields", () => {
    const lines: string[] = [];
    const dest = new Writable({
      write(chunk, _encoding, callback) {
        lines.push(chunk.toString("utf8").trim());
        callback();
      },
    });

    const logger = new AppLogger("info", "test-instance-1", dest);

    runWithLogContext(
      {
        correlationId: "corr-xyz",
        messageId: "msg-xyz",
      },
      () => {
        updateLogContext({
          walletId: "wallet-123",
          transactionId: "tx-456",
          providerId: "provider-abc",
        });

        logger.info(
          {
            status: "PROCESSED",
            amount: "999.99",
            balance: { amount: "5000.00", currency: "BRL" },
            playerId: "secret-player-uuid",
            payload: { raw: "sensitive" },
          },
          "Transaction completed",
        );
      },
    );

    expect(lines).toHaveLength(1);
    const parsed = JSON.parse(lines[0]!) as Record<string, unknown>;
    expect(parsed.level).toBe("info");
    expect(parsed.instanceId).toBe("test-instance-1");
    expect(parsed.correlationId).toBe("corr-xyz");
    expect(parsed.messageId).toBe("msg-xyz");
    expect(parsed.walletId).toBe("wallet-123");
    expect(parsed.transactionId).toBe("tx-456");
    expect(parsed.providerId).toBe("provider-abc");
    expect(parsed.status).toBe("PROCESSED");

    // Sensitive & financial fields must be redacted
    expect(parsed.amount).toBeUndefined();
    expect(parsed.balance).toBeUndefined();
    expect(parsed.playerId).toBeUndefined();
    expect(parsed.payload).toBeUndefined();
  });

  it("exposes all required Prometheus metrics under GET /metrics", async () => {
    const metrics = app.get(PrometheusMetrics);
    metrics.incrementTransaction(
      WagerTransactionStatus.Processed,
      WagerTransactionKind.Bet,
      "HTTP",
    );
    metrics.incrementIdempotentReplay("HTTP");
    metrics.incrementInboxDuplicate();
    metrics.incrementRetry("sqs_transient");
    metrics.incrementDlqMessage("MALFORMED_JSON");
    metrics.setDlqDepth(0);
    metrics.incrementLockConflict();
    metrics.setOutboxLagSeconds(0.15);
    metrics.incrementOutboxPublished("WalletBalanceChanged");
    metrics.incrementOutboxPublishFailure();
    metrics.observeProcessingDuration("HTTP", 0.012);
    metrics.incrementPendingReference();
    metrics.incrementReconciliationMismatch();

    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
    const body = await res.text();

    const expectedMetrics = [
      "wagering_transactions_total",
      "wagering_idempotent_replays_total",
      "wagering_inbox_duplicates_total",
      "wagering_retries_total",
      "wagering_dlq_messages_total",
      "wagering_dlq_depth",
      "wagering_lock_conflicts_total",
      "wagering_outbox_lag_seconds",
      "wagering_outbox_published_total",
      "wagering_outbox_publish_failures_total",
      "wagering_processing_duration_seconds_bucket",
      "wagering_pending_reference_total",
      "wagering_reconciliation_mismatches_total",
    ];

    for (const metricName of expectedMetrics) {
      expect(body).toContain(metricName);
    }
  });

  it("separates /health/live and /health/ready and returns 503 when a dependency is unreachable", async () => {
    const liveRes = await fetch(`${baseUrl}/health/live`);
    expect(liveRes.status).toBe(200);

    const readyRes = await fetch(`${baseUrl}/health/ready`);
    expect(readyRes.status).toBe(200);
    const readyJson = (await readyRes.json()) as {
      status: string;
      dependencies: { postgres: string; sqs: string };
    };
    expect(readyJson.status).toBe("ready");
    expect(readyJson.dependencies).toEqual({ postgres: "up", sqs: "up" });

    // Create an app with unreachable SQS endpoint -> /health/live stays 200, /health/ready returns 503
    const degradedApp = await createApp({
      DB_NAME: TEST_DB_NAME,
      LOG_LEVEL: "silent",
      SQS_ENDPOINT: "http://127.0.0.1:59999",
      SQS_WAGER_QUEUE_NAME: `missing-${uuidv7()}.fifo`,
      RUN_CONSUMER: "false",
      RUN_OUTBOX: "false",
      RUN_PENDING: "false",
      RUN_DLQ_METRICS: "false",
    });
    await degradedApp.listen(0, "127.0.0.1");
    const degAddr = degradedApp.getHttpServer().address();
    const degPort = typeof degAddr === "object" && degAddr ? degAddr.port : 3097;
    const degUrl = `http://127.0.0.1:${degPort}`;

    try {
      const degLive = await fetch(`${degUrl}/health/live`);
      expect(degLive.status).toBe(200);

      const degReady = await fetch(`${degUrl}/health/ready`);
      expect(degReady.status).toBe(503);
      const degBody = (await degReady.json()) as {
        status: string;
        dependencies: { postgres: string; sqs: string };
      };
      expect(degBody.status).toBe("degraded");
      expect(degBody.dependencies.postgres).toBe("up");
      expect(degBody.dependencies.sqs).toBe("down");
    } finally {
      await degradedApp.close();
    }
  });
});
