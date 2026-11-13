import { MikroORM } from "@mikro-orm/postgresql";
import { uuidv7 } from "uuidv7";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[idx]!;
}

async function main() {
  console.log("=== Jungle Wagering Processor — Load Test ===");
  const app = await createApp({
    DB_NAME: TEST_DB_NAME,
    LOG_LEVEL: "silent",
    DB_POOL_MAX: "30",
    RUN_CONSUMER: "false",
    RUN_OUTBOX: "true",
    RUN_PENDING: "true",
    RUN_DLQ_METRICS: "false",
    OUTBOX_POLL_INTERVAL_MS: "100",
  });
  const orm = app.get<MikroORM>(MikroORM);
  await orm.getMigrator().up();
  await truncateAllTables(orm);

  await app.listen(0, "127.0.0.1");
  const addr = app.getHttpServer().address();
  const port = typeof addr === "object" && addr ? addr.port : 3095;
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    const walletCount = 25;
    const opsPerWallet = 20;
    const concurrency = 25;

    console.log(`Seeding ${walletCount} wallets...`);
    const wallets = await Promise.all(
      Array.from({ length: walletCount }, async () => {
        const res = await fetch(`${baseUrl}/wallets`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            playerId: uuidv7(),
            initialBalance: { amount: "1000.00", currency: "BRL" },
          }),
        });
        return (await res.json()) as { id: string; playerId: string };
      }),
    );

    const tasks: Array<() => Promise<{ status: number; latencyMs: number }>> = [];
    for (let wIdx = 0; wIdx < wallets.length; wIdx++) {
      const w = wallets[wIdx]!;
      for (let opIdx = 0; opIdx < opsPerWallet; opIdx++) {
        const extId = `load-w${wIdx}-op${opIdx}`;
        const kind = opIdx % 4 === 3 ? "WIN" : opIdx % 5 === 4 ? "LOSS" : "BET";
        const amount = kind === "LOSS" ? "0.00" : "10.00";
        tasks.push(async () => {
          const t0 = performance.now();
          const res = await fetch(`${baseUrl}/wagering/transactions`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Idempotency-Key": `prov-load:${extId}`,
            },
            body: JSON.stringify({
              providerId: "prov-load",
              externalTransactionId: extId,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `round-${wIdx}-${opIdx}`,
              gameId: "fortune-chimp",
              kind,
              money: { amount, currency: "BRL" },
            }),
          });
          await res.json();
          const latencyMs = performance.now() - t0;
          return { status: res.status, latencyMs };
        });
      }
    }

    console.log(
      `Executing ${tasks.length} wager transactions across ${walletCount} wallets (concurrency=${concurrency})...`,
    );

    const results: Array<{ status: number; latencyMs: number }> = [];
    let cursor = 0;
    const startWall = performance.now();

    const workers = Array.from({ length: concurrency }, async () => {
      while (true) {
        const idx = cursor++;
        if (idx >= tasks.length) break;
        const r = await tasks[idx]!();
        results.push(r);
      }
    });
    await Promise.all(workers);

    const elapsedSec = (performance.now() - startWall) / 1000;
    const latencies = results.map((r) => r.latencyMs).sort((a, b) => a - b);
    const errors5xx = results.filter((r) => r.status >= 500).length;
    const rps = results.length / elapsedSec;

    // Verify 100% reconciliation and invariant consistency
    for (const w of wallets) {
      await assertWalletConsistent(orm, w.id);
    }

    const metricsRes = await fetch(`${baseUrl}/metrics`);
    const metricsText = await metricsRes.text();
    const outboxLagMatch = metricsText.match(
      /^wagering_outbox_lag_seconds\s+([0-9.]+)/m,
    );
    const lockConflictMatch = metricsText.match(
      /^wagering_lock_conflicts_total\s+([0-9.]+)/m,
    );

    const summary = {
      totalRequests: results.length,
      wallets: walletCount,
      concurrency,
      elapsedSeconds: Number(elapsedSec.toFixed(3)),
      throughputRps: Number(rps.toFixed(2)),
      latencyMs: {
        p50: Number(percentile(latencies, 50).toFixed(2)),
        p95: Number(percentile(latencies, 95).toFixed(2)),
        p99: Number(percentile(latencies, 99).toFixed(2)),
      },
      errorRate5xx: errors5xx / results.length,
      lockConflicts: Number(lockConflictMatch?.[1] ?? 0),
      outboxLagSeconds: Number(outboxLagMatch?.[1] ?? 0),
      allWalletsConsistent: true,
    };

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await app.close();
    process.exit(0);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
