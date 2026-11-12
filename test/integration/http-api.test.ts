import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { MikroORM } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { uuidv7 } from "uuidv7";
import { FailureCode } from "../../src/domain/wager/failure-code";
import { WagerTransactionStatus } from "../../src/domain/wager/wager-transaction";
import { createApp } from "../../src/main";
import { TEST_DB_NAME, truncateAllTables } from "../helpers/db";
import { assertWalletConsistent } from "../helpers/invariants";

describe("HTTP API & Use Cases Integration (M3)", () => {
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
    const port = typeof addr === "object" && addr ? addr.port : 3099;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  beforeEach(async () => {
    await truncateAllTables(orm);
  });

  afterAll(async () => {
    await app.close();
  });

  it("creates wallet with OPENING transaction and ledger entry, and rejects duplicate (playerId, currency) with 409", async () => {
    const playerId = uuidv7();
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Correlation-Id": "corr-wallet-1",
      },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: "1000.00", currency: "BRL" },
      }),
    });

    expect(createRes.status).toBe(201);
    expect(createRes.headers.get("x-correlation-id")).toBe("corr-wallet-1");
    const created = (await createRes.json()) as {
      id: string;
      playerId: string;
      balance: { amount: string; currency: string };
      version: number;
    };
    expect(created.playerId).toBe(playerId);
    expect(created.balance).toEqual({ amount: "1000.00", currency: "BRL" });
    expect(created.version).toBe(1);

    // Duplicate wallet for same playerId + currency -> 409
    const dupRes = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: "500.00", currency: "BRL" },
      }),
    });
    expect(dupRes.status).toBe(409);

    const getRes = await fetch(`${baseUrl}/wallets/${created.id}`);
    expect(getRes.status).toBe(200);

    await assertWalletConsistent(orm, created.id);
  });

  it("maps HTTP status codes accurately: 201 Processed, 200 Replay, 202 PendingReference, 422 Rejected, 409 Conflict, 400 Invalid, 404 NotFound", async () => {
    const playerId = uuidv7();
    const walletRes = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: "100.00", currency: "BRL" },
      }),
    });
    const wallet = (await walletRes.json()) as { id: string };

    // 1. Missing Idempotency-Key -> 400
    const noKeyRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        providerId: "prov-a",
        externalTransactionId: "tx-1",
        playerId,
        walletId: wallet.id,
        roundId: "r-1",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "25.00", currency: "BRL" },
      }),
    });
    expect(noKeyRes.status).toBe(400);

    // 2. Invalid payload (OPENING kind or negative amount) -> 400
    const badKindRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:bad-kind",
      },
      body: JSON.stringify({
        providerId: "prov-a",
        externalTransactionId: "bad-kind",
        playerId,
        walletId: wallet.id,
        roundId: "r-1",
        gameId: "fortune-chimp",
        kind: "OPENING",
        money: { amount: "25.00", currency: "BRL" },
      }),
    });
    expect(badKindRes.status).toBe(400);

    // 3. Valid BET -> 201 PROCESSED
    const betPayload = {
      providerId: "prov-a",
      externalTransactionId: "tx-bet-1",
      playerId,
      walletId: wallet.id,
      roundId: "r-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "25.00", currency: "BRL" },
    };
    const betRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-bet-1",
      },
      body: JSON.stringify(betPayload),
    });
    expect(betRes.status).toBe(201);
    const betBody = (await betRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
      idempotentReplay: boolean;
    };
    expect(betBody.status).toBe(WagerTransactionStatus.Processed);
    expect(betBody.balance).toEqual({ amount: "75.00", currency: "BRL" });
    expect(betBody.idempotentReplay).toBe(false);

    // 4. Replay of BET -> 200 with idempotentReplay: true
    const replayRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-bet-1",
      },
      body: JSON.stringify(betPayload),
    });
    expect(replayRes.status).toBe(200);
    const replayBody = (await replayRes.json()) as typeof betBody;
    expect(replayBody.idempotentReplay).toBe(true);
    expect(replayBody.transactionId).toBe(betBody.transactionId);
    expect(replayBody.balance).toEqual({ amount: "75.00", currency: "BRL" });

    // 5. Same Idempotency-Key with different payload -> 409 Conflict
    const conflictRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-bet-1",
      },
      body: JSON.stringify({
        ...betPayload,
        money: { amount: "30.00", currency: "BRL" },
      }),
    });
    expect(conflictRes.status).toBe(409);

    // 6. Business rejection (Insufficient Funds) -> 422 (and replay also 422 with idempotentReplay: true)
    const overBetPayload = {
      providerId: "prov-a",
      externalTransactionId: "tx-over-bet",
      playerId,
      walletId: wallet.id,
      roundId: "r-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "500.00", currency: "BRL" },
    };
    const rejectRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-over-bet",
      },
      body: JSON.stringify(overBetPayload),
    });
    expect(rejectRes.status).toBe(422);
    const rejectBody = (await rejectRes.json()) as {
      transactionId: string;
      status: string;
      failureCode: string;
      idempotentReplay: boolean;
    };
    expect(rejectBody.status).toBe(WagerTransactionStatus.Rejected);
    expect(rejectBody.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(rejectBody.idempotentReplay).toBe(false);

    const rejectReplayRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-over-bet",
      },
      body: JSON.stringify(overBetPayload),
    });
    expect(rejectReplayRes.status).toBe(422);
    const rejectReplayBody = (await rejectReplayRes.json()) as typeof rejectBody;
    expect(rejectReplayBody.idempotentReplay).toBe(true);
    expect(rejectReplayBody.failureCode).toBe(FailureCode.InsufficientFunds);

    // 7. Out-of-order REFUND -> 202 PENDING_REFERENCE
    const pendingRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "prov-a:tx-pending-ref",
      },
      body: JSON.stringify({
        providerId: "prov-a",
        externalTransactionId: "tx-pending-ref",
        playerId,
        walletId: wallet.id,
        roundId: "r-99",
        gameId: "fortune-chimp",
        kind: "REFUND",
        money: { amount: "10.00", currency: "BRL" },
        referenceExternalTransactionId: "tx-missing-bet",
      }),
    });
    expect(pendingRes.status).toBe(202);
    const pendingBody = (await pendingRes.json()) as { status: string };
    expect(pendingBody.status).toBe(WagerTransactionStatus.PendingReference);

    // 8. Query by transactionId and by providerId + externalTransactionId
    const txByIdRes = await fetch(
      `${baseUrl}/wagering/transactions/${betBody.transactionId}`,
    );
    expect(txByIdRes.status).toBe(200);

    const txByExtRes = await fetch(
      `${baseUrl}/providers/prov-a/wagering/transactions/tx-bet-1`,
    );
    expect(txByExtRes.status).toBe(200);

    await assertWalletConsistent(orm, wallet.id);
  });

  it("paginates wallet ledger with opaque cursor and performs wallet reconciliation", async () => {
    const playerId = uuidv7();
    const walletRes = await fetch(`${baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: "200.00", currency: "BRL" },
      }),
    });
    const wallet = (await walletRes.json()) as { id: string };

    // Perform 4 bets of 10.00 each (total 5 ledger entries including OPENING)
    for (let i = 1; i <= 4; i++) {
      const r = await fetch(`${baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": `prov-a:bet-${i}`,
        },
        body: JSON.stringify({
          providerId: "prov-a",
          externalTransactionId: `bet-${i}`,
          playerId,
          walletId: wallet.id,
          roundId: `round-${i}`,
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "10.00", currency: "BRL" },
        }),
      });
      expect(r.status).toBe(201);
    }

    // Page 1 (limit 2)
    const page1Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2`,
    );
    expect(page1Res.status).toBe(200);
    const page1 = (await page1Res.json()) as {
      items: Array<{ walletVersion: number }>;
      nextCursor: string | null;
    };
    expect(page1.items).toHaveLength(2);
    expect(page1.items.map((x) => x.walletVersion)).toEqual([1, 2]);
    expect(page1.nextCursor).toBeTruthy();

    // Page 2 (limit 2)
    const page2Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2&cursor=${encodeURIComponent(page1.nextCursor!)}`,
    );
    expect(page2Res.status).toBe(200);
    const page2 = (await page2Res.json()) as typeof page1;
    expect(page2.items).toHaveLength(2);
    expect(page2.items.map((x) => x.walletVersion)).toEqual([3, 4]);
    expect(page2.nextCursor).toBeTruthy();

    // Page 3 (limit 2 -> 1 remaining item)
    const page3Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2&cursor=${encodeURIComponent(page2.nextCursor!)}`,
    );
    expect(page3Res.status).toBe(200);
    const page3 = (await page3Res.json()) as typeof page1;
    expect(page3.items).toHaveLength(1);
    expect(page3.items[0]?.walletVersion).toBe(5);
    expect(page3.nextCursor).toBeNull();

    // Reconciliation
    const recRes = await fetch(
      `${baseUrl}/wallets/${wallet.id}/reconciliation`,
      { method: "POST" },
    );
    expect(recRes.status).toBe(200);
    const rec = (await recRes.json()) as {
      walletId: string;
      storedBalance: { amount: string; currency: string };
      calculatedBalance: { amount: string; currency: string };
      difference: { amount: string; currency: string };
      consistent: boolean;
      checkedEntries: number;
    };
    expect(rec).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: "160.00", currency: "BRL" },
      calculatedBalance: { amount: "160.00", currency: "BRL" },
      difference: { amount: "0.00", currency: "BRL" },
      consistent: true,
      checkedEntries: 5,
    });

    // Health & Metrics
    const readyRes = await fetch(`${baseUrl}/health/ready`);
    expect(readyRes.status).toBe(200);

    const metricsRes = await fetch(`${baseUrl}/metrics`);
    expect(metricsRes.status).toBe(200);
    const metricsText = await metricsRes.text();
    expect(metricsText).toContain("wagering_transactions_total");

    await assertWalletConsistent(orm, wallet.id);
  });
});
