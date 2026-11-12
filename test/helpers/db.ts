import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { createMikroOrmOptions } from "../../src/infrastructure/persistence/mikro-orm.config";

export const TEST_DB_NAME = process.env.TEST_DB_NAME ?? "wagering_test";

export async function initTestOrm(dbName = TEST_DB_NAME): Promise<MikroORM> {
  const orm = await MikroORM.init(
    createMikroOrmOptions({
      DB_NAME: dbName,
      LOG_LEVEL: "silent",
    }),
  );
  await orm.getMigrator().up();
  return orm;
}

export async function truncateAllTables(orm: MikroORM): Promise<void> {
  const em = orm.em.fork() as SqlEntityManager;
  await em.execute(`
    TRUNCATE TABLE
      outbox_messages,
      inbox_messages,
      wallet_ledger_entries,
      wager_transactions,
      wallets
    RESTART IDENTITY CASCADE;
  `);
}

export async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  timeoutMs = 10000,
  intervalMs = 50,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) {
        return;
      }
    } catch (err) {
      lastError = err;
    }
    await Bun.sleep(intervalMs);
  }
  if (lastError) {
    throw lastError;
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}
