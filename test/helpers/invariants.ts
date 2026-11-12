import { expect } from "bun:test";
import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import { Money } from "../../src/domain/money/money";
import {
  PgLedgerRepository,
  PgWalletRepository,
} from "../../src/infrastructure/persistence/repositories";

export async function assertWalletConsistent(
  orm: MikroORM,
  walletId: string,
): Promise<void> {
  const em = orm.em.fork({ clear: true }) as SqlEntityManager;
  const walletRepo = new PgWalletRepository(em);
  const ledgerRepo = new PgLedgerRepository(em);

  const wallet = await walletRepo.findById(walletId);
  expect(wallet).not.toBeNull();
  if (!wallet) return;

  const summary = await ledgerRepo.calculateWalletSummary(
    walletId,
    wallet.currency,
  );
  const { items: entries } = await ledgerRepo.listByWallet(
    walletId,
    undefined,
    10000,
  );

  // 1. Stored balance == reconstructed ledger sum
  expect(wallet.balance.equals(summary.calculatedBalance)).toBe(true);

  // 2. Every individual ledger entry is balanced and sequential
  let runningBalance = Money.zero(wallet.currency);
  let expectedVersion = wallet.balance.isZero() && entries.length === 0 ? 1 : 1;

  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i]!;
    expect(entry.isBalanced()).toBe(true);
    expect(entry.balanceBefore.equals(runningBalance)).toBe(true);
    runningBalance = entry.balanceAfter;
    if (i === 0) {
      expectedVersion = entry.walletVersion;
    } else {
      expectedVersion += 1;
      expect(entry.walletVersion).toBe(expectedVersion);
    }
  }

  expect(runningBalance.equals(wallet.balance)).toBe(true);

  if (entries.length > 0) {
    expect(summary.lastBalanceAfter?.equals(wallet.balance)).toBe(true);
    expect(summary.maxWalletVersion).toBe(wallet.version);
  } else {
    expect(wallet.balance.isZero()).toBe(true);
    expect(wallet.version).toBe(1);
  }

  // 3. Number of ledger entries == number of PROCESSED transactions affecting balance
  const txCountRows = await em.execute(
    `SELECT COUNT(*)::int AS cnt
     FROM wager_transactions
     WHERE wallet_id = ?
       AND status = 'PROCESSED'
       AND kind <> 'LOSS'`,
    [walletId],
  );
  const processedBalanceAffectingCount = parseInt(
    String((txCountRows[0] as Record<string, unknown>)?.cnt ?? 0),
    10,
  );
  expect(entries.length).toBe(processedBalanceAffectingCount);
}
