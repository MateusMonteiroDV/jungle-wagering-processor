import { SqlEntityManager } from "@mikro-orm/postgresql";
import {
  LedgerRepository,
  PaginatedLedgerResult,
  WalletLedgerSummary,
} from "../../../application/ports";
import { WalletLedgerEntry } from "../../../domain/ledger/wallet-ledger-entry";
import { Money } from "../../../domain/money/money";
import { triggerFaultIfActive } from "../../fault-injection";
import { WalletLedgerEntryMapper } from "../mappers";

export function encodeLedgerCursor(seq: bigint): string {
  return Buffer.from(JSON.stringify({ seq: seq.toString() }), "utf8").toString(
    "base64url",
  );
}

export function decodeLedgerCursor(cursor: string): bigint {
  try {
    const raw = Buffer.from(cursor, "base64url").toString("utf8");
    const parsed = JSON.parse(raw) as { seq?: string };
    if (!parsed || typeof parsed.seq !== "string" || !/^\d+$/.test(parsed.seq)) {
      throw new Error("Invalid cursor seq");
    }
    return BigInt(parsed.seq);
  } catch {
    throw new Error("Invalid ledger pagination cursor");
  }
}

export class PgLedgerRepository implements LedgerRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async insert(entry: WalletLedgerEntry): Promise<void> {
    await this.em.execute(
      `INSERT INTO wallet_ledger_entries (
        id, wallet_id, transaction_id, wallet_version, direction,
        amount, currency, balance_before, balance_after, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.walletVersion,
        entry.direction,
        entry.money.amount,
        entry.money.currency,
        entry.balanceBefore.amount,
        entry.balanceAfter.amount,
        entry.createdAt.toISOString(),
      ],
    );
    triggerFaultIfActive("after_ledger_insert");
  }

  async listByWallet(
    walletId: string,
    afterSeq: bigint | undefined,
    limit: number,
  ): Promise<PaginatedLedgerResult> {
    const fetchLimit = limit + 1;
    const rows =
      afterSeq !== undefined
        ? await this.em.execute(
            `SELECT * FROM wallet_ledger_entries
             WHERE wallet_id = ? AND seq > ?
             ORDER BY seq ASC
             LIMIT ?`,
            [walletId, afterSeq.toString(), fetchLimit],
          )
        : await this.em.execute(
            `SELECT * FROM wallet_ledger_entries
             WHERE wallet_id = ?
             ORDER BY seq ASC
             LIMIT ?`,
            [walletId, fetchLimit],
          );

    const hasMore = rows.length > limit;
    const slice = hasMore ? rows.slice(0, limit) : rows;
    const items = slice.map((r) => WalletLedgerEntryMapper.toDomain(r));
    const lastItem = items[items.length - 1];
    const nextCursor =
      hasMore && lastItem?.seq !== undefined
        ? encodeLedgerCursor(lastItem.seq)
        : undefined;

    return { items, nextCursor };
  }

  async calculateWalletSummary(
    walletId: string,
    currency: string,
  ): Promise<WalletLedgerSummary> {
    const aggRows = await this.em.execute(
      `SELECT
         COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)::numeric(20,2) AS net_balance,
         COUNT(*)::int AS checked_entries,
         COALESCE(MAX(wallet_version), 1)::int AS max_wallet_version
       FROM wallet_ledger_entries
       WHERE wallet_id = ?`,
      [walletId],
    );

    const agg = aggRows[0] as Record<string, unknown>;
    const netBalanceStr = String(agg?.net_balance ?? "0.00");
    const checkedEntries = parseInt(String(agg?.checked_entries ?? 0), 10);
    const maxWalletVersion = parseInt(String(agg?.max_wallet_version ?? 1), 10);

    const lastRows = await this.em.execute(
      `SELECT balance_after
       FROM wallet_ledger_entries
       WHERE wallet_id = ?
       ORDER BY wallet_version DESC, seq DESC
       LIMIT 1`,
      [walletId],
    );
    const lastRow = lastRows[0] as Record<string, unknown> | undefined;
    const lastBalanceAfter = lastRow
      ? Money.fromPersistence(String(lastRow.balance_after), currency)
      : undefined;

    return {
      calculatedBalance: Money.fromPersistence(netBalanceStr, currency),
      checkedEntries,
      lastBalanceAfter,
      maxWalletVersion,
    };
  }
}
