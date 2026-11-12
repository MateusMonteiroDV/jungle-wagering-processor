import { WalletNotFoundError } from "../../domain/errors";
import { MoneyProps } from "../../domain/money/money";
import { MetricsPort, UnitOfWork } from "../ports";

export interface ReconcileWalletResult {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

export interface ReconciliationLogger {
  error(meta: Record<string, unknown>, message: string): void;
}

export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly metrics: MetricsPort,
    private readonly logger?: ReconciliationLogger,
  ) {}

  async execute(walletId: string): Promise<ReconcileWalletResult> {
    return this.uow.runInTransaction(async (tx) => {
      const wallet = await tx.wallets.findByIdForShare(walletId);
      if (!wallet) {
        throw new WalletNotFoundError(walletId);
      }

      const summary = await tx.ledger.calculateWalletSummary(
        walletId,
        wallet.currency,
      );
      const storedBalance = wallet.balance;
      const calculatedBalance = summary.calculatedBalance;
      const difference = storedBalance.subtract(calculatedBalance);

      const lastBalanceMatches =
        summary.checkedEntries === 0
          ? storedBalance.isZero()
          : Boolean(summary.lastBalanceAfter?.equals(storedBalance));

      const versionMatches =
        summary.checkedEntries === 0
          ? wallet.version === 1
          : summary.maxWalletVersion === wallet.version;

      const consistent =
        difference.isZero() && lastBalanceMatches && versionMatches;

      if (!consistent) {
        this.metrics.incrementReconciliationMismatch();
        this.logger?.error(
          {
            walletId,
            consistent,
            checkedEntries: summary.checkedEntries,
            walletVersion: wallet.version,
            maxLedgerVersion: summary.maxWalletVersion,
          },
          "Wallet reconciliation divergence detected",
        );
      }

      return {
        walletId: wallet.id,
        storedBalance: storedBalance.toJSON(),
        calculatedBalance: calculatedBalance.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries: summary.checkedEntries,
      };
    });
  }
}
