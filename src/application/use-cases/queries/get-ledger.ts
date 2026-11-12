import { DomainError, WalletNotFoundError } from "../../../domain/errors";
import { LedgerDirection } from "../../../domain/ledger/wallet-ledger-entry";
import { MoneyProps } from "../../../domain/money/money";
import { decodeLedgerCursor } from "../../cursor";
import { UnitOfWork } from "../../ports";

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  walletVersion: number;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface GetLedgerResult {
  walletId: string;
  items: LedgerEntryView[];
  nextCursor: string | null;
}

export class GetLedgerQuery {
  constructor(private readonly uow: UnitOfWork) {}

  async execute(params: {
    walletId: string;
    cursor?: string;
    limit?: number;
  }): Promise<GetLedgerResult> {
    const limit = params.limit ?? 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      throw new DomainError(
        "VALIDATION_ERROR",
        "limit must be an integer between 1 and 200",
      );
    }

    let afterSeq: bigint | undefined;
    if (params.cursor !== undefined && params.cursor !== "") {
      try {
        afterSeq = decodeLedgerCursor(params.cursor);
      } catch {
        throw new DomainError("VALIDATION_ERROR", "Invalid pagination cursor");
      }
    }

    return this.uow.runInTransaction(async (tx) => {
      const wallet = await tx.wallets.findById(params.walletId);
      if (!wallet) {
        throw new WalletNotFoundError(params.walletId);
      }

      const page = await tx.ledger.listByWallet(
        params.walletId,
        afterSeq,
        limit,
      );

      return {
        walletId: params.walletId,
        items: page.items.map((e) => ({
          id: e.id,
          transactionId: e.transactionId,
          walletVersion: e.walletVersion,
          direction: e.direction,
          money: e.money.toJSON(),
          balanceBefore: e.balanceBefore.toJSON(),
          balanceAfter: e.balanceAfter.toJSON(),
          createdAt: e.createdAt.toISOString(),
        })),
        nextCursor: page.nextCursor ?? null,
      };
    });
  }
}
