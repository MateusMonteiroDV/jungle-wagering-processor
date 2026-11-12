import { TransactionNotFoundError } from "../../../domain/errors";
import { MoneyProps } from "../../../domain/money/money";
import { FailureCode } from "../../../domain/wager/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../../domain/wager/wager-transaction";
import { UnitOfWork } from "../../ports";

export interface WagerTransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  balance?: MoneyProps;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  createdAt: string;
  processedAt?: string;
}

export class GetTransactionQuery {
  constructor(private readonly uow: UnitOfWork) {}

  async executeById(transactionId: string): Promise<WagerTransactionView> {
    return this.uow.runInTransaction(async (tx) => {
      const found = await tx.transactions.findById(transactionId);
      if (!found) {
        throw new TransactionNotFoundError(transactionId);
      }
      return this.toView(found);
    });
  }

  async executeByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransactionView> {
    return this.uow.runInTransaction(async (tx) => {
      const found = await tx.transactions.findByProviderAndExternalId(
        providerId,
        externalTransactionId,
      );
      if (!found) {
        throw new TransactionNotFoundError(`${providerId}:${externalTransactionId}`);
      }
      return this.toView(found);
    });
  }

  private toView(tx: WagerTransaction): WagerTransactionView {
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      status: tx.status,
      failureCode: tx.failureCode,
      balance: tx.resultingBalance?.toJSON(),
      referenceExternalTransactionId: tx.referenceExternalTransactionId,
      referenceTransactionId: tx.referenceTransactionId,
      createdAt: tx.createdAt.toISOString(),
      processedAt: tx.processedAt?.toISOString(),
    };
  }
}
