import {
  DomainError,
  InvalidMoneyError,
  InvalidReferenceError,
  InvalidTransactionStateError,
} from "../errors";
import { LedgerDirection } from "../ledger/wallet-ledger-entry";
import { Money } from "../money/money";
import { FailureCode } from "./failure-code";

export enum WagerTransactionKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
  Failed = "FAILED",
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  createdAt?: Date;
}

export interface CreateOpeningTransactionProps {
  id: string;
  walletId: string;
  playerId: string;
  money: Money;
  payloadHash: string;
  correlationId?: string;
  createdAt?: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  resultingBalance?: Money;
  attempts: number;
  nextAttemptAt?: Date;
  correlationId?: string;
  createdAt: Date;
  updatedAt: Date;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _resultingBalance?: Money,
    private _attempts: number = 0,
    private _nextAttemptAt?: Date,
    private _updatedAt: Date = createdAt,
    public readonly correlationId?: string,
  ) {}

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) {
      throw new DomainError(
        "OPENING_KIND_FORBIDDEN",
        "OPENING transactions cannot be submitted externally",
      );
    }

    if (
      (props.kind === WagerTransactionKind.Refund ||
        props.kind === WagerTransactionKind.Rollback) &&
      (!props.referenceExternalTransactionId ||
        props.referenceExternalTransactionId.trim() === "")
    ) {
      throw new InvalidReferenceError(
        `Transaction kind ${props.kind} requires referenceExternalTransactionId`,
      );
    }

    if (props.kind !== WagerTransactionKind.Loss && !props.money.isPositive()) {
      throw new InvalidMoneyError(
        `Transaction kind ${props.kind} requires a strictly positive amount`,
      );
    }

    if (props.money.isNegative()) {
      throw new InvalidMoneyError("Transaction amount cannot be negative");
    }

    const now = props.createdAt ?? new Date();
    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      now,
      WagerTransactionStatus.Pending,
      undefined,
      undefined,
      undefined,
      undefined,
      0,
      undefined,
      now,
      props.correlationId,
    );
  }

  static createOpening(props: CreateOpeningTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvalidMoneyError("Opening transaction requires a positive initial balance");
    }
    const now = props.createdAt ?? new Date();
    const externalId = `opening:${props.walletId}`;
    return new WagerTransaction(
      props.id,
      "internal",
      externalId,
      externalId,
      props.payloadHash,
      props.walletId,
      props.playerId,
      "opening",
      "opening",
      WagerTransactionKind.Opening,
      props.money,
      undefined,
      now,
      WagerTransactionStatus.Processed,
      undefined,
      undefined,
      now,
      props.money,
      0,
      undefined,
      now,
      props.correlationId,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.resultingBalance,
      state.attempts,
      state.nextAttemptAt,
      state.updatedAt,
      state.correlationId,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  get resultingBalance(): Money | undefined {
    return this._resultingBalance;
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
    resultingBalance?: Money,
  ): void {
    this.assertNotTerminal("markProcessed");
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._failureCode = undefined;
    this._processedAt = at;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
    if (resultingBalance) {
      this._resultingBalance = resultingBalance;
    }
  }

  markPendingReference(nextAttemptAt?: Date, resultingBalance?: Money): void {
    this.assertNotTerminal("markPendingReference");
    const now = new Date();
    this._status = WagerTransactionStatus.PendingReference;
    this._nextAttemptAt = nextAttemptAt ?? new Date(now.getTime() + 1000);
    this._updatedAt = now;
    if (resultingBalance) {
      this._resultingBalance = resultingBalance;
    }
  }

  schedulePendingRetry(
    now: Date,
    baseDelayMs = 1000,
    maxDelayMs = 60000,
    resultingBalance?: Money,
  ): void {
    this.assertNotTerminal("schedulePendingRetry");
    this._status = WagerTransactionStatus.PendingReference;
    this._attempts += 1;
    const delay = Math.min(baseDelayMs * Math.pow(2, this._attempts - 1), maxDelayMs);
    this._nextAttemptAt = new Date(now.getTime() + delay);
    this._updatedAt = now;
    if (resultingBalance) {
      this._resultingBalance = resultingBalance;
    }
  }

  reject(code: FailureCode, at: Date = new Date(), resultingBalance?: Money): void {
    this.assertNotTerminal("reject");
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
    if (resultingBalance) {
      this._resultingBalance = resultingBalance;
    }
  }

  fail(code: FailureCode, at: Date = new Date(), resultingBalance?: Money): void {
    this.assertNotTerminal("fail");
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
    if (resultingBalance) {
      this._resultingBalance = resultingBalance;
    }
  }

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Loss:
        throw new DomainError(
          "LOSS_NO_LEDGER_DIRECTION",
          "LOSS transactions do not affect balance or generate ledger entries",
        );
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidReferenceError(
            "ROLLBACK requires a resolved reference transaction to determine ledger direction",
          );
        }
        if (reference.kind === WagerTransactionKind.Bet) {
          return LedgerDirection.Credit;
        }
        if (
          reference.kind === WagerTransactionKind.Win ||
          reference.kind === WagerTransactionKind.Refund
        ) {
          return LedgerDirection.Debit;
        }
        throw new InvalidReferenceError(
          `ROLLBACK cannot reverse reference of kind ${reference.kind}`,
          FailureCode.ReferenceInvalidKind,
        );
      }
    }
  }

  private assertNotTerminal(targetOperation: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this._status, targetOperation);
    }
  }
}
