import { CurrencyMismatchError, InvalidLedgerEntryError } from "../errors";
import { Money } from "../money/money";

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  walletVersion?: number;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  seq?: bigint;
  walletId: string;
  transactionId: string;
  walletVersion: number;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
    public readonly walletVersion: number = 1,
    public readonly seq?: bigint,
  ) {}

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.id || !props.walletId || !props.transactionId) {
      throw new InvalidLedgerEntryError("Ledger entry requires id, walletId and transactionId");
    }
    if (!props.money.isPositive()) {
      throw new InvalidLedgerEntryError("Ledger entry money must be strictly positive");
    }
    if (props.balanceBefore.isNegative()) {
      throw new InvalidLedgerEntryError("balanceBefore cannot be negative");
    }
    if (props.balanceAfter.isNegative()) {
      throw new InvalidLedgerEntryError("balanceAfter cannot be negative");
    }
    if (
      props.money.currency !== props.balanceBefore.currency ||
      props.money.currency !== props.balanceAfter.currency
    ) {
      throw new CurrencyMismatchError(props.balanceBefore.currency, props.money.currency);
    }
    const walletVersion = props.walletVersion ?? 1;
    if (walletVersion < 1) {
      throw new InvalidLedgerEntryError("walletVersion must be >= 1");
    }

    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ?? new Date(),
      walletVersion,
    );

    if (!entry.isBalanced()) {
      throw new InvalidLedgerEntryError(
        `Unbalanced ledger entry: ${props.balanceBefore.toString()} ${props.direction} ${props.money.toString()} != ${props.balanceAfter.toString()}`,
      );
    }

    return Object.freeze(entry);
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return Object.freeze(
      new WalletLedgerEntry(
        state.id,
        state.walletId,
        state.transactionId,
        state.direction,
        state.money,
        state.balanceBefore,
        state.balanceAfter,
        state.createdAt,
        state.walletVersion,
        state.seq,
      ),
    );
  }

  isBalanced(): boolean {
    if (
      this.money.currency !== this.balanceBefore.currency ||
      this.money.currency !== this.balanceAfter.currency
    ) {
      return false;
    }
    if (this.direction === LedgerDirection.Credit) {
      return this.balanceBefore.add(this.money).equals(this.balanceAfter);
    }
    if (this.direction === LedgerDirection.Debit) {
      return this.balanceBefore.subtract(this.money).equals(this.balanceAfter);
    }
    return false;
  }
}
