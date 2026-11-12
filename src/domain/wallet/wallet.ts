import {
  CurrencyMismatchError,
  DomainError,
  InsufficientFundsError,
  InvalidMoneyError,
} from "../errors";
import { LedgerDirection, WalletLedgerEntry } from "../ledger/wallet-ledger-entry";
import { Money } from "../money/money";

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  createdAt?: Date;
}

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementProps {
  entryId: string;
  transactionId: string;
  money: Money;
  occurredAt?: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
    private _loadedVersion: number,
  ) {}

  static open(props: OpenWalletProps): Wallet {
    if (!props.id || !props.playerId) {
      throw new DomainError("INVALID_WALLET", "Wallet requires id and playerId");
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError("Initial wallet balance cannot be negative");
    }
    const now = props.createdAt ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
      1,
    );
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
      state.version,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get loadedVersion(): number {
    return this._loadedVersion;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  createOpeningEntry(params: {
    entryId: string;
    transactionId: string;
  }): WalletLedgerEntry | undefined {
    if (this._balance.isZero()) {
      return undefined;
    }
    return WalletLedgerEntry.create({
      id: params.entryId,
      walletId: this.id,
      transactionId: params.transactionId,
      walletVersion: 1,
      direction: LedgerDirection.Credit,
      money: this._balance,
      balanceBefore: Money.zero(this.currency),
      balanceAfter: this._balance,
      createdAt: this.createdAt,
    });
  }

  debit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.money);
    if (!props.money.isPositive()) {
      throw new InvalidMoneyError("Debit amount must be strictly positive");
    }
    const nextBalance = this._balance.subtract(props.money);
    if (nextBalance.isNegative()) {
      throw new InsufficientFundsError(this.id);
    }
    const balanceBefore = this._balance;
    const occurredAt = props.occurredAt ?? new Date();
    this._balance = nextBalance;
    this._version += 1;
    this._updatedAt = occurredAt;

    return WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      walletVersion: this._version,
      direction: LedgerDirection.Debit,
      money: props.money,
      balanceBefore,
      balanceAfter: nextBalance,
      createdAt: occurredAt,
    });
  }

  credit(props: WalletMovementProps): WalletLedgerEntry {
    this.assertSameCurrency(props.money);
    if (!props.money.isPositive()) {
      throw new InvalidMoneyError("Credit amount must be strictly positive");
    }
    const balanceBefore = this._balance;
    const nextBalance = this._balance.add(props.money);
    const occurredAt = props.occurredAt ?? new Date();
    this._balance = nextBalance;
    this._version += 1;
    this._updatedAt = occurredAt;

    return WalletLedgerEntry.create({
      id: props.entryId,
      walletId: this.id,
      transactionId: props.transactionId,
      walletVersion: this._version,
      direction: LedgerDirection.Credit,
      money: props.money,
      balanceBefore,
      balanceAfter: nextBalance,
      createdAt: occurredAt,
    });
  }

  markPersisted(): void {
    this._loadedVersion = this._version;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
