import { describe, expect, it } from "bun:test";
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidMoneyError,
} from "../../src/domain/errors";
import { LedgerDirection } from "../../src/domain/ledger/wallet-ledger-entry";
import { Money } from "../../src/domain/money/money";
import { Wallet } from "../../src/domain/wallet/wallet";

describe("Wallet Aggregate Root", () => {
  it("opens a wallet with version 1 and generates opening ledger entry when balance > 0", () => {
    const wallet = Wallet.open({
      id: "w-1",
      playerId: "p-1",
      initialBalance: Money.from({ amount: "1000.00", currency: "BRL" }),
    });

    expect(wallet.version).toBe(1);
    expect(wallet.balance.toJSON()).toEqual({ amount: "1000.00", currency: "BRL" });

    const openingEntry = wallet.createOpeningEntry({
      entryId: "le-open",
      transactionId: "tx-open",
    });
    expect(openingEntry).toBeDefined();
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.toJSON().amount).toBe("0.00");
    expect(openingEntry?.balanceAfter.toJSON().amount).toBe("1000.00");
    expect(openingEntry?.isBalanced()).toBe(true);
  });

  it("does not generate opening ledger entry when initial balance is zero", () => {
    const wallet = Wallet.open({
      id: "w-zero",
      playerId: "p-1",
      initialBalance: Money.zero("BRL"),
    });

    expect(wallet.version).toBe(1);
    expect(
      wallet.createOpeningEntry({ entryId: "le-1", transactionId: "tx-1" }),
    ).toBeUndefined();
  });

  it("debits and credits updating balance, incrementing version and returning balanced ledger entries", () => {
    const wallet = Wallet.open({
      id: "w-1",
      playerId: "p-1",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    const debitEntry = wallet.debit({
      entryId: "le-1",
      transactionId: "tx-1",
      money: Money.from({ amount: "25.50", currency: "BRL" }),
    });

    expect(wallet.balance.toJSON().amount).toBe("74.50");
    expect(wallet.version).toBe(2);
    expect(debitEntry.direction).toBe(LedgerDirection.Debit);
    expect(debitEntry.balanceBefore.toJSON().amount).toBe("100.00");
    expect(debitEntry.balanceAfter.toJSON().amount).toBe("74.50");
    expect(debitEntry.walletVersion).toBe(2);
    expect(debitEntry.isBalanced()).toBe(true);

    const creditEntry = wallet.credit({
      entryId: "le-2",
      transactionId: "tx-2",
      money: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    expect(wallet.balance.toJSON().amount).toBe("124.50");
    expect(wallet.version).toBe(3);
    expect(creditEntry.direction).toBe(LedgerDirection.Credit);
    expect(creditEntry.balanceBefore.toJSON().amount).toBe("74.50");
    expect(creditEntry.balanceAfter.toJSON().amount).toBe("124.50");
    expect(creditEntry.walletVersion).toBe(3);
    expect(creditEntry.isBalanced()).toBe(true);
  });

  it("forbids negative balance and leaves wallet state untouched on InsufficientFundsError", () => {
    const wallet = Wallet.open({
      id: "w-1",
      playerId: "p-1",
      initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
    });

    expect(() =>
      wallet.debit({
        entryId: "le-1",
        transactionId: "tx-1",
        money: Money.from({ amount: "50.01", currency: "BRL" }),
      }),
    ).toThrow(InsufficientFundsError);

    expect(wallet.balance.toJSON().amount).toBe("50.00");
    expect(wallet.version).toBe(1);
  });

  it("rejects currency mismatch and zero movements", () => {
    const wallet = Wallet.open({
      id: "w-1",
      playerId: "p-1",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });

    expect(() =>
      wallet.debit({
        entryId: "le-1",
        transactionId: "tx-1",
        money: Money.from({ amount: "10.00", currency: "USD" }),
      }),
    ).toThrow(CurrencyMismatchError);

    expect(() =>
      wallet.credit({
        entryId: "le-2",
        transactionId: "tx-2",
        money: Money.zero("BRL"),
      }),
    ).toThrow(InvalidMoneyError);
  });
});
