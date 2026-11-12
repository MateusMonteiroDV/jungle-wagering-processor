import { describe, expect, it } from "bun:test";
import { CurrencyMismatchError, InvalidMoneyError } from "../../src/domain/errors";
import { Money } from "../../src/domain/money/money";

describe("Money (Domain Value Object)", () => {
  it("creates Money with valid amount and formats to fixed 2 decimals", () => {
    const m1 = Money.from({ amount: "25.00", currency: "BRL" });
    expect(m1.toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
    expect(m1.toString()).toBe("25.00 BRL");

    const m2 = Money.from({ amount: "100", currency: "BRL" });
    expect(m2.toJSON()).toEqual({ amount: "100.00", currency: "BRL" });

    const m3 = Money.from({ amount: "0.5", currency: "USD" });
    expect(m3.toJSON()).toEqual({ amount: "0.50", currency: "USD" });
  });

  it("creates zero money for a valid currency", () => {
    const zero = Money.zero("BRL");
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
    expect(zero.isNegative()).toBe(false);
    expect(zero.toJSON()).toEqual({ amount: "0.00", currency: "BRL" });
  });

  it("rejects invalid amount strings (NaN, Infinity, scientific notation, empty, negative, >2 decimals)", () => {
    const invalidAmounts = [
      "",
      " ",
      "NaN",
      "Infinity",
      "-Infinity",
      "1e5",
      "1E2",
      "25.000",
      "10.123",
      "-10.00",
      "-0.01",
      "25,00",
      "abc",
      " 25.00",
      "25.00 ",
      ".50",
      "25.",
    ];

    for (const amount of invalidAmounts) {
      expect(() => Money.from({ amount, currency: "BRL" })).toThrow(InvalidMoneyError);
    }
  });

  it("rejects invalid currencies", () => {
    const invalidCurrencies = ["", "brl", "BR", "BRLX", "123", "BR$"];
    for (const currency of invalidCurrencies) {
      expect(() => Money.from({ amount: "10.00", currency })).toThrow(InvalidMoneyError);
    }
  });

  it("is immutable and performs exact decimal arithmetic without floating-point errors", () => {
    const a = Money.from({ amount: "0.10", currency: "BRL" });
    const b = Money.from({ amount: "0.20", currency: "BRL" });
    const sum = a.add(b);

    expect(sum.toJSON()).toEqual({ amount: "0.30", currency: "BRL" });
    expect(a.toJSON()).toEqual({ amount: "0.10", currency: "BRL" });
    expect(b.toJSON()).toEqual({ amount: "0.20", currency: "BRL" });

    const diff = b.subtract(a);
    expect(diff.toJSON()).toEqual({ amount: "0.10", currency: "BRL" });

    const neg = a.subtract(b);
    expect(neg.isNegative()).toBe(true);
    expect(neg.toJSON()).toEqual({ amount: "-0.10", currency: "BRL" });
    expect(neg.negate().equals(diff)).toBe(true);
  });

  it("compares Money instances accurately", () => {
    const a = Money.from({ amount: "10.00", currency: "BRL" });
    const b = Money.from({ amount: "20.00", currency: "BRL" });
    const c = Money.from({ amount: "10", currency: "BRL" });

    expect(a.isLessThan(b)).toBe(true);
    expect(b.isLessThan(a)).toBe(false);
    expect(a.equals(c)).toBe(true);
  });

  it("throws CurrencyMismatchError when operating across different currencies", () => {
    const brl = Money.from({ amount: "10.00", currency: "BRL" });
    const usd = Money.from({ amount: "10.00", currency: "USD" });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.equals(usd)).toThrow(CurrencyMismatchError);
  });
});
