import Decimal from "decimal.js";
import { CurrencyMismatchError, InvalidMoneyError } from "../errors";

export interface MoneyProps {
  amount: string;
  currency: string;
}

const INPUT_AMOUNT_REGEX = /^\d+(\.\d{1,2})?$/;
const PERSISTENCE_AMOUNT_REGEX = /^-?\d+(\.\d{1,2})?$/;
const CURRENCY_REGEX = /^[A-Z]{3}$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  static from(props: MoneyProps): Money {
    if (!props || typeof props !== "object") {
      throw new InvalidMoneyError("Money props must be an object");
    }
    const { amount, currency } = props;
    if (typeof amount !== "string" || !INPUT_AMOUNT_REGEX.test(amount)) {
      throw new InvalidMoneyError(
        `Invalid money amount '${String(amount)}': must be a non-negative decimal string with up to 2 decimal places`,
      );
    }
    Money.assertValidCurrency(currency);
    const dec = new Decimal(amount);
    if (!dec.isFinite() || dec.isNaN() || dec.isNegative()) {
      throw new InvalidMoneyError(`Invalid money amount '${amount}'`);
    }
    return new Money(dec, currency);
  }

  /**
   * Reconstructs Money from persistence (PostgreSQL numeric(20,2) string).
   */
  static fromPersistence(amount: string, currency: string): Money {
    if (typeof amount !== "string" || !PERSISTENCE_AMOUNT_REGEX.test(amount)) {
      throw new InvalidMoneyError(`Invalid persisted money amount '${String(amount)}'`);
    }
    Money.assertValidCurrency(currency);
    return new Money(new Decimal(amount), currency);
  }

  static zero(currency: string): Money {
    Money.assertValidCurrency(currency);
    return new Money(new Decimal(0), currency);
  }

  get amount(): string {
    return this.value.toFixed(2);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.equals(other.value);
  }

  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  toString(): string {
    return `${this.value.toFixed(2)} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (!(other instanceof Money)) {
      throw new InvalidMoneyError("Expected instance of Money");
    }
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static assertValidCurrency(currency: string): void {
    if (typeof currency !== "string" || !CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(
        `Invalid currency '${String(currency)}': must be a 3-letter uppercase ISO-4217 code`,
      );
    }
  }
}
