import { createHash } from "crypto";
import { Money } from "../domain/money/money";
import { WagerTransactionKind } from "../domain/wager/wager-transaction";

export interface WagerPayloadFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind | string;
  money: {
    amount: string;
    currency: string;
  };
  referenceExternalTransactionId?: string;
}

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map((item) => canonicalJson(item)).join(",") + "]";
  }
  const obj = value as Record<string, unknown>;
  const sortedKeys = Object.keys(obj)
    .filter((key) => obj[key] !== undefined)
    .sort();
  const pairs = sortedKeys.map(
    (key) => `${JSON.stringify(key)}:${canonicalJson(obj[key])}`,
  );
  return "{" + pairs.join(",") + "}";
}

export function computeWagerPayloadHash(input: WagerPayloadFields): string {
  const normalizedMoney = Money.from({
    amount: input.money.amount,
    currency: input.money.currency,
  }).toJSON();

  const businessSubset: Record<string, unknown> = {
    providerId: input.providerId,
    externalTransactionId: input.externalTransactionId,
    playerId: input.playerId,
    walletId: input.walletId,
    roundId: input.roundId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: normalizedMoney.amount,
      currency: normalizedMoney.currency,
    },
  };

  if (
    input.referenceExternalTransactionId !== undefined &&
    input.referenceExternalTransactionId !== ""
  ) {
    businessSubset.referenceExternalTransactionId =
      input.referenceExternalTransactionId;
  }

  const canonical = canonicalJson(businessSubset);
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

export function computeSha256Hex(raw: string): string {
  return createHash("sha256").update(raw, "utf8").digest("hex");
}
