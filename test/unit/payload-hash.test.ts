import { describe, expect, it } from "bun:test";
import {
  canonicalJson,
  computeWagerPayloadHash,
} from "../../src/application/payload-hash";
import { WagerTransactionKind } from "../../src/domain/wager/wager-transaction";

describe("Canonical JSON & Payload Hash", () => {
  it("produces identical hash regardless of object key order or amount trailing zeroes", () => {
    const h1 = computeWagerPayloadHash({
      providerId: "provider-a",
      externalTransactionId: "tx-1",
      playerId: "p-1",
      walletId: "w-1",
      roundId: "r-1",
      gameId: "g-1",
      kind: WagerTransactionKind.Bet,
      money: { amount: "25", currency: "BRL" },
    });

    const h2 = computeWagerPayloadHash({
      kind: WagerTransactionKind.Bet,
      gameId: "g-1",
      roundId: "r-1",
      walletId: "w-1",
      playerId: "p-1",
      externalTransactionId: "tx-1",
      providerId: "provider-a",
      money: { currency: "BRL", amount: "25.00" },
      referenceExternalTransactionId: undefined,
    });

    expect(h1).toBe(h2);
    expect(h1).toHaveLength(64);
  });

  it("produces different hash when any business field changes", () => {
    const base = {
      providerId: "provider-a",
      externalTransactionId: "tx-1",
      playerId: "p-1",
      walletId: "w-1",
      roundId: "r-1",
      gameId: "g-1",
      kind: WagerTransactionKind.Bet,
      money: { amount: "25.00", currency: "BRL" },
    };

    const baseHash = computeWagerPayloadHash(base);
    const diffAmountHash = computeWagerPayloadHash({
      ...base,
      money: { amount: "25.01", currency: "BRL" },
    });
    const diffKindHash = computeWagerPayloadHash({
      ...base,
      kind: WagerTransactionKind.Win,
    });

    expect(baseHash).not.toBe(diffAmountHash);
    expect(baseHash).not.toBe(diffKindHash);
    expect(canonicalJson({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
  });
});
