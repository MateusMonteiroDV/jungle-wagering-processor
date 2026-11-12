import { describe, expect, it } from "bun:test";
import {
  DomainError,
  InvalidReferenceError,
  InvalidTransactionStateError,
} from "../../src/domain/errors";
import { LedgerDirection } from "../../src/domain/ledger/wallet-ledger-entry";
import { Money } from "../../src/domain/money/money";
import { FailureCode } from "../../src/domain/wager/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/wager/wager-transaction";

describe("WagerTransaction State Machine & Rules", () => {
  const baseProps = {
    id: "tx-1",
    providerId: "prov-1",
    externalTransactionId: "ext-1",
    idempotencyKey: "prov-1:ext-1",
    payloadHash: "a".repeat(64),
    walletId: "w-1",
    playerId: "p-1",
    roundId: "r-1",
    gameId: "g-1",
    kind: WagerTransactionKind.Bet,
    money: Money.from({ amount: "25.00", currency: "BRL" }),
  };

  it("starts in PENDING and transitions to PROCESSED, blocking any further transitions", () => {
    const tx = WagerTransaction.create(baseProps);
    expect(tx.status).toBe(WagerTransactionStatus.Pending);
    expect(tx.isTerminal()).toBe(false);

    const now = new Date();
    tx.markProcessed(undefined, now);
    expect(tx.status).toBe(WagerTransactionStatus.Processed);
    expect(tx.processedAt).toEqual(now);
    expect(tx.isTerminal()).toBe(true);

    expect(() => tx.reject(FailureCode.InsufficientFunds)).toThrow(
      InvalidTransactionStateError,
    );
    expect(() => tx.fail(FailureCode.ProcessingFailed)).toThrow(
      InvalidTransactionStateError,
    );
    expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
    expect(() => tx.markProcessed(undefined, new Date())).toThrow(
      InvalidTransactionStateError,
    );
  });

  it("allows PENDING -> PENDING_REFERENCE -> PROCESSED or REJECTED", () => {
    const tx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: "ext-bet-1",
    });

    tx.markPendingReference();
    expect(tx.status).toBe(WagerTransactionStatus.PendingReference);
    expect(tx.isTerminal()).toBe(false);

    tx.schedulePendingRetry(new Date());
    expect(tx.attempts).toBe(1);

    tx.reject(FailureCode.ReferenceNotFound);
    expect(tx.status).toBe(WagerTransactionStatus.Rejected);
    expect(tx.failureCode).toBe(FailureCode.ReferenceNotFound);
    expect(tx.isTerminal()).toBe(true);

    expect(() => tx.markProcessed("ref-1", new Date())).toThrow(
      InvalidTransactionStateError,
    );
  });

  it("forbids external creation of OPENING transactions", () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Opening,
      }),
    ).toThrow(DomainError);
  });

  it("requires referenceExternalTransactionId for REFUND and ROLLBACK", () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Refund,
      }),
    ).toThrow(InvalidReferenceError);

    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Rollback,
      }),
    ).toThrow(InvalidReferenceError);
  });

  it("correctly computes ledgerDirectionFor each kind", () => {
    const betTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });
    const winTx = WagerTransaction.create({
      ...baseProps,
      id: "tx-win",
      kind: WagerTransactionKind.Win,
    });
    const refundTx = WagerTransaction.create({
      ...baseProps,
      id: "tx-refund",
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: "ext-1",
    });
    const lossTx = WagerTransaction.create({
      ...baseProps,
      id: "tx-loss",
      kind: WagerTransactionKind.Loss,
      money: Money.zero("BRL"),
    });
    const rollbackTx = WagerTransaction.create({
      ...baseProps,
      id: "tx-rb",
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: "ext-1",
    });

    expect(betTx.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(winTx.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refundTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    expect(lossTx.affectsBalance()).toBe(false);
    expect(() => lossTx.ledgerDirectionFor()).toThrow(DomainError);

    expect(rollbackTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    expect(rollbackTx.ledgerDirectionFor(winTx)).toBe(LedgerDirection.Debit);
    expect(rollbackTx.ledgerDirectionFor(refundTx)).toBe(LedgerDirection.Debit);
  });
});
