import { describe, expect, it } from "bun:test";
import { InvalidLedgerEntryError } from "../../src/domain/errors";
import { WalletBalanceChanged } from "../../src/domain/events";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../src/domain/ledger/wallet-ledger-entry";
import { InboxMessage } from "../../src/domain/messaging/inbox-message";
import { OutboxMessage } from "../../src/domain/messaging/outbox-message";
import { Money } from "../../src/domain/money/money";
import { Wallet } from "../../src/domain/wallet/wallet";

describe("WalletLedgerEntry & Messaging Entities", () => {
  it("validates ledger arithmetic on creation and is structurally immutable", () => {
    const entry = WalletLedgerEntry.create({
      id: "le-1",
      walletId: "w-1",
      transactionId: "tx-1",
      walletVersion: 2,
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: "30.00", currency: "BRL" }),
      balanceBefore: Money.from({ amount: "100.00", currency: "BRL" }),
      balanceAfter: Money.from({ amount: "70.00", currency: "BRL" }),
    });

    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);

    expect(() =>
      WalletLedgerEntry.create({
        id: "le-bad",
        walletId: "w-1",
        transactionId: "tx-1",
        walletVersion: 2,
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: "30.00", currency: "BRL" }),
        balanceBefore: Money.from({ amount: "100.00", currency: "BRL" }),
        balanceAfter: Money.from({ amount: "75.00", currency: "BRL" }),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  it("manages InboxMessage lifecycle", () => {
    const inbox = InboxMessage.receive({
      consumerName: "wager-consumer",
      messageId: "msg-1",
      payloadHash: "h".repeat(64),
    });

    expect(inbox.isProcessed()).toBe(false);
    const now = new Date();
    inbox.markProcessed(now);
    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.processedAt).toEqual(now);
  });

  it("enqueues IntegrationEvent into OutboxMessage and schedules exponential retry", () => {
    const wallet = Wallet.open({
      id: "w-1",
      playerId: "p-1",
      initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
    });
    const entry = wallet.debit({
      entryId: "le-1",
      transactionId: "tx-1",
      money: Money.from({ amount: "20.00", currency: "BRL" }),
    });
    const now = new Date("2026-07-29T15:00:00.000Z");
    const event = WalletBalanceChanged.from(wallet, entry, {
      eventId: "ev-1",
      correlationId: "corr-1",
      causationId: "tx-1",
      occurredAt: now,
    });

    const outbox = OutboxMessage.enqueue(event);
    expect(outbox.id).toBe("ev-1");
    expect(outbox.eventType).toBe("WalletBalanceChanged");
    expect(outbox.isPending()).toBe(true);
    expect(outbox.isDue(now)).toBe(true);

    outbox.scheduleRetry(now, "SQS timeout");
    expect(outbox.attempts).toBe(1);
    expect(outbox.isDue(now)).toBe(false);
    expect(outbox.nextAttemptAt!.getTime()).toBeGreaterThan(now.getTime());

    outbox.markPublished(new Date());
    expect(outbox.isPending()).toBe(false);
  });
});
