import { InboxMessageRecord, InboxMessageSchema } from "./inbox-message.schema";
import { OutboxMessageRecord, OutboxMessageSchema } from "./outbox-message.schema";
import {
  WagerTransactionRecord,
  WagerTransactionSchema,
} from "./wager-transaction.schema";
import {
  WalletLedgerEntryRecord,
  WalletLedgerEntrySchema,
} from "./wallet-ledger-entry.schema";
import { WalletRecord, WalletSchema } from "./wallet.schema";

export {
  WalletRecord,
  WalletSchema,
  WagerTransactionRecord,
  WagerTransactionSchema,
  WalletLedgerEntryRecord,
  WalletLedgerEntrySchema,
  InboxMessageRecord,
  InboxMessageSchema,
  OutboxMessageRecord,
  OutboxMessageSchema,
};

export const ALL_SCHEMAS = [
  WalletSchema,
  WagerTransactionSchema,
  WalletLedgerEntrySchema,
  InboxMessageSchema,
  OutboxMessageSchema,
];
