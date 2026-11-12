import { EntitySchema } from "@mikro-orm/core";

export class WalletLedgerEntryRecord {
  id!: string;
  seq?: string;
  walletId!: string;
  transactionId!: string;
  walletVersion!: number;
  direction!: string;
  amount!: string;
  currency!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  createdAt!: Date;
}

export const WalletLedgerEntrySchema = new EntitySchema<WalletLedgerEntryRecord>({
  class: WalletLedgerEntryRecord,
  tableName: "wallet_ledger_entries",
  properties: {
    id: { type: "uuid", primary: true },
    seq: { type: "bigint", persist: false, nullable: true },
    walletId: { type: "uuid", fieldName: "wallet_id" },
    transactionId: { type: "uuid", fieldName: "transaction_id" },
    walletVersion: { type: "integer", fieldName: "wallet_version" },
    direction: { type: "text" },
    amount: { type: "decimal", precision: 20, scale: 2 },
    currency: { type: "string", length: 3 },
    balanceBefore: {
      type: "decimal",
      precision: 20,
      scale: 2,
      fieldName: "balance_before",
    },
    balanceAfter: {
      type: "decimal",
      precision: 20,
      scale: 2,
      fieldName: "balance_after",
    },
    createdAt: { type: "datetime", fieldName: "created_at" },
  },
});
