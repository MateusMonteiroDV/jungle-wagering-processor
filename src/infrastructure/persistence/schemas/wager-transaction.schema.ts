import { EntitySchema } from "@mikro-orm/core";

export class WagerTransactionRecord {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  resultingBalance?: string | null;
  attempts!: number;
  nextAttemptAt?: Date | null;
  correlationId?: string | null;
  createdAt!: Date;
  updatedAt!: Date;
  processedAt?: Date | null;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: "wager_transactions",
  properties: {
    id: { type: "uuid", primary: true },
    providerId: { type: "text", fieldName: "provider_id" },
    externalTransactionId: { type: "text", fieldName: "external_transaction_id" },
    idempotencyKey: { type: "text", fieldName: "idempotency_key" },
    payloadHash: { type: "string", length: 64, fieldName: "payload_hash" },
    walletId: { type: "uuid", fieldName: "wallet_id" },
    playerId: { type: "uuid", fieldName: "player_id" },
    roundId: { type: "text", fieldName: "round_id" },
    gameId: { type: "text", fieldName: "game_id" },
    kind: { type: "text" },
    amount: { type: "decimal", precision: 20, scale: 2 },
    currency: { type: "string", length: 3 },
    referenceExternalTransactionId: {
      type: "text",
      fieldName: "reference_external_transaction_id",
      nullable: true,
    },
    referenceTransactionId: {
      type: "uuid",
      fieldName: "reference_transaction_id",
      nullable: true,
    },
    status: { type: "text" },
    failureCode: { type: "text", fieldName: "failure_code", nullable: true },
    resultingBalance: {
      type: "decimal",
      precision: 20,
      scale: 2,
      fieldName: "resulting_balance",
      nullable: true,
    },
    attempts: { type: "integer", default: 0 },
    nextAttemptAt: {
      type: "datetime",
      fieldName: "next_attempt_at",
      nullable: true,
    },
    correlationId: { type: "text", fieldName: "correlation_id", nullable: true },
    createdAt: { type: "datetime", fieldName: "created_at" },
    updatedAt: { type: "datetime", fieldName: "updated_at" },
    processedAt: { type: "datetime", fieldName: "processed_at", nullable: true },
  },
});
