import { EntitySchema } from "@mikro-orm/core";

export class OutboxMessageRecord {
  id!: string;
  seq?: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt!: Date;
  lockedUntil?: Date | null;
  lockedBy?: string | null;
  lastError?: string | null;
  publishedAt?: Date | null;
}

export const OutboxMessageSchema = new EntitySchema<OutboxMessageRecord>({
  class: OutboxMessageRecord,
  tableName: "outbox_messages",
  properties: {
    id: { type: "uuid", primary: true },
    seq: { type: "bigint", persist: false, nullable: true },
    aggregateId: { type: "uuid", fieldName: "aggregate_id" },
    eventType: { type: "text", fieldName: "event_type" },
    payload: { type: "json" },
    occurredAt: { type: "datetime", fieldName: "occurred_at" },
    attempts: { type: "integer", default: 0 },
    nextAttemptAt: { type: "datetime", fieldName: "next_attempt_at" },
    lockedUntil: { type: "datetime", fieldName: "locked_until", nullable: true },
    lockedBy: { type: "text", fieldName: "locked_by", nullable: true },
    lastError: { type: "text", fieldName: "last_error", nullable: true },
    publishedAt: { type: "datetime", fieldName: "published_at", nullable: true },
  },
});
