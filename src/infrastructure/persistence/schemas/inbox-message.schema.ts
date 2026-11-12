import { EntitySchema } from "@mikro-orm/core";

export class InboxMessageRecord {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export const InboxMessageSchema = new EntitySchema<InboxMessageRecord>({
  class: InboxMessageRecord,
  tableName: "inbox_messages",
  properties: {
    consumerName: { type: "text", fieldName: "consumer_name", primary: true },
    messageId: { type: "text", fieldName: "message_id", primary: true },
    payloadHash: { type: "string", length: 64, fieldName: "payload_hash" },
    receivedAt: { type: "datetime", fieldName: "received_at" },
    processedAt: { type: "datetime", fieldName: "processed_at", nullable: true },
  },
});
