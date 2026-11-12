import { SqlEntityManager } from "@mikro-orm/postgresql";
import {
  InboxRepository,
  InboxTryInsertResult,
} from "../../../application/ports";
import { InboxMessage } from "../../../domain/messaging/inbox-message";
import { InboxMessageMapper } from "../mappers";

export class PgInboxRepository implements InboxRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async tryInsert(message: InboxMessage): Promise<InboxTryInsertResult> {
    const insertedRows = await this.em.execute(
      `INSERT INTO inbox_messages (
        consumer_name, message_id, payload_hash, received_at, processed_at
      ) VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (consumer_name, message_id) DO NOTHING
      RETURNING *`,
      [
        message.consumerName,
        message.messageId,
        message.payloadHash,
        message.receivedAt.toISOString(),
        message.processedAt ? message.processedAt.toISOString() : null,
      ],
    );

    if (Array.isArray(insertedRows) && insertedRows.length === 1) {
      return { inserted: true };
    }

    const existingRows = await this.em.execute(
      `SELECT * FROM inbox_messages
       WHERE consumer_name = ? AND message_id = ?
       LIMIT 1`,
      [message.consumerName, message.messageId],
    );

    const existingRow = existingRows[0];
    return {
      inserted: false,
      existing: existingRow
        ? InboxMessageMapper.toDomain(existingRow)
        : undefined,
    };
  }

  async markProcessed(
    consumerName: string,
    messageId: string,
    processedAt: Date,
  ): Promise<void> {
    await this.em.execute(
      `UPDATE inbox_messages
       SET processed_at = ?
       WHERE consumer_name = ? AND message_id = ?`,
      [processedAt.toISOString(), consumerName, messageId],
    );
  }
}
