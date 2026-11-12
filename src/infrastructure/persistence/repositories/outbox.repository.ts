import { SqlEntityManager } from "@mikro-orm/postgresql";
import { OutboxRepository } from "../../../application/ports";
import { OutboxMessage } from "../../../domain/messaging/outbox-message";
import { OutboxMessageMapper } from "../mappers";

export class PgOutboxRepository implements OutboxRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async insert(message: OutboxMessage): Promise<void> {
    await this.em.execute(
      `INSERT INTO outbox_messages (
        id, aggregate_id, event_type, payload, occurred_at,
        attempts, next_attempt_at, locked_until, locked_by, last_error, published_at
      ) VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?)`,
      [
        message.id,
        message.aggregateId,
        message.eventType,
        JSON.stringify(message.payload),
        message.occurredAt.toISOString(),
        message.attempts,
        (message.nextAttemptAt ?? message.occurredAt).toISOString(),
        message.lockedUntil ? message.lockedUntil.toISOString() : null,
        message.lockedBy ?? null,
        message.lastError ?? null,
        message.publishedAt ? message.publishedAt.toISOString() : null,
      ],
    );
  }

  async insertMany(messages: OutboxMessage[]): Promise<void> {
    for (const msg of messages) {
      await this.insert(msg);
    }
  }

  async leaseDueMessages(
    now: Date,
    limit: number,
    instanceId: string,
    leaseDurationSec: number,
  ): Promise<OutboxMessage[]> {
    const leaseUntil = new Date(now.getTime() + leaseDurationSec * 1000);
    const rows = await this.em.execute(
      `UPDATE outbox_messages
       SET locked_until = ?, locked_by = ?
       WHERE id IN (
         SELECT id FROM outbox_messages
         WHERE published_at IS NULL
           AND next_attempt_at <= ?
           AND (locked_until IS NULL OR locked_until < ?)
         ORDER BY seq ASC
         LIMIT ?
         FOR UPDATE SKIP LOCKED
       )
       RETURNING *`,
      [
        leaseUntil.toISOString(),
        instanceId,
        now.toISOString(),
        now.toISOString(),
        limit,
      ],
    );

    const messages = rows.map((r) => OutboxMessageMapper.toDomain(r));
    messages.sort((a, b) => {
      if (a.seq !== undefined && b.seq !== undefined) {
        return a.seq < b.seq ? -1 : a.seq > b.seq ? 1 : 0;
      }
      return 0;
    });
    return messages;
  }

  async markPublished(id: string, publishedAt: Date): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_messages
       SET published_at = ?, locked_until = NULL, locked_by = NULL
       WHERE id = ?`,
      [publishedAt.toISOString(), id],
    );
  }

  async scheduleRetry(message: OutboxMessage): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_messages
       SET attempts = ?,
           next_attempt_at = ?,
           locked_until = NULL,
           locked_by = NULL,
           last_error = ?
       WHERE id = ?`,
      [
        message.attempts,
        (message.nextAttemptAt ?? new Date()).toISOString(),
        message.lastError ?? null,
        message.id,
      ],
    );
  }

  async getOldestUnpublishedOccurredAt(): Promise<Date | null> {
    const rows = await this.em.execute(
      `SELECT MIN(occurred_at) AS oldest
       FROM outbox_messages
       WHERE published_at IS NULL`,
    );
    const oldest = (rows[0] as Record<string, unknown> | undefined)?.oldest;
    return oldest ? new Date(oldest as string | Date) : null;
  }
}
