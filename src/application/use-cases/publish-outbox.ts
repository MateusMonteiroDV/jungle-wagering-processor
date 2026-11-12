import type {
  ClockPort,
  EventPublisherPort,
  MetricsPort,
  UnitOfWork,
} from "../ports";

export interface PublishOutboxOptions {
  instanceId: string;
  batchSize: number;
  leaseDurationSec: number;
  onLeased?: (count: number) => void;
}

export interface PublishOutboxBatchResult {
  leasedCount: number;
  publishedCount: number;
  failedCount: number;
}

export class PublishOutboxUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly publisher: EventPublisherPort,
    private readonly clock: ClockPort,
    private readonly metrics: MetricsPort,
    private readonly options: PublishOutboxOptions,
  ) {}

  async executeBatch(): Promise<PublishOutboxBatchResult> {
    const now = this.clock.now();

    const leased = await this.uow.runInTransaction((tx) =>
      tx.outbox.leaseDueMessages(
        now,
        this.options.batchSize,
        this.options.instanceId,
        this.options.leaseDurationSec,
      ),
    );

    if (leased.length > 0) {
      this.options.onLeased?.(leased.length);
    }

    let publishedCount = 0;
    let failedCount = 0;

    for (const msg of leased) {
      try {
        await this.publisher.publish(msg);
        const publishedAt = this.clock.now();
        msg.markPublished(publishedAt);
        await this.uow.runInTransaction((tx) =>
          tx.outbox.markPublished(msg.id, publishedAt),
        );
        this.metrics.incrementOutboxPublished(msg.eventType);
        publishedCount += 1;
      } catch (err) {
        failedCount += 1;
        const retryNow = this.clock.now();
        const errMsg = err instanceof Error ? err.message : String(err);
        msg.scheduleRetry(retryNow, errMsg);
        await this.uow.runInTransaction((tx) => tx.outbox.scheduleRetry(msg));
        this.metrics.incrementOutboxPublishFailure();
        this.metrics.incrementRetry("outbox_publish");
      }
    }

    await this.updateLagMetric();

    return {
      leasedCount: leased.length,
      publishedCount,
      failedCount,
    };
  }

  async updateLagMetric(): Promise<void> {
    const now = this.clock.now();
    const oldest = await this.uow.runInTransaction((tx) =>
      tx.outbox.getOldestUnpublishedOccurredAt(),
    );
    if (!oldest) {
      this.metrics.setOutboxLagSeconds(0);
      return;
    }
    const lagSec = Math.max(0, (now.getTime() - oldest.getTime()) / 1000);
    this.metrics.setOutboxLagSeconds(lagSec);
  }
}
