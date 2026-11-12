import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "prom-client";
import { MetricsPort, ProcessingSource } from "../../application/ports";
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/wager/wager-transaction";

export class PrometheusMetrics implements MetricsPort {
  public readonly registry: Registry;

  private readonly transactionsTotal: Counter<"status" | "kind" | "source">;
  private readonly idempotentReplaysTotal: Counter<"source">;
  private readonly inboxDuplicatesTotal: Counter;
  private readonly retriesTotal: Counter<"reason">;
  private readonly dlqMessagesTotal: Counter<"reason">;
  private readonly dlqDepthGauge: Gauge;
  private readonly lockConflictsTotal: Counter;
  private readonly outboxLagSecondsGauge: Gauge;
  private readonly outboxPublishedTotal: Counter<"event_type">;
  private readonly outboxPublishFailuresTotal: Counter;
  private readonly processingDurationSeconds: Histogram<"source">;
  private readonly pendingReferenceTotal: Counter;
  private readonly reconciliationMismatchesTotal: Counter;

  constructor(registry?: Registry) {
    this.registry = registry ?? new Registry();
    collectDefaultMetrics({ register: this.registry, prefix: "wagering_" });

    this.transactionsTotal = new Counter({
      name: "wagering_transactions_total",
      help: "Total wager transactions processed by final status, kind and source",
      labelNames: ["status", "kind", "source"],
      registers: [this.registry],
    });

    this.idempotentReplaysTotal = new Counter({
      name: "wagering_idempotent_replays_total",
      help: "Total idempotent replays detected",
      labelNames: ["source"],
      registers: [this.registry],
    });

    this.inboxDuplicatesTotal = new Counter({
      name: "wagering_inbox_duplicates_total",
      help: "Total duplicate SQS messages deduplicated via persistent inbox",
      registers: [this.registry],
    });

    this.retriesTotal = new Counter({
      name: "wagering_retries_total",
      help: "Total retry attempts by reason",
      labelNames: ["reason"],
      registers: [this.registry],
    });

    this.dlqMessagesTotal = new Counter({
      name: "wagering_dlq_messages_total",
      help: "Total messages routed to DLQ by reason",
      labelNames: ["reason"],
      registers: [this.registry],
    });

    this.dlqDepthGauge = new Gauge({
      name: "wagering_dlq_depth",
      help: "Approximate number of messages currently in the SQS DLQ",
      registers: [this.registry],
    });

    this.lockConflictsTotal = new Counter({
      name: "wagering_lock_conflicts_total",
      help: "Total database lock timeouts or deadlock conflicts",
      registers: [this.registry],
    });

    this.outboxLagSecondsGauge = new Gauge({
      name: "wagering_outbox_lag_seconds",
      help: "Age in seconds of the oldest unpublished outbox message",
      registers: [this.registry],
    });

    this.outboxPublishedTotal = new Counter({
      name: "wagering_outbox_published_total",
      help: "Total integration events published from outbox",
      labelNames: ["event_type"],
      registers: [this.registry],
    });

    this.outboxPublishFailuresTotal = new Counter({
      name: "wagering_outbox_publish_failures_total",
      help: "Total failures while publishing outbox messages",
      registers: [this.registry],
    });

    this.processingDurationSeconds = new Histogram({
      name: "wagering_processing_duration_seconds",
      help: "Transaction processing latency in seconds",
      labelNames: ["source"],
      buckets: [0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [this.registry],
    });

    this.pendingReferenceTotal = new Counter({
      name: "wagering_pending_reference_total",
      help: "Total transactions parked as PENDING_REFERENCE due to missing reference",
      registers: [this.registry],
    });

    this.reconciliationMismatchesTotal = new Counter({
      name: "wagering_reconciliation_mismatches_total",
      help: "Total wallet reconciliation mismatches detected",
      registers: [this.registry],
    });
  }

  incrementTransaction(
    status: WagerTransactionStatus,
    kind: WagerTransactionKind,
    source: ProcessingSource,
  ): void {
    this.transactionsTotal.inc({ status, kind, source });
  }

  incrementIdempotentReplay(source: ProcessingSource): void {
    this.idempotentReplaysTotal.inc({ source });
  }

  incrementInboxDuplicate(): void {
    this.inboxDuplicatesTotal.inc();
  }

  incrementRetry(reason: string): void {
    this.retriesTotal.inc({ reason });
  }

  incrementDlqMessage(reason: string): void {
    this.dlqMessagesTotal.inc({ reason });
  }

  setDlqDepth(depth: number): void {
    this.dlqDepthGauge.set(depth);
  }

  incrementLockConflict(): void {
    this.lockConflictsTotal.inc();
  }

  setOutboxLagSeconds(lagSeconds: number): void {
    this.outboxLagSecondsGauge.set(lagSeconds);
  }

  incrementOutboxPublished(eventType: string): void {
    this.outboxPublishedTotal.inc({ event_type: eventType });
  }

  incrementOutboxPublishFailure(): void {
    this.outboxPublishFailuresTotal.inc();
  }

  observeProcessingDuration(
    source: ProcessingSource,
    durationSeconds: number,
  ): void {
    this.processingDurationSeconds.observe({ source }, durationSeconds);
  }

  incrementPendingReference(): void {
    this.pendingReferenceTotal.inc();
  }

  incrementReconciliationMismatch(): void {
    this.reconciliationMismatchesTotal.inc();
  }

  async getMetricsText(): Promise<string> {
    return this.registry.metrics();
  }

  getContentType(): string {
    return this.registry.contentType;
  }
}
