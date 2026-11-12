import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from "@nestjs/common";
import { TOKENS } from "../../application/ports";
import type { MetricsPort } from "../../application/ports";
import { ENV_TOKEN } from "../config/env";
import type { AppEnv } from "../config/env";
import { SqsQueueManager } from "../messaging/sqs-client";
import { AppLogger } from "../observability/logger";

@Injectable()
export class DlqMetricsWorker
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(
    @Inject(ENV_TOKEN) private readonly env: AppEnv,
    @Inject(SqsQueueManager) private readonly sqs: SqsQueueManager,
    @Inject(TOKENS.MetricsPort) private readonly metrics: MetricsPort,
    @Inject(AppLogger) private readonly logger: AppLogger,
  ) {}

  onApplicationBootstrap(): void {
    if (!this.env.RUN_DLQ_METRICS) return;
    this.running = true;
    this.loopPromise = this.runLoop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.running = false;
    if (this.loopPromise) {
      await this.loopPromise;
    }
  }

  async tick(): Promise<void> {
    const depth = await this.sqs.getDlqApproximateDepth();
    this.metrics.setDlqDepth(depth);
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.tick();
      } catch (err) {
        this.logger.warn(
          { errorMessage: err instanceof Error ? err.message : String(err) },
          "DlqMetricsWorker failed to query DLQ depth",
        );
      }
      if (!this.running) break;
      await Bun.sleep(this.env.DLQ_METRICS_POLL_INTERVAL_MS);
    }
  }
}
