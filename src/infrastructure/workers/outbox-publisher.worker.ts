import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from "@nestjs/common";
import { PublishOutboxUseCase } from "../../application/use-cases/publish-outbox";
import { ENV_TOKEN } from "../config/env";
import type { AppEnv } from "../config/env";
import { AppLogger } from "../observability/logger";

@Injectable()
export class OutboxPublisherWorker
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(
    @Inject(ENV_TOKEN) private readonly env: AppEnv,
    @Inject(PublishOutboxUseCase)
    private readonly publishOutbox: PublishOutboxUseCase,
    @Inject(AppLogger) private readonly logger: AppLogger,
  ) {}

  onApplicationBootstrap(): void {
    if (!this.env.RUN_OUTBOX) return;
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
    await this.publishOutbox.executeBatch();
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        const res = await this.publishOutbox.executeBatch();
        if (res.leasedCount > 0 && res.failedCount === 0) {
          // If a full batch was leased, poll again immediately
          if (res.leasedCount >= this.env.OUTBOX_BATCH_SIZE) {
            continue;
          }
        }
      } catch (err) {
        this.logger.error(
          { errorMessage: err instanceof Error ? err.message : String(err) },
          "OutboxPublisherWorker tick error",
        );
      }
      if (!this.running) break;
      await Bun.sleep(this.env.OUTBOX_POLL_INTERVAL_MS);
    }
  }
}
