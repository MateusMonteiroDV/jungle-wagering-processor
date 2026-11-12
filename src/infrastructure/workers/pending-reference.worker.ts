import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from "@nestjs/common";
import { ResolvePendingReferencesUseCase } from "../../application/use-cases/resolve-pending-references";
import { ENV_TOKEN } from "../config/env";
import type { AppEnv } from "../config/env";
import { AppLogger } from "../observability/logger";

@Injectable()
export class PendingReferenceWorker
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(
    @Inject(ENV_TOKEN) private readonly env: AppEnv,
    @Inject(ResolvePendingReferencesUseCase)
    private readonly resolvePending: ResolvePendingReferencesUseCase,
    @Inject(AppLogger) private readonly logger: AppLogger,
  ) {}

  onApplicationBootstrap(): void {
    if (!this.env.RUN_PENDING) return;
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
    await this.resolvePending.executeBatch();
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.resolvePending.executeBatch();
      } catch (err) {
        this.logger.error(
          { errorMessage: err instanceof Error ? err.message : String(err) },
          "PendingReferenceWorker tick error",
        );
      }
      if (!this.running) break;
      await Bun.sleep(this.env.PENDING_REF_POLL_INTERVAL_MS);
    }
  }
}
