import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import {
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Inject,
  Res,
} from "@nestjs/common";
import type { Response } from "express";
import { SqsQueueManager } from "../../../infrastructure/messaging/sqs-client";

@Controller("health")
export class HealthController {
  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Inject(SqsQueueManager) private readonly sqs: SqsQueueManager,
  ) {}

  @Get("live")
  @HttpCode(HttpStatus.OK)
  live() {
    return {
      status: "ok",
      timestamp: new Date().toISOString(),
    };
  }

  @Get("ready")
  async ready(@Res({ passthrough: true }) res: Response) {
    let postgresOk = false;
    let sqsOk = false;

    try {
      const em = this.orm.em.fork() as SqlEntityManager;
      await em.execute("SELECT 1");
      postgresOk = true;
    } catch {
      postgresOk = false;
    }

    try {
      await this.sqs.checkReadiness();
      sqsOk = true;
    } catch {
      sqsOk = false;
    }

    const allReady = postgresOk && sqsOk;
    res.status(allReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);

    return {
      status: allReady ? "ready" : "degraded",
      dependencies: {
        postgres: postgresOk ? "up" : "down",
        sqs: sqsOk ? "up" : "down",
      },
      timestamp: new Date().toISOString(),
    };
  }
}
