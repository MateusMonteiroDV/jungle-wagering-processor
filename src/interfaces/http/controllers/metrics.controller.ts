import { Controller, Get, Inject, Res } from "@nestjs/common";
import type { Response } from "express";
import { PrometheusMetrics } from "../../../infrastructure/observability/metrics";

@Controller("metrics")
export class MetricsController {
  constructor(
    @Inject(PrometheusMetrics)
    private readonly metrics: PrometheusMetrics,
  ) {}

  @Get()
  async getMetrics(@Res() res: Response): Promise<void> {
    const body = await this.metrics.getMetricsText();
    res.setHeader("Content-Type", this.metrics.getContentType());
    res.status(200).send(body);
  }
}
