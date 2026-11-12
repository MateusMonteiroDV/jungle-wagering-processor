import { Controller, Get, HttpCode, HttpStatus } from "@nestjs/common";

@Controller("health")
export class HealthController {
  @Get("live")
  @HttpCode(HttpStatus.OK)
  live() {
    return {
      status: "ok",
      timestamp: new Date().toISOString(),
    };
  }
}
