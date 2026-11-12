import "reflect-metadata";
import { INestApplication } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";
import { loadEnv } from "./infrastructure/config/env";
import { correlationMiddleware } from "./interfaces/http/correlation.middleware";

export async function createApp(
  envOverrides?: Record<string, string | undefined>,
): Promise<INestApplication> {
  const env = loadEnv(envOverrides);
  const app = await NestFactory.create(AppModule.register(envOverrides), {
    logger: env.LOG_LEVEL === "silent" ? false : ["error", "warn"],
  });
  app.use(correlationMiddleware);
  app.enableShutdownHooks();
  return app;
}

async function bootstrap() {
  const env = loadEnv();
  const app = await createApp();

  if (env.RUN_HTTP) {
    await app.listen(env.PORT, "0.0.0.0");
  } else {
    await app.init();
  }
}

if (import.meta.main) {
  bootstrap().catch((err) => {
    console.error("Failed to bootstrap application", err);
    process.exit(1);
  });
}
