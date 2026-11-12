import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";
import { loadEnv } from "./infrastructure/config/env";

async function bootstrap() {
  const env = loadEnv();
  const app = await NestFactory.create(AppModule, {
    logger: env.LOG_LEVEL === "silent" ? false : ["error", "warn", "log"],
  });
  app.enableShutdownHooks();

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
