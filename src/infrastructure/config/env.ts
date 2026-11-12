import { z } from "zod";

const boolFromString = (defaultVal: boolean) =>
  z
    .string()
    .optional()
    .transform((v) => {
      if (v === undefined || v === "") return defaultVal;
      return v.toLowerCase() === "true" || v === "1";
    });

const intFromString = (defaultVal: number) =>
  z
    .string()
    .optional()
    .transform((v) => {
      if (v === undefined || v === "") return defaultVal;
      const parsed = parseInt(v, 10);
      if (Number.isNaN(parsed)) {
        throw new Error(`Invalid integer value: ${v}`);
      }
      return parsed;
    });

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: intFromString(3001),
  INSTANCE_ID: z.string().default("app-local-1"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace", "silent"]).default("info"),

  RUN_HTTP: boolFromString(true),
  RUN_CONSUMER: boolFromString(true),
  RUN_OUTBOX: boolFromString(true),
  RUN_PENDING: boolFromString(true),
  RUN_DLQ_METRICS: boolFromString(true),

  DB_HOST: z.string().default("localhost"),
  DB_PORT: intFromString(5433),
  DB_USER: z.string().default("wagering"),
  DB_PASSWORD: z.string().default("wagering_secret"),
  DB_NAME: z.string().default("wagering"),
  DB_LOCK_TIMEOUT_MS: intFromString(5000),
  DB_STATEMENT_TIMEOUT_MS: intFromString(10000),
  DB_POOL_MAX: intFromString(20),

  AWS_REGION: z.string().default("us-east-1"),
  AWS_ACCESS_KEY_ID: z.string().default("test"),
  AWS_SECRET_ACCESS_KEY: z.string().default("test"),
  SQS_ENDPOINT: z.string().default("http://localhost:4566"),
  SQS_WAGER_QUEUE_NAME: z.string().default("wager-transactions.fifo"),
  SQS_WAGER_DLQ_NAME: z.string().default("wager-transactions-dlq.fifo"),
  SQS_EVENTS_QUEUE_NAME: z.string().default("wallet-events.fifo"),
  SQS_VISIBILITY_TIMEOUT_SEC: intFromString(30),
  SQS_MAX_RECEIVE_COUNT: intFromString(5),
  SQS_WAIT_TIME_SEC: intFromString(5),
  SQS_CONSUMER_POLL_INTERVAL_MS: intFromString(100),
  SQS_RETRY_BASE_SEC: intFromString(2),

  OUTBOX_POLL_INTERVAL_MS: intFromString(500),
  OUTBOX_BATCH_SIZE: intFromString(50),
  OUTBOX_LEASE_SEC: intFromString(30),

  PENDING_REF_POLL_INTERVAL_MS: intFromString(1000),
  PENDING_REF_BATCH_SIZE: intFromString(50),
  PENDING_REF_MAX_ATTEMPTS: intFromString(10),
  PENDING_REF_TTL_MS: intFromString(900000),
  PENDING_REF_BASE_DELAY_MS: intFromString(1000),

  DLQ_METRICS_POLL_INTERVAL_MS: intFromString(15000),
  SHUTDOWN_GRACE_MS: intFromString(20000),

  FAULT_INJECTION: z.string().default(""),
});

export type AppEnv = z.infer<typeof envSchema>;

export const ENV_TOKEN = Symbol.for("AppEnv");

export function loadEnv(overrides?: Record<string, string | undefined>): AppEnv {
  return envSchema.parse({ ...process.env, ...overrides });
}
