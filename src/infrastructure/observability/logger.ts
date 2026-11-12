import pino, { Logger as PinoLogger } from "pino";
import { loadEnv } from "../config/env";
import { getLogContext } from "./context";

const REDACTED_KEYS = new Set([
  "amount",
  "balance",
  "balanceBefore",
  "balanceAfter",
  "initialBalance",
  "storedBalance",
  "calculatedBalance",
  "difference",
  "money",
  "playerId",
  "payload",
  "password",
  "secret",
  "authorization",
]);

function sanitizeObject(obj: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (REDACTED_KEYS.has(k)) {
      continue;
    }
    if (v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Date)) {
      out[k] = sanitizeObject(v as Record<string, unknown>);
    } else {
      out[k] = v;
    }
  }
  return out;
}

export class AppLogger {
  private readonly logger: PinoLogger;

  constructor(level?: string, instanceId?: string) {
    const env = loadEnv();
    this.logger = pino({
      level: level ?? env.LOG_LEVEL,
      base: {
        service: "jungle-wagering-processor",
        instanceId: instanceId ?? env.INSTANCE_ID,
      },
      timestamp: pino.stdTimeFunctions.isoTime,
      formatters: {
        level(label) {
          return { level: label };
        },
      },
    });
  }

  info(meta: Record<string, unknown>, message: string): void {
    this.logger.info({ ...getLogContext(), ...sanitizeObject(meta) }, message);
  }

  warn(meta: Record<string, unknown>, message: string): void {
    this.logger.warn({ ...getLogContext(), ...sanitizeObject(meta) }, message);
  }

  error(meta: Record<string, unknown>, message: string): void {
    this.logger.error({ ...getLogContext(), ...sanitizeObject(meta) }, message);
  }

  debug(meta: Record<string, unknown>, message: string): void {
    this.logger.debug({ ...getLogContext(), ...sanitizeObject(meta) }, message);
  }
}
