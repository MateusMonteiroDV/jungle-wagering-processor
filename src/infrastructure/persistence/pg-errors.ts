import {
  DomainError,
  IdempotencyConflictError,
  TransientInfrastructureError,
  WalletAlreadyExistsError,
} from "../../domain/errors";

export interface PgErrorLike {
  code?: string;
  constraint?: string;
  message?: string;
  cause?: PgErrorLike;
}

export function extractPgError(err: unknown): {
  code?: string;
  constraint?: string;
  message: string;
} {
  if (!err || typeof err !== "object") {
    return { message: String(err) };
  }
  const e = err as PgErrorLike;
  const code = e.code ?? e.cause?.code;
  const constraint = e.constraint ?? e.cause?.constraint;
  const message = e.message ?? e.cause?.message ?? "Database error";
  return { code, constraint, message };
}

export function translatePgError(err: unknown): Error {
  if (err instanceof DomainError) {
    return err;
  }

  const { code, constraint, message } = extractPgError(err);

  // 23505 = unique_violation
  if (code === "23505") {
    if (constraint === "uq_wallet_player_currency") {
      return new WalletAlreadyExistsError("unknown", "unknown");
    }
    if (
      constraint === "uq_wtx_idempotency_key" ||
      constraint === "uq_wtx_provider_external"
    ) {
      // Concurrent race on the same idempotency key or external id across transactions;
      // treated as transient so the caller retries in a fresh transaction and hits the replay/conflict path
      return new TransientInfrastructureError(
        `Concurrent idempotency key insert race (${constraint})`,
        err,
      );
    }
    return new IdempotencyConflictError(
      constraint ?? "unique_constraint",
      message,
    );
  }

  // 23514 = check_violation (or custom constraint trigger violation)
  if (code === "23514") {
    return new DomainError("DB_CONSTRAINT_VIOLATION", message);
  }

  // 40P01 = deadlock_detected
  // 40001 = serialization_failure
  // 55P03 = lock_not_available (lock_timeout)
  // 57014 = query_canceled (statement_timeout)
  // 08xxx = connection exception
  if (
    code === "40P01" ||
    code === "40001" ||
    code === "55P03" ||
    code === "57014" ||
    (code && code.startsWith("08"))
  ) {
    return new TransientInfrastructureError(
      `Database transient/lock error (${code}): ${message}`,
      err,
    );
  }

  if (
    message.includes("ECONNREFUSED") ||
    message.includes("Connection terminated") ||
    message.includes("timeout")
  ) {
    return new TransientInfrastructureError(
      `Database connectivity error: ${message}`,
      err,
    );
  }

  return err instanceof Error ? err : new Error(message);
}

export function isLockConflictError(err: unknown): boolean {
  const { code } = extractPgError(err);
  return code === "40P01" || code === "55P03" || code === "40001";
}
