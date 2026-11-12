import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Inject,
  Optional,
} from "@nestjs/common";
import type { Request, Response } from "express";
import {
  DomainError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidReferenceError,
  TransactionNotFoundError,
  TransientInfrastructureError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from "../../domain/errors";
import { getLogContext } from "../../infrastructure/observability/context";
import { AppLogger } from "../../infrastructure/observability/logger";

@Catch()
export class ErrorMapperFilter implements ExceptionFilter {
  constructor(@Optional() @Inject(AppLogger) private readonly logger?: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const res = ctx.getResponse<Response>();
    const req = ctx.getRequest<Request>();
    const correlationId = getLogContext().correlationId;
    if (correlationId && !res.getHeader("X-Correlation-Id")) {
      res.setHeader("X-Correlation-Id", correlationId);
    }

    if (exception instanceof TransientInfrastructureError) {
      this.logger?.warn(
        { path: req?.url, code: exception.code, message: exception.message },
        "Transient infrastructure error mapped to 503",
      );
      res.setHeader("Retry-After", "1");
      res.status(HttpStatus.SERVICE_UNAVAILABLE).json({
        error: {
          code: "SERVICE_UNAVAILABLE",
          message: exception.message,
        },
      });
      return;
    }

    if (
      exception instanceof WalletNotFoundError ||
      exception instanceof TransactionNotFoundError
    ) {
      res.status(HttpStatus.NOT_FOUND).json({
        error: {
          code: exception.code,
          message: exception.message,
        },
      });
      return;
    }

    if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof WalletAlreadyExistsError
    ) {
      res.status(HttpStatus.CONFLICT).json({
        error: {
          code: exception.code,
          message: exception.message,
        },
      });
      return;
    }

    if (
      exception instanceof InvalidMoneyError ||
      exception instanceof InvalidReferenceError
    ) {
      res.status(HttpStatus.BAD_REQUEST).json({
        error: {
          code: "VALIDATION_ERROR",
          message: exception.message,
        },
      });
      return;
    }

    if (exception instanceof DomainError) {
      const status =
        exception.code === "VALIDATION_ERROR" ||
        exception.code === "IDEMPOTENCY_KEY_MISSING" ||
        exception.code === "OPENING_KIND_FORBIDDEN"
          ? HttpStatus.BAD_REQUEST
          : HttpStatus.UNPROCESSABLE_ENTITY;

      const details = (exception as DomainError & { details?: unknown }).details;
      res.status(status).json({
        error: {
          code: exception.code,
          message: exception.message,
          ...(details ? { details } : {}),
        },
      });
      return;
    }

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      res.status(status).json({
        error: {
          code: status === 400 ? "VALIDATION_ERROR" : "HTTP_ERROR",
          message: exception.message,
        },
      });
      return;
    }

    const msg =
      exception instanceof Error ? exception.message : "Internal server error";
    this.logger?.error(
      { path: req?.url, errorMessage: msg },
      "Unhandled exception in HTTP request",
    );
    res.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "An unexpected internal error occurred",
      },
    });
  }
}
