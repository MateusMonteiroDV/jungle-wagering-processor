import {
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Inject,
  Param,
  Post,
  Res,
} from "@nestjs/common";
import type { Response } from "express";
import { ProcessWagerTransactionUseCase } from "../../../application/use-cases/process-wager-transaction";
import { GetTransactionQuery } from "../../../application/use-cases/queries/get-transaction";
import { DomainError } from "../../../domain/errors";
import { WagerTransactionStatus } from "../../../domain/wager/wager-transaction";
import {
  getLogContext,
  updateLogContext,
} from "../../../infrastructure/observability/context";
import { AppLogger } from "../../../infrastructure/observability/logger";
import {
  parseWithValidation,
  submitWagerTransactionSchema,
  uuidParamSchema,
} from "../dto/schemas";

@Controller("wagering/transactions")
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransactionUseCase)
    private readonly processWager: ProcessWagerTransactionUseCase,
    @Inject(GetTransactionQuery)
    private readonly getTransaction: GetTransactionQuery,
    @Inject(AppLogger)
    private readonly logger: AppLogger,
  ) {}

  @Post()
  async submit(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) res: Response,
  ) {
    if (!idempotencyKey || idempotencyKey.trim() === "") {
      throw new DomainError(
        "IDEMPOTENCY_KEY_MISSING",
        "Header 'Idempotency-Key' is required",
      );
    }

    const dto = parseWithValidation(submitWagerTransactionSchema, body);
    const correlationId = getLogContext().correlationId ?? "http";
    updateLogContext({
      walletId: dto.walletId,
      providerId: dto.providerId,
    });

    const result = await this.processWager.execute(dto, {
      source: "HTTP",
      idempotencyKey: idempotencyKey.trim(),
      correlationId,
    });

    updateLogContext({ transactionId: result.transactionId });
    this.logger.info(
      {
        transactionId: result.transactionId,
        walletId: dto.walletId,
        providerId: dto.providerId,
        kind: dto.kind,
        status: result.status,
        failureCode: result.failureCode,
        idempotentReplay: result.idempotentReplay,
      },
      "Processed HTTP wager transaction",
    );

    if (result.status === WagerTransactionStatus.Rejected) {
      res.status(HttpStatus.UNPROCESSABLE_ENTITY);
    } else if (result.status === WagerTransactionStatus.PendingReference) {
      res.status(HttpStatus.ACCEPTED);
    } else if (result.idempotentReplay) {
      res.status(HttpStatus.OK);
    } else {
      res.status(HttpStatus.CREATED);
    }

    return result;
  }

  @Get(":transactionId")
  async findById(@Param("transactionId") transactionId: string) {
    const validId = parseWithValidation(uuidParamSchema, transactionId);
    updateLogContext({ transactionId: validId });
    return this.getTransaction.executeById(validId);
  }
}
