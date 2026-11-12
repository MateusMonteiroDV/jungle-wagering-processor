import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Inject,
  Param,
  Post,
  Query,
} from "@nestjs/common";
import { CreateWalletUseCase } from "../../../application/use-cases/create-wallet";
import { GetLedgerQuery } from "../../../application/use-cases/queries/get-ledger";
import { GetWalletQuery } from "../../../application/use-cases/queries/get-wallet";
import { ReconcileWalletUseCase } from "../../../application/use-cases/reconcile-wallet";
import { DomainError } from "../../../domain/errors";
import {
  getLogContext,
  updateLogContext,
} from "../../../infrastructure/observability/context";
import { AppLogger } from "../../../infrastructure/observability/logger";
import {
  createWalletSchema,
  parseWithValidation,
  uuidParamSchema,
} from "../dto/schemas";

@Controller("wallets")
export class WalletsController {
  constructor(
    @Inject(CreateWalletUseCase)
    private readonly createWallet: CreateWalletUseCase,
    @Inject(GetWalletQuery)
    private readonly getWallet: GetWalletQuery,
    @Inject(GetLedgerQuery)
    private readonly getLedger: GetLedgerQuery,
    @Inject(ReconcileWalletUseCase)
    private readonly reconcileWallet: ReconcileWalletUseCase,
    @Inject(AppLogger)
    private readonly logger: AppLogger,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(@Body() body: unknown) {
    const dto = parseWithValidation(createWalletSchema, body);
    const correlationId = getLogContext().correlationId ?? "http";
    const result = await this.createWallet.execute({
      playerId: dto.playerId,
      initialBalance: dto.initialBalance,
      correlationId,
    });
    updateLogContext({ walletId: result.id });
    this.logger.info({ walletId: result.id, version: result.version }, "Wallet created");
    return result;
  }

  @Get(":walletId")
  async findOne(@Param("walletId") walletId: string) {
    const validId = parseWithValidation(uuidParamSchema, walletId);
    updateLogContext({ walletId: validId });
    return this.getWallet.execute(validId);
  }

  @Get(":walletId/ledger")
  async findLedger(
    @Param("walletId") walletId: string,
    @Query("cursor") cursor?: string,
    @Query("limit") limitRaw?: string,
  ) {
    const validId = parseWithValidation(uuidParamSchema, walletId);
    updateLogContext({ walletId: validId });
    let limit = 50;
    if (limitRaw !== undefined && limitRaw !== "") {
      if (!/^\d+$/.test(limitRaw)) {
        throw new DomainError("VALIDATION_ERROR", "limit must be a positive integer");
      }
      limit = parseInt(limitRaw, 10);
    }
    return this.getLedger.execute({ walletId: validId, cursor, limit });
  }

  @Post(":walletId/reconciliation")
  @HttpCode(HttpStatus.OK)
  async reconcile(@Param("walletId") walletId: string) {
    const validId = parseWithValidation(uuidParamSchema, walletId);
    updateLogContext({ walletId: validId });
    return this.reconcileWallet.execute(validId);
  }
}
