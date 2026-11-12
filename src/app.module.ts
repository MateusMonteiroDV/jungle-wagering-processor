import { MikroORM } from "@mikro-orm/postgresql";
import {
  DynamicModule,
  Inject,
  Module,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { APP_FILTER, APP_GUARD } from "@nestjs/core";
import { uuidv7 } from "uuidv7";
import {
  ClockPort,
  IdGeneratorPort,
  MetricsPort,
  TOKENS,
  UnitOfWork,
} from "./application/ports";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet";
import { ProcessWagerTransactionUseCase } from "./application/use-cases/process-wager-transaction";
import { GetLedgerQuery } from "./application/use-cases/queries/get-ledger";
import { GetTransactionQuery } from "./application/use-cases/queries/get-transaction";
import { GetWalletQuery } from "./application/use-cases/queries/get-wallet";
import { ReconcileWalletUseCase } from "./application/use-cases/reconcile-wallet";
import { AppEnv, loadEnv } from "./infrastructure/config/env";
import { SqsQueueManager } from "./infrastructure/messaging/sqs-client";
import { AppLogger } from "./infrastructure/observability/logger";
import { PrometheusMetrics } from "./infrastructure/observability/metrics";
import { createMikroOrmOptions } from "./infrastructure/persistence/mikro-orm.config";
import { MikroOrmUnitOfWork } from "./infrastructure/persistence/unit-of-work";
import {
  AuthGuard,
  NoOpProviderIdentityAdapter,
} from "./interfaces/http/auth.guard";
import { HealthController } from "./interfaces/http/controllers/health.controller";
import { MetricsController } from "./interfaces/http/controllers/metrics.controller";
import { ProvidersController } from "./interfaces/http/controllers/providers.controller";
import { WageringController } from "./interfaces/http/controllers/wagering.controller";
import { WalletsController } from "./interfaces/http/controllers/wallets.controller";
import { ErrorMapperFilter } from "./interfaces/http/error-mapper.filter";

export const ENV_TOKEN = Symbol.for("AppEnv");

@Module({})
export class AppModule implements OnModuleInit, OnModuleDestroy {
  constructor(
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Inject(SqsQueueManager) private readonly sqs: SqsQueueManager,
  ) {}

  static register(envOverrides?: Record<string, string | undefined>): DynamicModule {
    const env = loadEnv(envOverrides);

    return {
      module: AppModule,
      controllers: [
        HealthController,
        MetricsController,
        WalletsController,
        WageringController,
        ProvidersController,
      ],
      providers: [
        {
          provide: ENV_TOKEN,
          useValue: env,
        },
        {
          provide: AppLogger,
          useFactory: () => new AppLogger(env.LOG_LEVEL, env.INSTANCE_ID),
        },
        {
          provide: PrometheusMetrics,
          useFactory: () => new PrometheusMetrics(),
        },
        {
          provide: TOKENS.MetricsPort,
          useExisting: PrometheusMetrics,
        },
        {
          provide: TOKENS.ClockPort,
          useValue: { now: () => new Date() } satisfies ClockPort,
        },
        {
          provide: TOKENS.IdGeneratorPort,
          useValue: { generate: () => uuidv7() } satisfies IdGeneratorPort,
        },
        {
          provide: TOKENS.ProviderIdentityPort,
          useClass: NoOpProviderIdentityAdapter,
        },
        {
          provide: MikroORM,
          useFactory: async () => {
            return MikroORM.init(createMikroOrmOptions(env));
          },
        },
        {
          provide: SqsQueueManager,
          useFactory: () => new SqsQueueManager(env),
        },
        {
          provide: TOKENS.UnitOfWork,
          useFactory: (orm: MikroORM, metrics: MetricsPort) =>
            new MikroOrmUnitOfWork(
              orm,
              env.DB_LOCK_TIMEOUT_MS,
              env.DB_STATEMENT_TIMEOUT_MS,
              metrics,
            ),
          inject: [MikroORM, TOKENS.MetricsPort],
        },
        {
          provide: CreateWalletUseCase,
          useFactory: (
            uow: UnitOfWork,
            clock: ClockPort,
            idGen: IdGeneratorPort,
          ) => new CreateWalletUseCase(uow, clock, idGen),
          inject: [TOKENS.UnitOfWork, TOKENS.ClockPort, TOKENS.IdGeneratorPort],
        },
        {
          provide: ProcessWagerTransactionUseCase,
          useFactory: (
            uow: UnitOfWork,
            clock: ClockPort,
            idGen: IdGeneratorPort,
            metrics: MetricsPort,
          ) =>
            new ProcessWagerTransactionUseCase(uow, clock, idGen, metrics, {
              maxAttempts: env.PENDING_REF_MAX_ATTEMPTS,
              ttlMs: env.PENDING_REF_TTL_MS,
              baseDelayMs: env.PENDING_REF_BASE_DELAY_MS,
            }),
          inject: [
            TOKENS.UnitOfWork,
            TOKENS.ClockPort,
            TOKENS.IdGeneratorPort,
            TOKENS.MetricsPort,
          ],
        },
        {
          provide: ReconcileWalletUseCase,
          useFactory: (
            uow: UnitOfWork,
            metrics: MetricsPort,
            logger: AppLogger,
          ) => new ReconcileWalletUseCase(uow, metrics, logger),
          inject: [TOKENS.UnitOfWork, TOKENS.MetricsPort, AppLogger],
        },
        {
          provide: GetWalletQuery,
          useFactory: (uow: UnitOfWork) => new GetWalletQuery(uow),
          inject: [TOKENS.UnitOfWork],
        },
        {
          provide: GetLedgerQuery,
          useFactory: (uow: UnitOfWork) => new GetLedgerQuery(uow),
          inject: [TOKENS.UnitOfWork],
        },
        {
          provide: GetTransactionQuery,
          useFactory: (uow: UnitOfWork) => new GetTransactionQuery(uow),
          inject: [TOKENS.UnitOfWork],
        },
        {
          provide: APP_FILTER,
          useFactory: (logger: AppLogger) => new ErrorMapperFilter(logger),
          inject: [AppLogger],
        },
        {
          provide: APP_GUARD,
          useClass: AuthGuard,
        },
      ],
      exports: [
        ENV_TOKEN,
        MikroORM,
        SqsQueueManager,
        AppLogger,
        PrometheusMetrics,
        TOKENS.UnitOfWork,
        CreateWalletUseCase,
        ProcessWagerTransactionUseCase,
        ReconcileWalletUseCase,
      ],
    };
  }

  async onModuleInit(): Promise<void> {
    try {
      await this.sqs.ensureQueues();
    } catch {
      // Readiness probe will surface SQS unavailability if SQS is down at startup
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.orm.close(true);
  }
}
