import { MikroORM, SqlEntityManager } from "@mikro-orm/postgresql";
import {
  MetricsPort,
  TransactionalContext,
  UnitOfWork,
} from "../../application/ports";
import { isLockConflictError, translatePgError } from "./pg-errors";
import {
  PgInboxRepository,
  PgLedgerRepository,
  PgOutboxRepository,
  PgWagerTransactionRepository,
  PgWalletRepository,
} from "./repositories";

export class MikroOrmUnitOfWork implements UnitOfWork {
  constructor(
    private readonly orm: MikroORM,
    private readonly lockTimeoutMs = 5000,
    private readonly statementTimeoutMs = 10000,
    private readonly metrics?: MetricsPort,
  ) {}

  async runInTransaction<T>(
    work: (ctx: TransactionalContext) => Promise<T>,
  ): Promise<T> {
    const fork = this.orm.em.fork({ clear: true }) as SqlEntityManager;
    try {
      return await fork.transactional(async (txEm) => {
        const sqlEm = txEm as SqlEntityManager;
        await sqlEm.execute(`SET LOCAL lock_timeout = '${this.lockTimeoutMs}ms'`);
        await sqlEm.execute(
          `SET LOCAL statement_timeout = '${this.statementTimeoutMs}ms'`,
        );

        const ctx: TransactionalContext = {
          wallets: new PgWalletRepository(sqlEm),
          transactions: new PgWagerTransactionRepository(sqlEm),
          ledger: new PgLedgerRepository(sqlEm),
          inbox: new PgInboxRepository(sqlEm),
          outbox: new PgOutboxRepository(sqlEm),
        };

        const result = await work(ctx);
        await sqlEm.execute("SET CONSTRAINTS ALL IMMEDIATE");
        return result;
      });
    } catch (err) {
      if (isLockConflictError(err)) {
        this.metrics?.incrementLockConflict();
      }
      throw translatePgError(err);
    }
  }
}
