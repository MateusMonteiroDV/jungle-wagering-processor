import { LockMode } from "@mikro-orm/core";
import { SqlEntityManager } from "@mikro-orm/postgresql";
import { WalletRepository } from "../../../application/ports";
import { OptimisticConcurrencyError } from "../../../domain/errors";
import { Wallet } from "../../../domain/wallet/wallet";
import { WalletMapper } from "../mappers";
import { WalletRecord } from "../schemas";

export class PgWalletRepository implements WalletRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async findById(id: string): Promise<Wallet | null> {
    const rec = await this.em.findOne(
      WalletRecord,
      { id },
      { disableIdentityMap: true },
    );
    return rec ? WalletMapper.toDomain(rec) : null;
  }

  async findByIdForUpdate(id: string): Promise<Wallet | null> {
    const rec = await this.em.findOne(
      WalletRecord,
      { id },
      {
        lockMode: LockMode.PESSIMISTIC_WRITE,
        disableIdentityMap: true,
      },
    );
    return rec ? WalletMapper.toDomain(rec) : null;
  }

  async findByIdForShare(id: string): Promise<Wallet | null> {
    const rec = await this.em.findOne(
      WalletRecord,
      { id },
      {
        lockMode: LockMode.PESSIMISTIC_READ,
        disableIdentityMap: true,
      },
    );
    return rec ? WalletMapper.toDomain(rec) : null;
  }

  async findByPlayerAndCurrency(
    playerId: string,
    currency: string,
  ): Promise<Wallet | null> {
    const rec = await this.em.findOne(
      WalletRecord,
      { playerId, currency },
      { disableIdentityMap: true },
    );
    return rec ? WalletMapper.toDomain(rec) : null;
  }

  async insert(wallet: Wallet): Promise<void> {
    await this.em.execute(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        wallet.id,
        wallet.playerId,
        wallet.currency,
        wallet.balance.amount,
        wallet.version,
        wallet.createdAt.toISOString(),
        wallet.updatedAt.toISOString(),
      ],
    );
    wallet.markPersisted();
  }

  async updateWithVersion(wallet: Wallet): Promise<void> {
    const rows = await this.em.execute(
      `UPDATE wallets
       SET balance = ?, version = ?, updated_at = ?
       WHERE id = ? AND version = ?
       RETURNING id`,
      [
        wallet.balance.amount,
        wallet.version,
        wallet.updatedAt.toISOString(),
        wallet.id,
        wallet.loadedVersion,
      ],
    );
    if (!Array.isArray(rows) || rows.length !== 1) {
      throw new OptimisticConcurrencyError(wallet.id);
    }
    wallet.markPersisted();
  }
}
