import { WalletAlreadyExistsError } from "../../domain/errors";
import { WalletBalanceChanged } from "../../domain/events";
import { OutboxMessage } from "../../domain/messaging/outbox-message";
import { Money, MoneyProps } from "../../domain/money/money";
import {
  WagerTransaction,
  WagerTransactionKind,
} from "../../domain/wager/wager-transaction";
import { Wallet } from "../../domain/wallet/wallet";
import { computeWagerPayloadHash } from "../payload-hash";
import { ClockPort, IdGeneratorPort, UnitOfWork } from "../ports";

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId: string;
}

export interface CreateWalletResult {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export class CreateWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: ClockPort,
    private readonly idGen: IdGeneratorPort,
  ) {}

  async execute(command: CreateWalletCommand): Promise<CreateWalletResult> {
    const initialMoney = Money.from(command.initialBalance);
    const now = this.clock.now();
    const walletId = this.idGen.generate();

    const wallet = Wallet.open({
      id: walletId,
      playerId: command.playerId,
      initialBalance: initialMoney,
      createdAt: now,
    });

    await this.uow.runInTransaction(async (tx) => {
      const existing = await tx.wallets.findByPlayerAndCurrency(
        command.playerId,
        initialMoney.currency,
      );
      if (existing) {
        throw new WalletAlreadyExistsError(
          command.playerId,
          initialMoney.currency,
        );
      }

      await tx.wallets.insert(wallet);

      if (initialMoney.isPositive()) {
        const openingTxId = this.idGen.generate();
        const externalId = `opening:${wallet.id}`;
        const payloadHash = computeWagerPayloadHash({
          providerId: "internal",
          externalTransactionId: externalId,
          playerId: command.playerId,
          walletId: wallet.id,
          roundId: "opening",
          gameId: "opening",
          kind: WagerTransactionKind.Opening,
          money: initialMoney.toJSON(),
        });

        const openingTx = WagerTransaction.createOpening({
          id: openingTxId,
          walletId: wallet.id,
          playerId: command.playerId,
          money: initialMoney,
          payloadHash,
          correlationId: command.correlationId,
          createdAt: now,
        });

        const openingEntry = wallet.createOpeningEntry({
          entryId: this.idGen.generate(),
          transactionId: openingTxId,
        })!;

        await tx.transactions.insert(openingTx);
        await tx.ledger.insert(openingEntry);

        const balanceEvent = WalletBalanceChanged.from(wallet, openingEntry, {
          eventId: this.idGen.generate(),
          correlationId: command.correlationId,
          causationId: openingTxId,
          occurredAt: now,
        });
        await tx.outbox.insert(OutboxMessage.enqueue(balanceEvent));
      }
    });

    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }
}
