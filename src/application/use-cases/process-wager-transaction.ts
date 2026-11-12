import {
  DomainError,
  IdempotencyConflictError,
  InsufficientFundsError,
  PermanentMessageError,
  TransientInfrastructureError,
  WalletNotFoundError,
} from "../../domain/errors";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../../domain/events";
import { LedgerDirection } from "../../domain/ledger/wallet-ledger-entry";
import { InboxMessage } from "../../domain/messaging/inbox-message";
import { OutboxMessage } from "../../domain/messaging/outbox-message";
import { Money, MoneyProps } from "../../domain/money/money";
import { FailureCode } from "../../domain/wager/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/wager/wager-transaction";
import { Wallet } from "../../domain/wallet/wallet";
import { computeWagerPayloadHash } from "../payload-hash";
import {
  ClockPort,
  IdGeneratorPort,
  MetricsPort,
  ProcessingSource,
  TransactionalContext,
  UnitOfWork,
} from "../ports";

export interface ProcessWagerCommand {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface ProcessWagerContext {
  source: ProcessingSource;
  idempotencyKey: string;
  correlationId: string;
  inbox?: {
    consumerName: string;
    messageId: string;
  };
  pendingConfig?: {
    maxAttempts: number;
    ttlMs: number;
    baseDelayMs: number;
  };
}

export interface ProcessWagerResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
  inboxDuplicate?: boolean;
}

export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: ClockPort,
    private readonly idGen: IdGeneratorPort,
    private readonly metrics: MetricsPort,
    private readonly defaultPendingConfig = {
      maxAttempts: 10,
      ttlMs: 15 * 60 * 1000,
      baseDelayMs: 1000,
    },
  ) {}

  async execute(
    command: ProcessWagerCommand,
    context: ProcessWagerContext,
  ): Promise<ProcessWagerResult> {
    const startMs = Date.now();
    try {
      return await this.executeWithRetry(command, context);
    } finally {
      const elapsedSec = (Date.now() - startMs) / 1000;
      this.metrics.observeProcessingDuration(context.source, elapsedSec);
    }
  }

  private async executeWithRetry(
    command: ProcessWagerCommand,
    context: ProcessWagerContext,
  ): Promise<ProcessWagerResult> {
    if (!context.idempotencyKey || context.idempotencyKey.trim() === "") {
      throw new DomainError("IDEMPOTENCY_KEY_MISSING", "Idempotency-Key is required");
    }

    if (command.kind === WagerTransactionKind.Opening) {
      throw new DomainError(
        "OPENING_KIND_FORBIDDEN",
        "OPENING transactions cannot be submitted externally",
      );
    }

    const money = Money.from(command.money);
    const payloadHash = computeWagerPayloadHash({
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      playerId: command.playerId,
      walletId: command.walletId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money: money.toJSON(),
      referenceExternalTransactionId: command.referenceExternalTransactionId,
    });

    const maxTransientRetries = 3;
    for (let attempt = 1; attempt <= maxTransientRetries; attempt++) {
      try {
        return await this.uow.runInTransaction((tx) =>
          this.executeInTx(tx, command, money, payloadHash, context),
        );
      } catch (err) {
        if (
          err instanceof TransientInfrastructureError &&
          attempt < maxTransientRetries
        ) {
          this.metrics.incrementRetry("transient_db_conflict");
          await new Promise((r) => setTimeout(r, 15 * attempt));
          continue;
        }
        throw err;
      }
    }
    throw new TransientInfrastructureError("Exhausted transient retries");
  }

  private async executeInTx(
    tx: TransactionalContext,
    command: ProcessWagerCommand,
    money: Money,
    payloadHash: string,
    context: ProcessWagerContext,
  ): Promise<ProcessWagerResult> {
    const now = this.clock.now();
    const pendingCfg = context.pendingConfig ?? this.defaultPendingConfig;

    // 1. Inbox deduplication for SQS messages
    if (context.inbox) {
      const inboxMsg = InboxMessage.receive({
        consumerName: context.inbox.consumerName,
        messageId: context.inbox.messageId,
        payloadHash,
        receivedAt: now,
      });
      const inboxResult = await tx.inbox.tryInsert(inboxMsg);
      if (!inboxResult.inserted && inboxResult.existing) {
        if (inboxResult.existing.payloadHash !== payloadHash) {
          throw new PermanentMessageError(
            `Inbox message ${context.inbox.messageId} already received with a different payloadHash`,
            "INBOX_PAYLOAD_CONFLICT",
          );
        }
        this.metrics.incrementInboxDuplicate();
        const existingTx =
          (await tx.transactions.findByIdempotencyKey(context.idempotencyKey)) ??
          (await tx.transactions.findByProviderAndExternalId(
            command.providerId,
            command.externalTransactionId,
          ));
        if (existingTx) {
          const wallet = await tx.wallets.findById(existingTx.walletId);
          const balance = existingTx.resultingBalance ?? wallet?.balance ?? money;
          return {
            transactionId: existingTx.id,
            status: existingTx.status,
            balance: balance.toJSON(),
            failureCode: existingTx.failureCode,
            idempotentReplay: true,
            inboxDuplicate: true,
          };
        }
      }
    }

    // 2. Pessimistic lock on Wallet (unit of concurrency = walletId)
    const wallet = await tx.wallets.findByIdForUpdate(command.walletId);
    if (!wallet) {
      throw new WalletNotFoundError(command.walletId);
    }

    // 3. Persistent idempotency check (now serialized by the wallet lock)
    const existingByKey = await tx.transactions.findByIdempotencyKey(
      context.idempotencyKey,
    );
    const existingByExternal = await tx.transactions.findByProviderAndExternalId(
      command.providerId,
      command.externalTransactionId,
    );

    if (existingByKey) {
      if (!existingByKey.matchesPayload(payloadHash)) {
        throw new IdempotencyConflictError(
          context.idempotencyKey,
          "Idempotency-Key reused with divergent payload",
        );
      }
      if (existingByExternal && existingByExternal.id !== existingByKey.id) {
        throw new IdempotencyConflictError(
          context.idempotencyKey,
          "externalTransactionId belongs to a different Idempotency-Key",
        );
      }

      const isWorkerResolvingPending =
        context.source === "WORKER" &&
        existingByKey.status === WagerTransactionStatus.PendingReference;

      if (!isWorkerResolvingPending) {
        if (context.inbox) {
          await tx.inbox.markProcessed(
            context.inbox.consumerName,
            context.inbox.messageId,
            now,
          );
        }
        this.metrics.incrementIdempotentReplay(context.source);
        return {
          transactionId: existingByKey.id,
          status: existingByKey.status,
          balance: (existingByKey.resultingBalance ?? wallet.balance).toJSON(),
          failureCode: existingByKey.failureCode,
          idempotentReplay: true,
        };
      }
    } else if (existingByExternal) {
      throw new IdempotencyConflictError(
        context.idempotencyKey,
        `Transaction (${command.providerId}, ${command.externalTransactionId}) already exists with another Idempotency-Key`,
      );
    }

    // 4. Instantiate or lock WagerTransaction
    const isExistingPendingTx = Boolean(existingByKey);
    const wagerTx = existingByKey
      ? ((await tx.transactions.findByIdForUpdate(existingByKey.id)) ?? existingByKey)
      : WagerTransaction.create({
          id: this.idGen.generate(),
          providerId: command.providerId,
          externalTransactionId: command.externalTransactionId,
          idempotencyKey: context.idempotencyKey,
          payloadHash,
          walletId: command.walletId,
          playerId: command.playerId,
          roundId: command.roundId,
          gameId: command.gameId,
          kind: command.kind,
          money,
          referenceExternalTransactionId: command.referenceExternalTransactionId,
          correlationId: context.correlationId,
          createdAt: now,
        });

    if (wagerTx.isTerminal()) {
      return {
        transactionId: wagerTx.id,
        status: wagerTx.status,
        balance: (wagerTx.resultingBalance ?? wallet.balance).toJSON(),
        failureCode: wagerTx.failureCode,
        idempotentReplay: true,
      };
    }

    const persistWagerTx = async () => {
      if (isExistingPendingTx) {
        await tx.transactions.update(wagerTx);
      } else {
        await tx.transactions.insert(wagerTx);
      }
    };

    const rejectAndPersist = async (
      failureCode: FailureCode,
    ): Promise<ProcessWagerResult> => {
      wagerTx.reject(failureCode, now, wallet.balance);
      await persistWagerTx();

      const rejectedEvent = WagerTransactionRejected.from(wagerTx, {
        eventId: this.idGen.generate(),
        correlationId: context.correlationId,
        causationId: wagerTx.id,
        occurredAt: now,
      });
      await tx.outbox.insert(OutboxMessage.enqueue(rejectedEvent));

      if (context.inbox) {
        await tx.inbox.markProcessed(
          context.inbox.consumerName,
          context.inbox.messageId,
          now,
        );
      }

      this.metrics.incrementTransaction(
        WagerTransactionStatus.Rejected,
        wagerTx.kind,
        context.source,
      );

      return {
        transactionId: wagerTx.id,
        status: WagerTransactionStatus.Rejected,
        balance: wallet.balance.toJSON(),
        failureCode,
        idempotentReplay: false,
      };
    };

    // 5. Business validations: player ownership and currency match
    if (wallet.playerId !== wagerTx.playerId) {
      return rejectAndPersist(FailureCode.PlayerWalletMismatch);
    }
    if (wallet.currency !== wagerTx.money.currency) {
      return rejectAndPersist(FailureCode.CurrencyMismatch);
    }

    // 6. Reference resolution & validation (REFUND, ROLLBACK required; WIN optional)
    let referenceTx: WagerTransaction | undefined;
    if (
      wagerTx.requiresReference() ||
      (wagerTx.referenceExternalTransactionId !== undefined &&
        wagerTx.referenceExternalTransactionId !== "")
    ) {
      const refExtId = wagerTx.referenceExternalTransactionId!;
      const foundRef = await tx.transactions.findByProviderAndExternalId(
        wagerTx.providerId,
        refExtId,
      );

      const isRefNotReady =
        !foundRef ||
        foundRef.status === WagerTransactionStatus.Pending ||
        foundRef.status === WagerTransactionStatus.PendingReference;

      if (isRefNotReady) {
        if (context.source === "WORKER") {
          const ageMs = now.getTime() - wagerTx.createdAt.getTime();
          const nextAttempts = wagerTx.attempts + 1;
          if (
            nextAttempts >= pendingCfg.maxAttempts ||
            ageMs >= pendingCfg.ttlMs
          ) {
            return rejectAndPersist(FailureCode.ReferenceNotFound);
          }
          wagerTx.schedulePendingRetry(
            now,
            pendingCfg.baseDelayMs,
            60000,
            wallet.balance,
          );
          await persistWagerTx();
          return {
            transactionId: wagerTx.id,
            status: WagerTransactionStatus.PendingReference,
            balance: wallet.balance.toJSON(),
            idempotentReplay: false,
          };
        }

        const nextAttempt = new Date(now.getTime() + pendingCfg.baseDelayMs);
        wagerTx.markPendingReference(nextAttempt, wallet.balance);
        await persistWagerTx();

        const pendingEvent = WagerTransactionPendingReference.from(wagerTx, {
          eventId: this.idGen.generate(),
          correlationId: context.correlationId,
          causationId: wagerTx.id,
          occurredAt: now,
        });
        await tx.outbox.insert(OutboxMessage.enqueue(pendingEvent));

        if (context.inbox) {
          await tx.inbox.markProcessed(
            context.inbox.consumerName,
            context.inbox.messageId,
            now,
          );
        }

        this.metrics.incrementTransaction(
          WagerTransactionStatus.PendingReference,
          wagerTx.kind,
          context.source,
        );
        this.metrics.incrementPendingReference();

        return {
          transactionId: wagerTx.id,
          status: WagerTransactionStatus.PendingReference,
          balance: wallet.balance.toJSON(),
          idempotentReplay: false,
        };
      }

      referenceTx = foundRef;

      // Rule 7.2: Reference must belong to same provider, player, wallet, currency, and round
      if (
        referenceTx.providerId !== wagerTx.providerId ||
        referenceTx.playerId !== wagerTx.playerId ||
        referenceTx.walletId !== wagerTx.walletId ||
        referenceTx.money.currency !== wagerTx.money.currency ||
        referenceTx.roundId !== wagerTx.roundId
      ) {
        return rejectAndPersist(FailureCode.ReferenceMismatch);
      }

      // Rule 7.3: REFUND only references BET; ROLLBACK references BET, WIN, or REFUND; WIN references BET
      if (
        wagerTx.kind === WagerTransactionKind.Refund &&
        referenceTx.kind !== WagerTransactionKind.Bet
      ) {
        return rejectAndPersist(FailureCode.ReferenceInvalidKind);
      }
      if (
        wagerTx.kind === WagerTransactionKind.Rollback &&
        referenceTx.kind !== WagerTransactionKind.Bet &&
        referenceTx.kind !== WagerTransactionKind.Win &&
        referenceTx.kind !== WagerTransactionKind.Refund
      ) {
        return rejectAndPersist(FailureCode.ReferenceInvalidKind);
      }
      if (
        wagerTx.kind === WagerTransactionKind.Win &&
        referenceTx.kind !== WagerTransactionKind.Bet
      ) {
        return rejectAndPersist(FailureCode.ReferenceInvalidKind);
      }

      // Must be PROCESSED
      if (referenceTx.status !== WagerTransactionStatus.Processed) {
        return rejectAndPersist(FailureCode.ReferenceNotProcessed);
      }

      // Reversal-specific checks (REFUND / ROLLBACK)
      if (
        wagerTx.kind === WagerTransactionKind.Refund ||
        wagerTx.kind === WagerTransactionKind.Rollback
      ) {
        // Rule 7.5: Amount of REFUND/ROLLBACK must equal reference amount
        if (!wagerTx.money.equals(referenceTx.money)) {
          return rejectAndPersist(FailureCode.ReferenceAmountMismatch);
        }

        // Rule 7.4: Reference cannot be reversed more than once
        const existingReversal =
          await tx.transactions.findProcessedReversalForReference(referenceTx.id);
        if (existingReversal) {
          return rejectAndPersist(FailureCode.ReferenceAlreadyReversed);
        }
      }
    }

    // 7. Apply financial movement and persist atomically
    if (!wagerTx.affectsBalance()) {
      // LOSS: records outcome without changing wallet balance or creating ledger entry
      wagerTx.markProcessed(referenceTx?.id, now, wallet.balance);
      await persistWagerTx();

      const processedEvent = WagerTransactionProcessed.from(wagerTx, wallet, {
        eventId: this.idGen.generate(),
        correlationId: context.correlationId,
        causationId: wagerTx.id,
        occurredAt: now,
      });
      await tx.outbox.insert(OutboxMessage.enqueue(processedEvent));
    } else {
      const direction = wagerTx.ledgerDirectionFor(referenceTx);
      let ledgerEntry;
      try {
        if (direction === LedgerDirection.Debit) {
          ledgerEntry = wallet.debit({
            entryId: this.idGen.generate(),
            transactionId: wagerTx.id,
            money: wagerTx.money,
            occurredAt: now,
          });
        } else {
          ledgerEntry = wallet.credit({
            entryId: this.idGen.generate(),
            transactionId: wagerTx.id,
            money: wagerTx.money,
            occurredAt: now,
          });
        }
      } catch (err) {
        if (err instanceof InsufficientFundsError) {
          const code =
            wagerTx.kind === WagerTransactionKind.Rollback
              ? FailureCode.ReversalInsufficientFunds
              : FailureCode.InsufficientFunds;
          return rejectAndPersist(code);
        }
        throw err;
      }

      wagerTx.markProcessed(referenceTx?.id, now, wallet.balance);
      await persistWagerTx();
      await tx.wallets.updateWithVersion(wallet);
      await tx.ledger.insert(ledgerEntry);

      const processedEvent = WagerTransactionProcessed.from(wagerTx, wallet, {
        eventId: this.idGen.generate(),
        correlationId: context.correlationId,
        causationId: wagerTx.id,
        occurredAt: now,
      });
      const balanceEvent = WalletBalanceChanged.from(wallet, ledgerEntry, {
        eventId: this.idGen.generate(),
        correlationId: context.correlationId,
        causationId: wagerTx.id,
        occurredAt: now,
      });
      await tx.outbox.insertMany([
        OutboxMessage.enqueue(processedEvent),
        OutboxMessage.enqueue(balanceEvent),
      ]);
    }

    if (context.inbox) {
      await tx.inbox.markProcessed(
        context.inbox.consumerName,
        context.inbox.messageId,
        now,
      );
    }

    this.metrics.incrementTransaction(
      WagerTransactionStatus.Processed,
      wagerTx.kind,
      context.source,
    );

    return {
      transactionId: wagerTx.id,
      status: WagerTransactionStatus.Processed,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
    };
  }
}
