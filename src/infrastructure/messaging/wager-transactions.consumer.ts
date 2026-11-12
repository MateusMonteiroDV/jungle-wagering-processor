import { Message } from "@aws-sdk/client-sqs";
import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from "@nestjs/common";
import { z } from "zod";
import { TOKENS } from "../../application/ports";
import type { MetricsPort } from "../../application/ports";
import { ProcessWagerTransactionUseCase } from "../../application/use-cases/process-wager-transaction";
import {
  DomainError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidReferenceError,
  PermanentMessageError,
  WalletNotFoundError,
} from "../../domain/errors";
import { WagerTransactionKind } from "../../domain/wager/wager-transaction";
import {
  parseWithValidation,
  submitWagerTransactionSchema,
} from "../../interfaces/http/dto/schemas";
import { ENV_TOKEN } from "../config/env";
import type { AppEnv } from "../config/env";
import { triggerFaultIfActive } from "../fault-injection";
import { runWithLogContext, updateLogContext } from "../observability/context";
import { AppLogger } from "../observability/logger";
import { SqsQueueManager } from "./sqs-client";

export const CONSUMER_NAME = "wager-transactions-consumer";

const sqsEnvelopeSchema = z.object({
  messageId: z.string().min(1, "messageId is required"),
  type: z.literal("WagerTransactionRequested"),
  occurredAt: z.string().min(1),
  data: z.record(z.unknown()),
});

@Injectable()
export class WagerTransactionsConsumer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private stopping = false;
  private pollLoopPromise?: Promise<void>;
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    @Inject(ENV_TOKEN) private readonly env: AppEnv,
    @Inject(SqsQueueManager) private readonly sqs: SqsQueueManager,
    @Inject(ProcessWagerTransactionUseCase)
    private readonly processWager: ProcessWagerTransactionUseCase,
    @Inject(TOKENS.MetricsPort) private readonly metrics: MetricsPort,
    @Inject(AppLogger) private readonly logger: AppLogger,
  ) {}

  onApplicationBootstrap(): void {
    if (!this.env.RUN_CONSUMER) return;
    this.stopping = false;
    this.pollLoopPromise = this.runPollLoop();
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopping = true;
    const gracePromise = Promise.allSettled(Array.from(this.inFlight));
    const timeoutPromise = Bun.sleep(this.env.SHUTDOWN_GRACE_MS);
    await Promise.race([gracePromise, timeoutPromise]);
    if (this.pollLoopPromise) {
      await Promise.race([this.pollLoopPromise, Bun.sleep(1000)]);
    }
  }

  async pollOnce(waitTimeSec = 0): Promise<number> {
    const messages = await this.sqs.receiveWagerMessages(10, waitTimeSec);
    if (messages.length === 0) return 0;

    await this.processBatch(messages);
    return messages.length;
  }

  private async runPollLoop(): Promise<void> {
    while (!this.stopping) {
      try {
        const messages = await this.sqs.receiveWagerMessages(
          10,
          this.env.SQS_WAIT_TIME_SEC,
        );
        if (messages.length > 0) {
          await this.processBatch(messages);
        }
      } catch (err) {
        if (!this.stopping) {
          this.logger.error(
            { errorMessage: err instanceof Error ? err.message : String(err) },
            "SQS consumer poll error",
          );
          await Bun.sleep(this.env.SQS_CONSUMER_POLL_INTERVAL_MS);
        }
      }
    }
  }

  private async processBatch(messages: Message[]): Promise<void> {
    const groups = new Map<string, Message[]>();
    for (const msg of messages) {
      const groupId =
        msg.Attributes?.MessageGroupId ?? msg.MessageId ?? "default";
      const list = groups.get(groupId) ?? [];
      list.push(msg);
      groups.set(groupId, list);
    }

    const groupPromises: Promise<void>[] = [];
    for (const [, groupMsgs] of groups.entries()) {
      const p = (async () => {
        for (let i = 0; i < groupMsgs.length; i++) {
          const msg = groupMsgs[i]!;
          if (this.stopping) {
            // Return visibility immediately for messages not yet started
            if (msg.ReceiptHandle) {
              await this.sqs
                .changeWagerMessageVisibility(msg.ReceiptHandle, 0)
                .catch(() => {});
            }
            continue;
          }
          await this.handleSingleMessage(msg);
        }
      })();
      this.inFlight.add(p);
      p.finally(() => this.inFlight.delete(p));
      groupPromises.push(p);
    }

    await Promise.all(groupPromises);
  }

  async handleSingleMessage(msg: Message): Promise<void> {
    const receiptHandle = msg.ReceiptHandle;
    const rawBody = msg.Body ?? "";
    const sqsMessageId = msg.MessageId ?? "unknown-sqs-id";
    const groupId = msg.Attributes?.MessageGroupId ?? "unknown-group";
    const receiveCount = parseInt(
      msg.Attributes?.ApproximateReceiveCount ?? "1",
      10,
    );

    await runWithLogContext({ messageId: sqsMessageId }, async () => {
      try {
        let parsedJson: unknown;
        try {
          parsedJson = JSON.parse(rawBody);
        } catch {
          throw new PermanentMessageError(
            "Message body is not valid JSON",
            "MALFORMED_JSON",
          );
        }

        const envParsed = sqsEnvelopeSchema.safeParse(parsedJson);
        if (!envParsed.success) {
          throw new PermanentMessageError(
            `Invalid SQS message envelope: ${envParsed.error.issues.map((i) => i.message).join("; ")}`,
            "INVALID_ENVELOPE",
          );
        }

        const envelope = envParsed.data;
        updateLogContext({
          messageId: envelope.messageId,
          correlationId: envelope.messageId,
        });

        triggerFaultIfActive("transient_sqs_consume");

        const rawData = envelope.data;
        if (rawData.kind === WagerTransactionKind.Opening) {
          throw new PermanentMessageError(
            "OPENING kind is forbidden on external channels",
            "OPENING_KIND_FORBIDDEN",
          );
        }

        const dto = parseWithValidation(submitWagerTransactionSchema, rawData);
        const idempotencyKey =
          typeof rawData.idempotencyKey === "string" &&
          rawData.idempotencyKey.trim() !== ""
            ? rawData.idempotencyKey.trim()
            : `${dto.providerId}:${dto.externalTransactionId}`;

        updateLogContext({
          walletId: dto.walletId,
          providerId: dto.providerId,
        });

        const result = await this.processWager.execute(dto, {
          source: "SQS",
          idempotencyKey,
          correlationId: envelope.messageId,
          inbox: {
            consumerName: CONSUMER_NAME,
            messageId: envelope.messageId,
          },
        });

        updateLogContext({ transactionId: result.transactionId });

        // Fault injection hook: crash AFTER SQL commit and BEFORE SQS DeleteMessage (ack)
        triggerFaultIfActive("crash_after_commit");

        if (receiptHandle) {
          await this.sqs.deleteWagerMessage(receiptHandle);
        }

        this.logger.info(
          {
            transactionId: result.transactionId,
            walletId: dto.walletId,
            providerId: dto.providerId,
            status: result.status,
            idempotentReplay: result.idempotentReplay,
            inboxDuplicate: result.inboxDuplicate ?? false,
          },
          "SQS wager message processed and acknowledged",
        );
      } catch (err) {
        if (this.isPermanentError(err)) {
          const reason =
            err instanceof DomainError ? err.code : "PERMANENT_ERROR";
          this.logger.warn(
            {
              reason,
              errorMessage: err instanceof Error ? err.message : String(err),
            },
            "Routing permanent failed SQS message to DLQ",
          );
          this.metrics.incrementDlqMessage(reason);
          if (receiptHandle) {
            const dedupId = `${sqsMessageId}-dlq`;
            await this.sqs.sendToDlq(rawBody || "{}", groupId, dedupId, reason);
            await this.sqs.deleteWagerMessage(receiptHandle);
          }
          return;
        }

        // Transient error handling with exponential backoff and maxReceiveCount check
        this.metrics.incrementRetry("sqs_transient");
        const errorMessage = err instanceof Error ? err.message : String(err);

        if (receiveCount >= this.env.SQS_MAX_RECEIVE_COUNT) {
          const reason = "MAX_RECEIVE_COUNT_EXCEEDED";
          this.logger.error(
            { receiveCount, errorMessage },
            "SQS message exceeded maxReceiveCount; routing to DLQ",
          );
          this.metrics.incrementDlqMessage(reason);
          if (receiptHandle) {
            const dedupId = `${sqsMessageId}-max-retries`;
            await this.sqs.sendToDlq(rawBody || "{}", groupId, dedupId, reason);
            await this.sqs.deleteWagerMessage(receiptHandle);
          }
          return;
        }

        const backoffSec = Math.min(
          Math.pow(2, receiveCount) * this.env.SQS_RETRY_BASE_SEC,
          300,
        );
        this.logger.warn(
          { receiveCount, backoffSec, errorMessage },
          "Transient SQS processing error; scheduling visibility retry",
        );
        if (receiptHandle) {
          await this.sqs
            .changeWagerMessageVisibility(receiptHandle, backoffSec)
            .catch(() => {});
        }
      }
    });
  }

  private isPermanentError(err: unknown): boolean {
    if (
      err instanceof PermanentMessageError ||
      err instanceof InvalidMoneyError ||
      err instanceof InvalidReferenceError ||
      err instanceof IdempotencyConflictError ||
      err instanceof WalletNotFoundError
    ) {
      return true;
    }
    if (err instanceof DomainError) {
      return (
        err.code === "VALIDATION_ERROR" ||
        err.code === "OPENING_KIND_FORBIDDEN" ||
        err.code === "IDEMPOTENCY_KEY_MISSING"
      );
    }
    return false;
  }
}
