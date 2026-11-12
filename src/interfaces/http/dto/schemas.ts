import { z } from "zod";
import { DomainError } from "../../../domain/errors";
import { WagerTransactionKind } from "../../../domain/wager/wager-transaction";

const moneySchema = z.object({
  amount: z
    .string()
    .regex(/^\d+(\.\d{1,2})?$/, "amount must be a non-negative decimal string with up to 2 decimal places"),
  currency: z
    .string()
    .regex(/^[A-Z]{3}$/, "currency must be a 3-letter uppercase ISO-4217 code"),
});

export const createWalletSchema = z.object({
  playerId: z.string().uuid("playerId must be a valid UUID"),
  initialBalance: moneySchema,
});

export const submitWagerTransactionSchema = z
  .object({
    providerId: z.string().min(1, "providerId is required"),
    externalTransactionId: z.string().min(1, "externalTransactionId is required"),
    playerId: z.string().uuid("playerId must be a valid UUID"),
    walletId: z.string().uuid("walletId must be a valid UUID"),
    roundId: z.string().min(1, "roundId is required"),
    gameId: z.string().min(1, "gameId is required"),
    kind: z.enum([
      WagerTransactionKind.Bet,
      WagerTransactionKind.Win,
      WagerTransactionKind.Loss,
      WagerTransactionKind.Refund,
      WagerTransactionKind.Rollback,
    ]),
    money: moneySchema,
    referenceExternalTransactionId: z.string().min(1).optional(),
  })
  .superRefine((val, ctx) => {
    if (
      (val.kind === WagerTransactionKind.Refund ||
        val.kind === WagerTransactionKind.Rollback) &&
      !val.referenceExternalTransactionId
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["referenceExternalTransactionId"],
        message: `${val.kind} requires referenceExternalTransactionId`,
      });
    }
  });

export const uuidParamSchema = z.string().uuid("Parameter must be a valid UUID");

export function parseWithValidation<T>(
  schema: z.ZodType<T>,
  input: unknown,
): T {
  const parsed = schema.safeParse(input);
  if (!parsed.success) {
    const details = parsed.error.issues.map((i) => ({
      path: i.path.join("."),
      message: i.message,
    }));
    const err = new DomainError(
      "VALIDATION_ERROR",
      details.map((d) => `${d.path || "body"}: ${d.message}`).join("; "),
    ) as DomainError & { details?: unknown };
    err.details = details;
    throw err;
  }
  return parsed.data;
}
