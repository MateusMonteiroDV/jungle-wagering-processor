import type { NextFunction, Request, Response } from "express";
import { uuidv7 } from "uuidv7";
import { runWithLogContext } from "../../infrastructure/observability/context";

export function correlationMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const headerVal = req.headers["x-correlation-id"];
  const correlationId =
    typeof headerVal === "string" && headerVal.trim() !== ""
      ? headerVal.trim()
      : uuidv7();

  res.setHeader("X-Correlation-Id", correlationId);
  (req as Request & { correlationId?: string }).correlationId = correlationId;

  runWithLogContext({ correlationId }, () => {
    next();
  });
}
