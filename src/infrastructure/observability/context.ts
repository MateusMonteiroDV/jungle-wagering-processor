import { AsyncLocalStorage } from "async_hooks";

export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const storage = new AsyncLocalStorage<LogContext>();

export function runWithLogContext<T>(ctx: LogContext, fn: () => T): T {
  const parent = storage.getStore() ?? {};
  return storage.run({ ...parent, ...ctx }, fn);
}

export function updateLogContext(partial: Partial<LogContext>): void {
  const store = storage.getStore();
  if (store) {
    Object.assign(store, partial);
  }
}

export function getLogContext(): LogContext {
  return storage.getStore() ?? {};
}
