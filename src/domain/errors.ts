export class DomainError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super("INVALID_MONEY", message);
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, actual: string) {
    super("CURRENCY_MISMATCH", `Currency mismatch: expected ${expected}, received ${actual}`);
  }
}

export class InsufficientFundsError extends DomainError {
  constructor(walletId: string) {
    super("INSUFFICIENT_FUNDS", `Insufficient funds on wallet ${walletId}`);
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(currentStatus: string, targetOperation: string) {
    super(
      "INVALID_TRANSACTION_STATE",
      `Cannot perform '${targetOperation}' when transaction status is '${currentStatus}'`,
    );
  }
}

export class InvalidReferenceError extends DomainError {
  constructor(message: string, code = "INVALID_REFERENCE") {
    super(code, message);
  }
}

export class InvalidLedgerEntryError extends DomainError {
  constructor(message: string) {
    super("INVALID_LEDGER_ENTRY", message);
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(key: string, reason = "Idempotency key reused with different payload") {
    super("IDEMPOTENCY_CONFLICT", `${reason} (key: ${key})`);
  }
}

export class WalletNotFoundError extends DomainError {
  constructor(walletId: string) {
    super("WALLET_NOT_FOUND", `Wallet ${walletId} not found`);
  }
}

export class WalletAlreadyExistsError extends DomainError {
  constructor(playerId: string, currency: string) {
    super("WALLET_ALREADY_EXISTS", `Wallet already exists for player ${playerId} and currency ${currency}`);
  }
}

export class TransactionNotFoundError extends DomainError {
  constructor(identifier: string) {
    super("TRANSACTION_NOT_FOUND", `Transaction ${identifier} not found`);
  }
}

export class OptimisticConcurrencyError extends DomainError {
  constructor(aggregateId: string) {
    super("OPTIMISTIC_CONCURRENCY_CONFLICT", `Concurrent modification detected for aggregate ${aggregateId}`);
  }
}

export class TransientInfrastructureError extends DomainError {
  constructor(message: string, public readonly causeError?: unknown) {
    super("SERVICE_UNAVAILABLE", message);
  }
}

export class PermanentMessageError extends DomainError {
  constructor(message: string, code = "PERMANENT_MESSAGE_ERROR") {
    super(code, message);
  }
}
