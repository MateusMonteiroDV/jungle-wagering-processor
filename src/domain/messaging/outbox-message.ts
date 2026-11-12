import { IntegrationEvent } from "../events/integration-event";

export interface OutboxMessageState {
  id: string;
  seq?: bigint;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  lockedUntil?: Date;
  lockedBy?: string;
  lastError?: string;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
    private _lockedUntil?: Date,
    private _lockedBy?: string,
    private _lastError?: string,
    public readonly seq?: bigint,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const serialized = event.toJSON() as unknown as Record<string, unknown>;
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze({ ...serialized }),
      event.occurredAt,
      0,
      event.occurredAt,
      undefined,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze({ ...state.payload }),
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
      state.lockedUntil,
      state.lockedBy,
      state.lastError,
      state.seq,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  get lockedUntil(): Date | undefined {
    return this._lockedUntil;
  }

  get lockedBy(): string | undefined {
    return this._lockedBy;
  }

  get lastError(): string | undefined {
    return this._lastError;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    if (!this.isPending()) return false;
    if (!this._nextAttemptAt) return true;
    return this._nextAttemptAt.getTime() <= now.getTime();
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
    this._lockedUntil = undefined;
    this._lockedBy = undefined;
  }

  scheduleRetry(now: Date, errorMessage?: string): void {
    this._attempts += 1;
    const baseDelaySec = Math.min(Math.pow(2, this._attempts), 300);
    this._nextAttemptAt = new Date(now.getTime() + baseDelaySec * 1000);
    this._lockedUntil = undefined;
    this._lockedBy = undefined;
    if (errorMessage !== undefined) {
      this._lastError = errorMessage;
    }
  }
}
