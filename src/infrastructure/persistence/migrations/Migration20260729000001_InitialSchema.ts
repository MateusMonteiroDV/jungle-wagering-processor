import { Migration } from "@mikro-orm/migrations";

export class Migration20260729000001_InitialSchema extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY,
        player_id uuid NOT NULL,
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance numeric(20,2) NOT NULL CHECK (balance >= 0),
        version integer NOT NULL CHECK (version >= 1),
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        CONSTRAINT uq_wallet_player_currency UNIQUE (player_id, currency)
      );

      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY,
        provider_id text NOT NULL,
        external_transaction_id text NOT NULL,
        idempotency_key text NOT NULL,
        payload_hash char(64) NOT NULL,
        wallet_id uuid NOT NULL REFERENCES wallets(id),
        player_id uuid NOT NULL,
        round_id text NOT NULL,
        game_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(20,2) NOT NULL CHECK (amount >= 0),
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        reference_external_transaction_id text NULL,
        reference_transaction_id uuid NULL REFERENCES wager_transactions(id),
        status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text NULL,
        resulting_balance numeric(20,2) NULL,
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz NULL,
        correlation_id text NULL,
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        processed_at timestamptz NULL,
        CONSTRAINT uq_wtx_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT uq_wtx_provider_external UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT ck_wtx_reference_required CHECK (
          kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL
        ),
        CONSTRAINT ck_wtx_failure_code CHECK (
          (status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)
        ),
        CONSTRAINT ck_wtx_processed_at CHECK (
          (status = 'PROCESSED') = (processed_at IS NOT NULL)
        )
      );

      CREATE UNIQUE INDEX uq_wtx_single_reversal
        ON wager_transactions (reference_transaction_id)
        WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED';

      CREATE INDEX ix_wtx_pending_ref
        ON wager_transactions (next_attempt_at)
        WHERE status = 'PENDING_REFERENCE';

      CREATE INDEX ix_wtx_ref_lookup
        ON wager_transactions (provider_id, reference_external_transaction_id);

      CREATE TABLE wallet_ledger_entries (
        id uuid PRIMARY KEY,
        seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
        wallet_id uuid NOT NULL REFERENCES wallets(id),
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
        wallet_version integer NOT NULL CHECK (wallet_version >= 1),
        direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount numeric(20,2) NOT NULL CHECK (amount > 0),
        currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
        balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(20,2) NOT NULL CHECK (balance_after >= 0),
        created_at timestamptz NOT NULL,
        CONSTRAINT uq_ledger_tx_wallet UNIQUE (transaction_id, wallet_id),
        CONSTRAINT uq_ledger_wallet_version UNIQUE (wallet_id, wallet_version),
        CONSTRAINT ck_ledger_arithmetic CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount) OR
          (direction = 'DEBIT'  AND balance_after = balance_before - amount)
        )
      );

      CREATE INDEX ix_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq ASC);

      CREATE OR REPLACE FUNCTION forbid_ledger_mutation()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is append-only (% forbidden)', TG_OP
          USING ERRCODE = '23514';
      END;
      $$;

      CREATE TRIGGER trg_ledger_no_update
        BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();

      CREATE OR REPLACE FUNCTION enforce_wtx_immutability()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
          RAISE EXCEPTION 'Cannot transition terminal wager_transaction % from status % to %',
            OLD.id, OLD.status, NEW.status
            USING ERRCODE = '23514';
        END IF;

        IF NEW.amount <> OLD.amount
          OR NEW.currency <> OLD.currency
          OR NEW.kind <> OLD.kind
          OR NEW.wallet_id <> OLD.wallet_id
          OR NEW.player_id <> OLD.player_id
          OR NEW.idempotency_key <> OLD.idempotency_key
          OR NEW.payload_hash <> OLD.payload_hash
          OR NEW.provider_id <> OLD.provider_id
          OR NEW.external_transaction_id <> OLD.external_transaction_id
        THEN
          RAISE EXCEPTION 'Cannot mutate immutable fields of wager_transaction %', OLD.id
            USING ERRCODE = '23514';
        END IF;

        RETURN NEW;
      END;
      $$;

      CREATE TRIGGER trg_wtx_terminal
        BEFORE UPDATE ON wager_transactions
        FOR EACH ROW EXECUTE FUNCTION enforce_wtx_immutability();

      CREATE OR REPLACE FUNCTION enforce_wallet_ledger_consistency()
      RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'UPDATE' THEN
          IF NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1 THEN
            RAISE EXCEPTION 'Wallet % balance changed without incrementing version by 1', NEW.id
              USING ERRCODE = '23514';
          END IF;
          IF NEW.balance = OLD.balance AND NEW.version <> OLD.version THEN
            RAISE EXCEPTION 'Wallet % version changed without a balance change', NEW.id
              USING ERRCODE = '23514';
          END IF;
        END IF;

        IF NEW.balance <> 0 OR NEW.version > 1 THEN
          IF NOT EXISTS (
            SELECT 1 FROM wallet_ledger_entries
            WHERE wallet_id = NEW.id
              AND wallet_version = NEW.version
              AND balance_after = NEW.balance
          ) THEN
            RAISE EXCEPTION 'Wallet % at version % (balance %) has no matching wallet_ledger_entries row',
              NEW.id, NEW.version, NEW.balance
              USING ERRCODE = '23514';
          END IF;
        END IF;

        RETURN NEW;
      END;
      $$;

      CREATE CONSTRAINT TRIGGER trg_wallet_ledger_consistency
        AFTER INSERT OR UPDATE ON wallets
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_consistency();

      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL,
        message_id text NOT NULL,
        payload_hash char(64) NOT NULL,
        received_at timestamptz NOT NULL,
        processed_at timestamptz NULL,
        PRIMARY KEY (consumer_name, message_id)
      );

      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY,
        seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
        aggregate_id uuid NOT NULL,
        event_type text NOT NULL,
        payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL,
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz NOT NULL,
        locked_until timestamptz NULL,
        locked_by text NULL,
        last_error text NULL,
        published_at timestamptz NULL
      );

      CREATE INDEX ix_outbox_due
        ON outbox_messages (next_attempt_at, seq)
        WHERE published_at IS NULL;
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      DROP TABLE IF EXISTS outbox_messages CASCADE;
      DROP TABLE IF EXISTS inbox_messages CASCADE;
      DROP TRIGGER IF EXISTS trg_wallet_ledger_consistency ON wallets;
      DROP FUNCTION IF EXISTS enforce_wallet_ledger_consistency();
      DROP TRIGGER IF EXISTS trg_wtx_terminal ON wager_transactions;
      DROP FUNCTION IF EXISTS enforce_wtx_immutability();
      DROP TRIGGER IF EXISTS trg_ledger_no_update ON wallet_ledger_entries;
      DROP FUNCTION IF EXISTS forbid_ledger_mutation();
      DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;
      DROP TABLE IF EXISTS wager_transactions CASCADE;
      DROP TABLE IF EXISTS wallets CASCADE;
    `);
  }
}
