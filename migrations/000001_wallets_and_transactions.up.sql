-- Wallets and the wagering transactions that move them.
--
-- Invariant violations raised by triggers use SQLSTATE class WL (WL001-WL009),
-- so the service can tell them apart from transient failures.

-- forbid_change rejects any DELETE, TRUNCATE (or, where attached, UPDATE).
CREATE FUNCTION forbid_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is not allowed', TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'WL003';
END
$$;

CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      text        NOT NULL CONSTRAINT wallets_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CONSTRAINT wallets_version_positive CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency),
    CONSTRAINT wallets_updated_after_created CHECK (updated_at >= created_at)
);

-- A wallet is born at version 1 and its version moves exactly with its balance.
CREATE FUNCTION wallets_guard_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.version <> 1 THEN
        RAISE EXCEPTION 'wallet % must be created at version 1, got %', NEW.id, NEW.version
            USING ERRCODE = 'WL007';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION wallets_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (NEW.id, NEW.player_id, NEW.currency, NEW.created_at)
        IS DISTINCT FROM (OLD.id, OLD.player_id, OLD.currency, OLD.created_at) THEN
        RAISE EXCEPTION 'wallet % identity is immutable', OLD.id
            USING ERRCODE = 'WL002';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet % balance changes must increment the version by one (% -> %)', OLD.id, OLD.version, NEW.version
            USING ERRCODE = 'WL007';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet % version only changes together with its balance', OLD.id
            USING ERRCODE = 'WL007';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER wallets_guard_insert BEFORE INSERT ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_insert();
CREATE TRIGGER wallets_guard_update BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();
CREATE TRIGGER wallets_no_delete BEFORE DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER wallets_no_truncate BEFORE TRUNCATE ON wallets
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();

CREATE TABLE wagering_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    -- PENDING exists only in memory: work is either completed in the same SQL
    -- transaction that records it, or persisted as PENDING_REFERENCE, which the
    -- reference worker always resumes.
    status                            text        NOT NULL CHECK (status IN ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         uuid        NOT NULL REFERENCES wallets (id),
    player_id                         uuid        NOT NULL,
    currency                          text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount_minor                      bigint      NOT NULL,
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text        CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wagering_transactions (id),
    failure_code                      text        CHECK (failure_code IN (
        'WALLET_PLAYER_MISMATCH', 'CURRENCY_MISMATCH', 'REFERENCE_KIND_NOT_ALLOWED', 'REFERENCE_MISMATCH',
        'REFERENCE_AMOUNT_MISMATCH', 'INSUFFICIENT_FUNDS', 'INSUFFICIENT_FUNDS_FOR_REVERSAL', 'BALANCE_LIMIT_EXCEEDED',
        'REFERENCE_NOT_FOUND', 'REFERENCE_NOT_PROCESSED', 'REFERENCE_ALREADY_REVERSED', 'PROCESSING_FAILED')),
    -- Balance returned to the provider, replayed verbatim for duplicates.
    result_balance_minor              bigint      CHECK (result_balance_minor >= 0),
    attempts                          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    correlation_id                    text        NOT NULL,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,

    -- OPENING is the only internal kind and the only one without provider metadata.
    CONSTRAINT wagering_transactions_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- LOSS moves nothing; every other kind moves a positive amount.
    CONSTRAINT wagering_transactions_amount_by_kind CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    -- REFUND and ROLLBACK need a reference, WIN may carry one, the others never do.
    CONSTRAINT wagering_transactions_reference_by_kind CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
        OR kind = 'WIN'
        OR (kind IN ('OPENING', 'BET', 'LOSS')
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
    ),
    -- Each status carries exactly the outcome fields that apply to it. Every
    -- comparison is NULL-safe: a CHECK that evaluates to NULL would pass.
    CONSTRAINT wagering_transactions_outcome_by_status CHECK (
        (status = 'PENDING_REFERENCE' AND next_attempt_at IS NOT NULL
            AND reference_external_transaction_id IS NOT NULL
            AND failure_code IS NULL AND result_balance_minor IS NULL)
        OR (status = 'PROCESSED' AND next_attempt_at IS NULL AND failure_code IS NULL
            AND result_balance_minor IS NOT NULL
            AND (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_transaction_id IS NOT NULL))
        OR (status = 'REJECTED' AND next_attempt_at IS NULL
            AND failure_code IS NOT NULL AND failure_code <> 'PROCESSING_FAILED'
            AND result_balance_minor IS NOT NULL)
        OR (status = 'FAILED' AND next_attempt_at IS NULL
            AND failure_code IS NOT DISTINCT FROM 'PROCESSING_FAILED')
    ),
    -- Idempotency keys are scoped to their provider; a provider operation is
    -- recorded once, whatever key it arrives with.
    CONSTRAINT wagering_transactions_idempotency_key UNIQUE (provider_id, idempotency_key),
    CONSTRAINT wagering_transactions_external_id_key UNIQUE (provider_id, external_transaction_id)
);

-- One internal opening credit per wallet, so the initial balance is never credited twice.
CREATE UNIQUE INDEX wagering_transactions_one_opening_per_wallet
    ON wagering_transactions (wallet_id) WHERE kind = 'OPENING';

-- A transaction can be reversed (REFUND or ROLLBACK) successfully at most once.
CREATE UNIQUE INDEX wagering_transactions_one_reversal_per_target
    ON wagering_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- Due work of the pending-reference worker.
CREATE INDEX wagering_transactions_pending_reference_due
    ON wagering_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';

-- Terminal transactions never change again, and business fields never change at all.
CREATE FUNCTION wagering_transactions_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wagering transaction % is % and cannot change', OLD.id, OLD.status
            USING ERRCODE = 'WL001';
    END IF;
    IF (NEW.id, NEW.origin, NEW.kind, NEW.wallet_id, NEW.player_id, NEW.currency, NEW.amount_minor,
        NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
        NEW.round_id, NEW.game_id, NEW.reference_external_transaction_id, NEW.correlation_id, NEW.created_at)
        IS DISTINCT FROM
       (OLD.id, OLD.origin, OLD.kind, OLD.wallet_id, OLD.player_id, OLD.currency, OLD.amount_minor,
        OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
        OLD.round_id, OLD.game_id, OLD.reference_external_transaction_id, OLD.correlation_id, OLD.created_at) THEN
        RAISE EXCEPTION 'wagering transaction % business fields are immutable', OLD.id
            USING ERRCODE = 'WL002';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER wagering_transactions_guard_update BEFORE UPDATE ON wagering_transactions
    FOR EACH ROW EXECUTE FUNCTION wagering_transactions_guard_update();
CREATE TRIGGER wagering_transactions_no_delete BEFORE DELETE ON wagering_transactions
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER wagering_transactions_no_truncate BEFORE TRUNCATE ON wagering_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();
