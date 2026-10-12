-- The append-only wallet ledger, and the triggers that bind every balance
-- change to exactly one ledger entry of a processed transaction.

CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    wallet_id            uuid        NOT NULL REFERENCES wallets (id),
    transaction_id       uuid        NOT NULL REFERENCES wagering_transactions (id),
    -- The wallet version this movement produced; consecutive per wallet.
    wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
    direction            text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    currency             text        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    amount_minor         bigint      NOT NULL CHECK (amount_minor > 0),
    balance_before_minor bigint      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  bigint      NOT NULL CHECK (balance_after_minor >= 0),
    created_at           timestamptz NOT NULL,
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

-- A new entry must (1) describe the wallet row as it stands after the movement,
-- (2) continue the wallet's chain of entries, and (3) belong to a PROCESSED
-- transaction of that wallet, with its amount and the direction its kind implies.
-- The service therefore writes the transaction, then the wallet, then the entry.
CREATE FUNCTION wallet_ledger_entries_check_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    wallet   wallets%ROWTYPE;
    tx       wagering_transactions%ROWTYPE;
    previous wallet_ledger_entries%ROWTYPE;
    expected_direction text;
BEGIN
    SELECT * INTO wallet FROM wallets WHERE id = NEW.wallet_id;
    IF wallet.balance_minor <> NEW.balance_after_minor
        OR wallet.version <> NEW.wallet_version
        OR wallet.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger entry of wallet % (version %, balance after %) does not match the wallet (version %, balance %)',
            NEW.wallet_id, NEW.wallet_version, NEW.balance_after_minor, wallet.version, wallet.balance_minor
            USING ERRCODE = 'WL004';
    END IF;

    SELECT * INTO previous FROM wallet_ledger_entries
        WHERE wallet_id = NEW.wallet_id ORDER BY wallet_version DESC LIMIT 1;
    IF FOUND THEN
        IF NEW.wallet_version <> previous.wallet_version + 1
            OR NEW.balance_before_minor <> previous.balance_after_minor THEN
            RAISE EXCEPTION 'ledger entry of wallet % breaks the chain: version % after %, balance before % after %',
                NEW.wallet_id, NEW.wallet_version, previous.wallet_version, NEW.balance_before_minor, previous.balance_after_minor
                USING ERRCODE = 'WL005';
        END IF;
    -- The first entry is the opening credit (version 1) or, for a wallet opened
    -- with a zero balance, its first movement (version 2).
    ELSIF NEW.balance_before_minor <> 0 OR NEW.wallet_version NOT IN (1, 2) THEN
        RAISE EXCEPTION 'first ledger entry of wallet % must start from 0 at version 1 or 2', NEW.wallet_id
            USING ERRCODE = 'WL005';
    END IF;

    SELECT * INTO tx FROM wagering_transactions WHERE id = NEW.transaction_id;
    IF tx.status <> 'PROCESSED' OR tx.kind = 'LOSS' OR tx.wallet_id <> NEW.wallet_id
        OR tx.amount_minor <> NEW.amount_minor OR tx.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger entry does not match transaction % (% %)', tx.id, tx.kind, tx.status
            USING ERRCODE = 'WL006';
    END IF;
    expected_direction := CASE
        WHEN tx.kind = 'BET' THEN 'DEBIT'
        WHEN tx.kind = 'ROLLBACK' THEN (
            SELECT CASE WHEN target.kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END
            FROM wagering_transactions target WHERE target.id = tx.reference_transaction_id)
        ELSE 'CREDIT'
    END;
    IF NEW.direction IS DISTINCT FROM expected_direction THEN
        RAISE EXCEPTION 'ledger entry of % transaction % must be a %', tx.kind, tx.id, expected_direction
            USING ERRCODE = 'WL006';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER wallet_ledger_entries_check_insert BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_check_insert();
CREATE TRIGGER wallet_ledger_entries_append_only BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
CREATE TRIGGER wallet_ledger_entries_no_truncate BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_change();

-- At commit, every balance change must have the matching ledger entry.
CREATE FUNCTION wallets_require_ledger_entry() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    balance_before bigint := 0;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        balance_before := OLD.balance_minor;
    END IF;
    IF NEW.balance_minor <> balance_before AND NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
        WHERE wallet_id = NEW.id AND wallet_version = NEW.version
          AND balance_before_minor = balance_before AND balance_after_minor = NEW.balance_minor
    ) THEN
        RAISE EXCEPTION 'wallet % balance changed from % to % without its ledger entry', NEW.id, balance_before, NEW.balance_minor
            USING ERRCODE = 'WL008';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER wallets_require_ledger_entry AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger_entry();

-- At commit, every processed movement (all kinds but LOSS) must have its ledger entry.
CREATE FUNCTION wagering_transactions_require_ledger_entry() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'PROCESSED' AND NEW.kind <> 'LOSS' AND NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries WHERE wallet_id = NEW.wallet_id AND transaction_id = NEW.id
    ) THEN
        RAISE EXCEPTION 'processed % transaction % has no ledger entry', NEW.kind, NEW.id
            USING ERRCODE = 'WL009';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER wagering_transactions_require_ledger_entry AFTER INSERT OR UPDATE ON wagering_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wagering_transactions_require_ledger_entry();
