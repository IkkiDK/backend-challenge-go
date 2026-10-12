-- Least privilege for the wallet_app login role the service connects with (it
-- is created by the deployment, see deploy/postgres/initdb). Migrations run as
-- the table owner. The service can insert and read, may update only the
-- columns its state machine moves, and can never delete, truncate, alter the
-- schema or disable the triggers above.

GRANT USAGE ON SCHEMA public TO wallet_app;

GRANT SELECT, INSERT ON wallets, wagering_transactions, wallet_ledger_entries, outbox_events, inbox_messages
    TO wallet_app;

GRANT UPDATE (balance_minor, version, updated_at) ON wallets TO wallet_app;

GRANT UPDATE (status, reference_transaction_id, failure_code, result_balance_minor, attempts, next_attempt_at, updated_at)
    ON wagering_transactions TO wallet_app;

GRANT UPDATE (attempts, next_attempt_at, published_at, claimed_by, last_error) ON outbox_events TO wallet_app;
