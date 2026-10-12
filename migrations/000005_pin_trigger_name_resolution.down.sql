DO $$
BEGIN
    EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO PUBLIC', current_database());
END
$$;

ALTER FUNCTION forbid_change() RESET search_path;
ALTER FUNCTION wallets_guard_insert() RESET search_path;
ALTER FUNCTION wallets_guard_update() RESET search_path;
ALTER FUNCTION wagering_transactions_guard_update() RESET search_path;
ALTER FUNCTION wallet_ledger_entries_check_insert() RESET search_path;
ALTER FUNCTION wallets_require_ledger_entry() RESET search_path;
ALTER FUNCTION wagering_transactions_require_ledger_entry() RESET search_path;
ALTER FUNCTION outbox_events_guard() RESET search_path;
