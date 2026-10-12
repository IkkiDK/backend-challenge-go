-- The trigger functions read wallets, wagering_transactions and
-- wallet_ledger_entries by unqualified name, and PostgreSQL resolves names
-- through the caller's search_path, where the session's temporary schema comes
-- first. A role able to create temporary tables could therefore shadow those
-- tables and satisfy a check with rows it invented. Two independent layers
-- prevent that: every trigger function resolves names in public, with pg_temp
-- explicitly last, and no role but the owner may create temporary tables.

ALTER FUNCTION forbid_change() SET search_path = public, pg_temp;
ALTER FUNCTION wallets_guard_insert() SET search_path = public, pg_temp;
ALTER FUNCTION wallets_guard_update() SET search_path = public, pg_temp;
ALTER FUNCTION wagering_transactions_guard_update() SET search_path = public, pg_temp;
ALTER FUNCTION wallet_ledger_entries_check_insert() SET search_path = public, pg_temp;
ALTER FUNCTION wallets_require_ledger_entry() SET search_path = public, pg_temp;
ALTER FUNCTION wagering_transactions_require_ledger_entry() SET search_path = public, pg_temp;
ALTER FUNCTION outbox_events_guard() SET search_path = public, pg_temp;

DO $$
BEGIN
    EXECUTE format('REVOKE TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
END
$$;
