DROP TRIGGER wagering_transactions_require_ledger_entry ON wagering_transactions;
DROP FUNCTION wagering_transactions_require_ledger_entry();
DROP TRIGGER wallets_require_ledger_entry ON wallets;
DROP FUNCTION wallets_require_ledger_entry();
DROP TABLE wallet_ledger_entries;
DROP FUNCTION wallet_ledger_entries_check_insert();
