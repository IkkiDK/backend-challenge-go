//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// The ledger triggers read tables by name, and a temporary table shadows a
// table of the same name for its session: a role that could create one could
// satisfy those checks with rows it invented.
func TestTemporaryTablesCannotShadowWhatTheTriggersCheck(t *testing.T) {
	w, _ := openedWallet(t)
	ctx := t.Context()

	_, err := newPool(t, env.appDSN).Exec(ctx, `CREATE TEMP TABLE wallet_ledger_entries (wallet_id uuid)`)
	if sqlState(err) != "42501" {
		t.Fatalf("the service role created a temporary table: %v, want SQLSTATE 42501", err)
	}

	// The owner may still create temporary tables, so the triggers must not
	// resolve names through them. A connection of its own keeps the shadow
	// table away from every other test.
	owner, err := pgx.Connect(ctx, env.ownerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	tx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	statements := []string{
		`CREATE TEMP TABLE wallet_ledger_entries (wallet_id uuid, wallet_version bigint,
			balance_before_minor bigint, balance_after_minor bigint) ON COMMIT DROP`,
		`INSERT INTO pg_temp.wallet_ledger_entries
			SELECT id, version + 1, balance_minor, balance_minor + 100000000 FROM public.wallets WHERE id = $1`,
		`UPDATE public.wallets SET balance_minor = balance_minor + 100000000, version = version + 1 WHERE id = $1`,
	}
	for i, statement := range statements {
		var args []any
		if i > 0 {
			args = []any{w.ID}
		}
		if _, err := tx.Exec(ctx, statement, args...); err != nil {
			t.Fatalf("statement %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); sqlState(err) != "WL008" {
		t.Fatalf("commit = %v, want WL008: the trigger read the shadow ledger", err)
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}
