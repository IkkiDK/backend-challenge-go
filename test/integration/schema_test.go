//go:build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/IkkiDK/backend-challenge-go/migrations"
)

// sqlState returns the SQLSTATE of a PostgreSQL error, or "" for other errors.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func TestMigrationsApplyAndRevertOnAFreshDatabase(t *testing.T) {
	const database = "migrations_check"
	ctx := t.Context()
	if _, err := env.owner.Exec(ctx, "DROP DATABASE IF EXISTS "+database); err != nil {
		t.Fatal(err)
	}
	if _, err := env.owner.Exec(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = env.owner.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" WITH (FORCE)")
	})

	db, err := sql.Open("pgx", strings.Replace(env.ownerDSN, "/wallet?", "/"+database+"?", 1))
	if err != nil {
		t.Fatal(err)
	}
	m, err := migrations.New(db)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	tables := func() int {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public'
			AND tablename IN ('wallets', 'wagering_transactions', 'wallet_ledger_entries', 'outbox_events', 'inbox_messages')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	if version, dirty, err := m.Version(); err != nil || dirty || version != 6 || tables() != 5 {
		t.Fatalf("after up: version %d dirty %t err %v tables %d", version, dirty, err, tables())
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	if version, _, _ := m.Version(); version != 5 {
		t.Fatalf("after down 1: version %d, want 5", version)
	}
	if err := m.Down(); err != nil {
		t.Fatalf("down all: %v", err)
	}
	if _, _, err := m.Version(); !errors.Is(err, migrate.ErrNilVersion) || tables() != 0 {
		t.Fatalf("after down all: %v, %d tables left", err, tables())
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
}

// openedWallet opens a wallet with 100.00 and places a 30.00 bet, so it has
// an OPENING, a BET and two ledger entries to tamper with.
func openedWallet(t *testing.T) (walletJSON, inProcess) {
	t.Helper()
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	if _, err := p.service.Submit(t.Context(), mustRequest(t, newOperation(w, "BET", "schema-bet-"+w.ID, "30.00")), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	return w, p
}

func TestLedgerIsAppendOnlyForTheServiceAndTheOwner(t *testing.T) {
	w, _ := openedWallet(t)
	app := newPool(t, env.appDSN)
	ctx := t.Context()

	for _, statement := range []string{
		"UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = $1",
		"DELETE FROM wallet_ledger_entries WHERE wallet_id = $1",
	} {
		if _, err := app.Exec(ctx, statement, w.ID); sqlState(err) != "42501" {
			t.Errorf("service role %q: %v, want permission denied (42501)", statement, err)
		}
		if _, err := env.owner.Exec(ctx, statement, w.ID); sqlState(err) != "WL003" {
			t.Errorf("owner %q: %v, want the append-only trigger (WL003)", statement, err)
		}
	}
	if _, err := app.Exec(ctx, "TRUNCATE wallet_ledger_entries"); sqlState(err) != "42501" {
		t.Errorf("service role TRUNCATE: %v, want permission denied", err)
	}
	if _, err := env.owner.Exec(ctx, "TRUNCATE wallet_ledger_entries CASCADE"); sqlState(err) != "WL003" {
		t.Errorf("owner TRUNCATE: %v, want the append-only trigger", err)
	}
	if ledgerEntries(t, w.ID) != 2 {
		t.Fatal("the ledger must be intact")
	}
}

func TestDatabaseRejectsBalanceChangesWithoutTheirLedgerEntry(t *testing.T) {
	w, _ := openedWallet(t)
	ctx := t.Context()
	cases := map[string]struct {
		statement string
		state     string
	}{
		"version not incremented":   {"UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = $1", "WL007"},
		"no ledger entry at commit": {"UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 1 WHERE id = $1", "WL008"},
		"negative balance":          {"UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1", "23514"},
		"version without balance":   {"UPDATE wallets SET version = version + 1 WHERE id = $1", "WL007"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Deferred triggers fire at commit, so each change runs in its own transaction.
			err := pgx.BeginFunc(ctx, env.owner, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, c.statement, w.ID)
				return err
			})
			if sqlState(err) != c.state {
				t.Fatalf("error = %v, want SQLSTATE %s", err, c.state)
			}
		})
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}

func TestDatabaseRejectsLedgerEntriesThatBreakTheChain(t *testing.T) {
	w, _ := openedWallet(t)
	var betID string
	if err := env.owner.QueryRow(t.Context(), `SELECT id FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.ID).Scan(&betID); err != nil {
		t.Fatal(err)
	}
	type entry struct {
		transactionID         string
		version               int64
		direction             string
		amount, before, after int64
	}
	// skipTriggers leaves only the table's own constraints to judge the entry.
	skipTriggers := func(ctx context.Context, tx pgx.Tx, _ entry) error {
		_, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`)
		return err
	}
	// matchWallet moves the wallet row to the entry, so the trigger's wallet
	// check (WL004) passes and its later checks judge the entry.
	matchWallet := func(ctx context.Context, tx pgx.Tx, e entry) error {
		_, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor = $2, version = $3 WHERE id = $1`, w.ID, e.after, e.version)
		return err
	}
	// The wallet holds 70.00 at version 2: version 1 credited the 100.00
	// opening and version 2 debited the 30.00 BET. Each entry must be refused
	// by exactly the guard its case names.
	cases := []struct {
		name              string
		prepare           func(context.Context, pgx.Tx, entry) error
		entry             entry
		state, constraint string
	}{
		{"second entry of a transaction", skipTriggers, entry{betID, 3, "DEBIT", 3000, 7000, 4000},
			"23505", "wallet_ledger_entries_wallet_transaction_key"},
		{"second entry of a wallet version", skipTriggers, entry{uuid.NewString(), 2, "DEBIT", 100, 7000, 6900},
			"23505", "wallet_ledger_entries_wallet_version_key"},
		{"BET entry that credits the wallet", matchWallet, entry{betID, 3, "CREDIT", 3000, 7000, 10000},
			"WL006", ""},
		{"entry that does not continue the previous balance", matchWallet, entry{betID, 3, "DEBIT", 3000, 9000, 6000},
			"WL005", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			tx, err := env.owner.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Never committed: a replica session would skip the commit-time checks too.
			defer func() { _ = tx.Rollback(ctx) }()
			if err := c.prepare(ctx, tx, c.entry); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, `
				INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, currency,
					amount_minor, balance_before_minor, balance_after_minor, created_at)
				VALUES (gen_random_uuid(), $1, $2, $3, $4, 'BRL', $5, $6, $7, now())`,
				w.ID, c.entry.transactionID, c.entry.version, c.entry.direction, c.entry.amount, c.entry.before, c.entry.after)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != c.state || pgErr.ConstraintName != c.constraint {
				t.Fatalf("error = %v, want SQLSTATE %s from %q", err, c.state, c.constraint)
			}
		})
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}

func TestTerminalTransactionsAndOutboxSnapshotsAreImmutable(t *testing.T) {
	w, _ := openedWallet(t)
	ctx := t.Context()
	for name, c := range map[string]struct{ statement, state string }{
		"terminal transaction transition": {"UPDATE wagering_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE wallet_id = $1 AND kind = 'BET'", "WL001"},
		"transaction deletion":            {"DELETE FROM wagering_transactions WHERE wallet_id = $1", "WL003"},
		"outbox payload rewrite":          {"UPDATE outbox_events SET payload = '{}' WHERE ordering_key = $1", "WL002"},
		"unpublished event deletion":      {"DELETE FROM outbox_events WHERE ordering_key = $1 AND published_at IS NULL", "WL003"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := env.owner.Exec(ctx, c.statement, w.ID); sqlState(err) != c.state {
				t.Fatalf("error = %v, want SQLSTATE %s", err, c.state)
			}
		})
	}
}

func TestDatabaseForbidsASecondOpeningOrReversal(t *testing.T) {
	w, p := openedWallet(t)
	ctx := t.Context()
	_, err := env.owner.Exec(ctx, `
		INSERT INTO wagering_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
			result_balance_minor, correlation_id, created_at, updated_at)
		SELECT gen_random_uuid(), 'INTERNAL', 'OPENING', 'PROCESSED', id, player_id, 'BRL', 100, 100, 'x', now(), now()
		FROM wallets WHERE id = $1`, w.ID)
	if sqlState(err) != "23505" {
		t.Fatalf("second OPENING: %v, want unique violation (23505)", err)
	}

	refund := newOperation(w, "REFUND", "schema-refund-"+w.ID, "30.00")
	refund.Reference = "schema-bet-" + w.ID
	if _, err := p.service.Submit(ctx, mustRequest(t, refund), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	// Forge a second successful reversal of the same BET behind the service's back.
	_, err = env.owner.Exec(ctx, `
		INSERT INTO wagering_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
			provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			reference_external_transaction_id, reference_transaction_id, result_balance_minor, correlation_id, created_at, updated_at)
		SELECT gen_random_uuid(), 'EXTERNAL', 'ROLLBACK', 'PROCESSED', wallet_id, player_id, currency, amount_minor,
			provider_id, 'forged-rollback', 'forged-key', payload_hash, round_id, game_id,
			external_transaction_id, id, 0, 'x', now(), now()
		FROM wagering_transactions WHERE external_transaction_id = $1`, "schema-bet-"+w.ID)
	if sqlState(err) != "23505" {
		t.Fatalf("second reversal: %v, want unique violation (23505)", err)
	}
	requireLedgerMatchesBalance(t, w.ID, 10000)
}
