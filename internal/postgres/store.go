// Package postgres implements the application's persistence ports with pgx and
// explicit SQL. Transactions, row locks and constraints stay visible here;
// the invariants themselves are enforced by the schema in /migrations.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

// maxTxAttempts bounds how often a unit of work lost to a deadlock, a
// serialization failure or a stale version is retried.
const maxTxAttempts = 3

// querier is what repositories need; both pgx.Tx and *pgxpool.Pool provide
// it, so the same SQL serves units of work and plain reads.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Config tunes the connection pool and the session limits of every connection.
type Config struct {
	URL      string
	MaxConns int32
	// LockTimeout turns a row lock held for too long into a transient error
	// instead of an unbounded wait.
	LockTimeout time.Duration
	// StatementTimeout bounds any single statement.
	StatementTimeout time.Duration
	// IdleInTransactionTimeout makes the server end sessions that keep a
	// transaction open without activity, releasing their locks.
	IdleInTransactionTimeout time.Duration
}

// NewPool creates a connection pool; it does not connect until first use.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	poolConfig.MaxConns = cfg.MaxConns
	params := poolConfig.ConnConfig.RuntimeParams
	params["application_name"] = "wallet-service"
	params["lock_timeout"] = milliseconds(cfg.LockTimeout)
	params["statement_timeout"] = milliseconds(cfg.StatementTimeout)
	params["idle_in_transaction_session_timeout"] = milliseconds(cfg.IdleInTransactionTimeout)
	return pgxpool.NewWithConfig(ctx, poolConfig)
}

func milliseconds(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// Store implements app.Store on a connection pool.
type Store struct {
	pool    *pgxpool.Pool
	metrics *observability.Metrics
}

// NewStore returns a store over pool.
func NewStore(pool *pgxpool.Pool, metrics *observability.Metrics) *Store {
	return &Store{pool: pool, metrics: metrics}
}

// InTx implements app.Store.
func (s *Store) InTx(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	for attempt := 1; ; attempt++ {
		err := s.runTx(ctx, fn)
		reason, retry := retryReason(err)
		if !retry || attempt == maxTxAttempts {
			return err
		}
		s.metrics.ConcurrencyConflicts.WithLabelValues(reason).Inc()
		s.metrics.Retries.WithLabelValues("database_transaction").Inc()
	}
}

func (s *Store) runTx(ctx context.Context, fn func(ctx context.Context, tx app.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return dbError(err)
	}
	// Rolling back after a successful commit is a no-op.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx, unitOfWork{q: tx}); err != nil {
		return err
	}
	// Deferred constraint triggers run here, so a ledger mismatch fails the commit.
	return dbError(tx.Commit(ctx))
}

// Reader implements app.Store.
func (s *Store) Reader() app.Reader {
	return reader{pool: s.pool}
}

// Ping checks that the database answers.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// unitOfWork binds every repository to one SQL transaction.
type unitOfWork struct {
	q querier
}

func (u unitOfWork) Wallets() app.WalletRepository           { return wallets{q: u.q} }
func (u unitOfWork) Transactions() app.TransactionRepository { return transactions{q: u.q} }
func (u unitOfWork) Ledger() app.LedgerWriter                { return ledger{q: u.q} }
func (u unitOfWork) Outbox() app.OutboxWriter                { return outboxWriter{q: u.q} }
func (u unitOfWork) Inbox() app.InboxRepository              { return inbox{q: u.q} }

// retryReason reports whether err lost a concurrency race that a fresh
// attempt of the whole unit of work resolves.
func retryReason(err error) (string, bool) {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, app.ErrStaleVersion):
		return "stale_version", true
	case !errors.As(err, &pgErr):
		return "", false
	case pgErr.Code == pgerrcode.DeadlockDetected:
		return "deadlock", true
	case pgErr.Code == pgerrcode.SerializationFailure:
		return "serialization_failure", true
	}
	return "", false
}

// uniqueConstraintErrors maps the unique constraints the service expects to
// race on to the application errors that resolve them.
var uniqueConstraintErrors = map[string]error{
	"wallets_player_currency_key":           app.ErrWalletAlreadyExists,
	"wagering_transactions_idempotency_key": app.ErrDuplicate,
	"wagering_transactions_external_id_key": app.ErrDuplicate,
	"inbox_messages_pkey":                   app.ErrDuplicate,
}

// invariantClasses are the SQLSTATE classes of data that can never be
// written: integrity violations (23), data exceptions (22) and the schema's
// own invariant triggers (WL). Retrying them cannot help.
var invariantClasses = map[string]bool{"23": true, "22": true, "WL": true}

// dbError classifies a database error for the application: expected unique
// races, permanent invariant violations, or transient unavailability. Errors
// such as permission or syntax errors are deployment faults, not properties
// of an operation, so they are reported as transient rather than recording
// operations as FAILED.
func dbError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if mapped, ok := uniqueConstraintErrors[pgErr.ConstraintName]; ok && pgErr.Code == pgerrcode.UniqueViolation {
			return fmt.Errorf("%w: %w", mapped, err)
		}
		if invariantClasses[pgErr.Code[:2]] {
			return fmt.Errorf("%w: %w", app.ErrIntegrity, err)
		}
		return fmt.Errorf("%w: %w", app.ErrUnavailable, err)
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	return fmt.Errorf("%w: %w", app.ErrUnavailable, err)
}
