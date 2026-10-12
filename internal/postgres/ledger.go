package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

type ledger struct {
	q querier
}

// Append inserts an entry; the database checks it against the wallet row and
// the previous entry, and rejects any later update or delete.
func (r ledger) Append(ctx context.Context, e domain.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallet_ledger_entries
			(id, wallet_id, transaction_id, wallet_version, direction, currency,
			 amount_minor, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID().String(), e.TransactionID().String(), e.WalletVersion(), e.Direction(),
		e.Amount().Currency().Code(), e.Amount().MinorUnits(), e.BalanceBefore().MinorUnits(),
		e.BalanceAfter().MinorUnits(), e.CreatedAt())
	return dbError(err)
}

// ledgerPage returns entries in the stable order of the wallet versions they
// produced, which is also their commit order.
func ledgerPage(ctx context.Context, q querier, walletID domain.WalletID, afterVersion int64, limit int) ([]domain.LedgerEntry, error) {
	rows, err := q.Query(ctx, `
		SELECT id, transaction_id, wallet_version, direction, currency,
			amount_minor, balance_before_minor, balance_after_minor, created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND wallet_version > $2
		ORDER BY wallet_version
		LIMIT $3`, walletID.String(), afterVersion, limit)
	if err != nil {
		return nil, dbError(err)
	}
	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.LedgerEntry, error) {
		var (
			id                                     uuid.UUID
			transactionID, direction, currencyCode string
			walletVersion, amount, before, after   int64
			createdAt                              time.Time
		)
		if err := row.Scan(&id, &transactionID, &walletVersion, &direction, &currencyCode, &amount, &before, &after, &createdAt); err != nil {
			return domain.LedgerEntry{}, err
		}
		parsing := storedValueParser{}
		p := domain.LedgerEntryParams{ID: id, WalletID: walletID, WalletVersion: walletVersion, CreatedAt: createdAt.UTC()}
		p.TransactionID = parse(&parsing, domain.ParseTransactionID, transactionID)
		p.Direction = parse(&parsing, domain.ParseDirection, direction)
		p.Amount, err = storedMoney(amount, currencyCode)
		parsing.keep(err)
		p.BalanceBefore, err = storedMoney(before, currencyCode)
		parsing.keep(err)
		p.BalanceAfter, err = storedMoney(after, currencyCode)
		parsing.keep(err)
		if parsing.err != nil {
			return domain.LedgerEntry{}, fmt.Errorf("stored ledger entry %s: %w", id, parsing.err)
		}
		return domain.RehydrateLedgerEntry(p)
	})
	if err != nil {
		return nil, dbError(err)
	}
	return entries, nil
}

// ledgerTotals reads the stored balance and the ledger sums inside one
// read-only REPEATABLE READ transaction, so both come from the same snapshot
// even while the wallet keeps moving.
func ledgerTotals(ctx context.Context, pool *pgxpool.Pool, walletID domain.WalletID) (app.LedgerTotals, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return app.LedgerTotals{}, dbError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	wallet, err := getWallet(ctx, tx, walletID)
	if err != nil {
		return app.LedgerTotals{}, err
	}
	var credits, debits, entries int64
	// SUM over bigint yields numeric; the cast fails loudly instead of overflowing.
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::bigint,
			COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::bigint,
			COUNT(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID.String()).Scan(&credits, &debits, &entries)
	if err != nil {
		return app.LedgerTotals{}, dbError(err)
	}
	currency := wallet.Currency().Code()
	totals := app.LedgerTotals{StoredBalance: wallet.Balance(), Entries: entries}
	if totals.Credits, err = storedMoney(credits, currency); err != nil {
		return app.LedgerTotals{}, err
	}
	if totals.Debits, err = storedMoney(debits, currency); err != nil {
		return app.LedgerTotals{}, err
	}
	return totals, nil
}
