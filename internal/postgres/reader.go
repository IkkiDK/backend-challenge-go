package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

// reader answers queries straight from the pool, outside any unit of work.
type reader struct {
	pool *pgxpool.Pool
}

func (r reader) Wallet(ctx context.Context, id domain.WalletID) (*domain.Wallet, error) {
	return getWallet(ctx, r.pool, id)
}

func (r reader) Transactions() app.TransactionReader { return transactions{q: r.pool} }

func (r reader) Inbox() app.InboxReader { return inbox{q: r.pool} }

func (r reader) LedgerPage(ctx context.Context, walletID domain.WalletID, afterVersion int64, limit int) ([]domain.LedgerEntry, error) {
	return ledgerPage(ctx, r.pool, walletID, afterVersion, limit)
}

func (r reader) LedgerTotals(ctx context.Context, walletID domain.WalletID) (app.LedgerTotals, error) {
	return ledgerTotals(ctx, r.pool, walletID)
}
