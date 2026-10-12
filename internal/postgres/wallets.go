package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

type wallets struct {
	q querier
}

func (r wallets) Insert(ctx context.Context, w *domain.Wallet) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallets (`+walletColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID().String(), w.PlayerID().String(), w.Currency().Code(), w.Balance().MinorUnits(),
		w.Version(), w.CreatedAt(), w.UpdatedAt())
	return dbError(err)
}

// Lock takes the wallet row lock with FOR NO KEY UPDATE: writers of the same
// wallet exclude each other, while foreign-key checks of rows that reference
// the wallet (which take KEY SHARE) are not blocked. Other wallets are never
// affected, so independent wallets progress in parallel.
func (r wallets) Lock(ctx context.Context, id domain.WalletID) (*domain.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `
		SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR NO KEY UPDATE`, id.String()))
}

// UpdateBalance is version-checked on top of the row lock, so a writer that
// skipped the lock could still never overwrite a committed update.
func (r wallets) UpdateBalance(ctx context.Context, w *domain.Wallet, previousVersion int64) error {
	tag, err := r.q.Exec(ctx, `
		UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		WHERE id = $1 AND version = $5`,
		w.ID().String(), w.Balance().MinorUnits(), w.Version(), w.UpdatedAt(), previousVersion)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: wallet %s is no longer at version %d", app.ErrStaleVersion, w.ID(), previousVersion)
	}
	return nil
}

func getWallet(ctx context.Context, q querier, id domain.WalletID) (*domain.Wallet, error) {
	return scanWallet(q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id.String()))
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		id, playerID, currencyCode string
		balanceMinor, version      int64
		createdAt, updatedAt       time.Time
	)
	err := row.Scan(&id, &playerID, &currencyCode, &balanceMinor, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrWalletNotFound
	}
	if err != nil {
		return nil, dbError(err)
	}
	walletID, err := domain.ParseWalletID(id)
	if err != nil {
		return nil, fmt.Errorf("stored wallet id: %w", err)
	}
	player, err := domain.ParsePlayerID(playerID)
	if err != nil {
		return nil, fmt.Errorf("stored player id: %w", err)
	}
	balance, err := storedMoney(balanceMinor, currencyCode)
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(domain.WalletSnapshot{
		ID: walletID, PlayerID: player, Balance: balance, Version: version,
		CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
	})
}

// storedMoney rebuilds an exact amount from its BIGINT minor units and code.
func storedMoney(minorUnits int64, currencyCode string) (money.Money, error) {
	currency, err := money.ParseCurrency(currencyCode)
	if err != nil {
		return money.Money{}, fmt.Errorf("stored currency: %w", err)
	}
	return money.New(minorUnits, currency)
}
