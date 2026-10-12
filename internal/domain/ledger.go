package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// LedgerEntry is the immutable, append-only record of one balance movement.
// Corrections are new entries, never edits.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      WalletID
	transactionID TransactionID
	walletVersion int64
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// LedgerEntryParams holds every field of a ledger entry.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      WalletID
	TransactionID TransactionID
	// WalletVersion is the wallet version the movement produced.
	WalletVersion int64
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// RehydrateLedgerEntry restores a stored entry, enforcing the same invariants as
// creation.
func RehydrateLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	if err := p.validate(); err != nil {
		return LedgerEntry{}, err
	}
	return LedgerEntry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		walletVersion: p.WalletVersion,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		createdAt:     p.CreatedAt,
	}, nil
}

// newLedgerEntry creates the entry of a movement with a fresh identifier.
func newLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	p.ID = newUUID()
	return RehydrateLedgerEntry(p)
}

// validate checks balanceAfter = balanceBefore ± amount for the direction, with a
// positive amount and non-negative balances, all in one currency.
func (p LedgerEntryParams) validate() error {
	if err := requireSet(
		requiredValue{"ledger entry walletId", p.WalletID},
		requiredValue{"ledger entry transactionId", p.TransactionID},
		requiredValue{"ledger entry createdAt", p.CreatedAt},
	); err != nil {
		return err
	}
	if p.ID == uuid.Nil {
		return fmt.Errorf("%w: ledger entry id is required", ErrInvalidValue)
	}
	if p.WalletVersion < InitialWalletVersion {
		return fmt.Errorf("%w: ledger entry wallet version %d is below %d", ErrInvalidValue, p.WalletVersion, InitialWalletVersion)
	}
	if !p.Amount.IsValid() || !p.Amount.IsPositive() {
		return fmt.Errorf("%w: ledger entry amount must be positive", ErrInvalidValue)
	}
	if !p.BalanceBefore.IsValid() || !p.BalanceAfter.IsValid() || p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return fmt.Errorf("%w: ledger entry balances must be non-negative", ErrInvalidValue)
	}

	var (
		expected money.Money
		err      error
	)
	switch p.Direction {
	case DirectionCredit:
		expected, err = p.BalanceBefore.Add(p.Amount)
	case DirectionDebit:
		expected, err = p.BalanceBefore.Sub(p.Amount)
	default:
		return fmt.Errorf("%w: ledger entry direction %q", ErrInvalidValue, p.Direction)
	}
	if err != nil {
		return fmt.Errorf("%w: ledger entry arithmetic: %w", ErrInvalidValue, err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return fmt.Errorf("%w: ledger entry balance after %s must be %s %s %s",
			ErrInvalidValue, p.BalanceAfter, p.BalanceBefore, p.Direction, p.Amount)
	}
	return nil
}

// ID returns the entry identifier.
func (e LedgerEntry) ID() uuid.UUID { return e.id }

// WalletID returns the wallet the entry moved.
func (e LedgerEntry) WalletID() WalletID { return e.walletID }

// TransactionID returns the transaction that caused the movement.
func (e LedgerEntry) TransactionID() TransactionID { return e.transactionID }

// WalletVersion returns the wallet version the movement produced.
func (e LedgerEntry) WalletVersion() int64 { return e.walletVersion }

// Direction returns whether the entry debits or credits the wallet.
func (e LedgerEntry) Direction() Direction { return e.direction }

// Amount returns the moved amount; always positive.
func (e LedgerEntry) Amount() money.Money { return e.amount }

// BalanceBefore returns the wallet balance before the movement.
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter returns the wallet balance after the movement.
func (e LedgerEntry) BalanceAfter() money.Money { return e.balanceAfter }

// CreatedAt returns when the movement happened.
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }
