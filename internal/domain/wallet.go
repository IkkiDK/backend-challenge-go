package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// InitialWalletVersion is the version of a newly opened wallet. The version
// grows by exactly one with every balance change and never otherwise.
const InitialWalletVersion int64 = 1

// Wallet is the financial aggregate root: the only place a balance changes.
// Every change produces the ledger entry that must be committed with it.
type Wallet struct {
	id        WalletID
	playerID  PlayerID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
	events    []Event
}

// OpenWalletParams describes a wallet to open.
type OpenWalletParams struct {
	ID             WalletID
	PlayerID       PlayerID
	InitialBalance money.Money
	// OpeningTransactionID identifies the internal OPENING credit; it is only
	// used when InitialBalance is positive.
	OpeningTransactionID TransactionID
	CorrelationID        string
	Now                  time.Time
}

// WalletOpening is everything that must be committed together to open a wallet.
type WalletOpening struct {
	Wallet *Wallet
	// Opening is the PROCESSED internal OPENING transaction, or nil when the
	// initial balance is zero.
	Opening *WagerTransaction
	// Entry is the credit entry of the opening, or nil when the initial balance
	// is zero.
	Entry *LedgerEntry
}

// OpenWallet creates a wallet at version 1. A positive initial balance is
// credited through a PROCESSED internal OPENING transaction and its ledger
// entry, emitting WagerTransactionProcessed and WalletBalanceChanged; a zero
// initial balance creates neither.
func OpenWallet(p OpenWalletParams) (WalletOpening, error) {
	if err := requireSet(
		requiredValue{"walletId", p.ID},
		requiredValue{"playerId", p.PlayerID},
		requiredValue{"opening time", p.Now},
	); err != nil {
		return WalletOpening{}, err
	}
	if !p.InitialBalance.IsValid() || p.InitialBalance.IsNegative() {
		return WalletOpening{}, fmt.Errorf("%w: initial balance must be a non-negative amount", ErrInvalidValue)
	}

	zero, err := money.Zero(p.InitialBalance.Currency())
	if err != nil {
		return WalletOpening{}, fmt.Errorf("%w: %w", ErrInvalidValue, err)
	}
	w := &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		balance:   zero,
		version:   InitialWalletVersion,
		createdAt: p.Now,
		updatedAt: p.Now,
	}
	if p.InitialBalance.IsZero() {
		return WalletOpening{Wallet: w}, nil
	}

	opening, err := newOpeningTransaction(p.OpeningTransactionID, w, p.InitialBalance, p.CorrelationID, p.Now)
	if err != nil {
		return WalletOpening{}, err
	}
	// The opening credit is the creation itself, so it keeps the initial version.
	entry, err := newLedgerEntry(LedgerEntryParams{
		WalletID:      w.id,
		TransactionID: opening.id,
		WalletVersion: InitialWalletVersion,
		Direction:     DirectionCredit,
		Amount:        p.InitialBalance,
		BalanceBefore: zero,
		BalanceAfter:  p.InitialBalance,
		CreatedAt:     p.Now,
	})
	if err != nil {
		return WalletOpening{}, err
	}
	w.balance = p.InitialBalance
	w.events = append(w.events, newWalletBalanceChanged(entry))
	if err := opening.markProcessed(w.balance, TransactionID{}, p.Now); err != nil {
		return WalletOpening{}, err
	}
	return WalletOpening{Wallet: w, Opening: opening, Entry: &entry}, nil
}

// WalletSnapshot holds the persisted state of a wallet.
type WalletSnapshot struct {
	ID        WalletID
	PlayerID  PlayerID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RehydrateWallet restores a stored wallet without replaying movements or
// emitting events.
func RehydrateWallet(s WalletSnapshot) (*Wallet, error) {
	if err := requireSet(
		requiredValue{"walletId", s.ID},
		requiredValue{"playerId", s.PlayerID},
		requiredValue{"createdAt", s.CreatedAt},
		requiredValue{"updatedAt", s.UpdatedAt},
	); err != nil {
		return nil, err
	}
	switch {
	case !s.Balance.IsValid() || s.Balance.IsNegative():
		return nil, fmt.Errorf("%w: wallet balance must be a non-negative amount", ErrInvalidValue)
	case s.Version < InitialWalletVersion:
		return nil, fmt.Errorf("%w: wallet version %d is below %d", ErrInvalidValue, s.Version, InitialWalletVersion)
	case s.UpdatedAt.Before(s.CreatedAt):
		return nil, fmt.Errorf("%w: wallet updatedAt precedes createdAt", ErrInvalidValue)
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt,
		updatedAt: s.UpdatedAt,
	}, nil
}

// Debit removes amount from the balance on behalf of transaction txID. It fails
// with ErrInsufficientFunds rather than let the balance go negative.
func (w *Wallet) Debit(txID TransactionID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(txID, DirectionDebit, amount, now)
}

// Credit adds amount to the balance on behalf of transaction txID.
func (w *Wallet) Credit(txID TransactionID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(txID, DirectionCredit, amount, now)
}

func (w *Wallet) move(txID TransactionID, direction Direction, amount money.Money, now time.Time) (LedgerEntry, error) {
	if err := requireSet(requiredValue{"transactionId", txID}, requiredValue{"movement time", now}); err != nil {
		return LedgerEntry{}, err
	}
	if !amount.IsValid() || !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: movement amount must be positive", ErrInvalidValue)
	}
	if amount.Currency() != w.Currency() {
		return LedgerEntry{}, fmt.Errorf("%w: wallet holds %s, movement is in %s", ErrCurrencyMismatch, w.Currency(), amount.Currency())
	}

	var (
		after money.Money
		err   error
	)
	if direction == DirectionDebit {
		after, err = w.balance.Sub(amount)
		if err == nil && after.IsNegative() {
			return LedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, w.balance, amount)
		}
	} else {
		after, err = w.balance.Add(amount)
		if errors.Is(err, money.ErrOverflow) {
			return LedgerEntry{}, fmt.Errorf("%w: %w", ErrBalanceLimitExceeded, err)
		}
	}
	if err != nil {
		return LedgerEntry{}, err
	}

	entry, err := newLedgerEntry(LedgerEntryParams{
		WalletID:      w.id,
		TransactionID: txID,
		WalletVersion: w.version + 1,
		Direction:     direction,
		Amount:        amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		CreatedAt:     now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version = entry.walletVersion
	// Each instance stamps movements with its own clock, and clocks drift
	// apart; an instance running behind must not date the wallet back before
	// an earlier movement or its creation.
	if now.After(w.updatedAt) {
		w.updatedAt = now
	}
	w.events = append(w.events, newWalletBalanceChanged(entry))
	return entry, nil
}

// PullEvents returns the events recorded since the last call and forgets them.
func (w *Wallet) PullEvents() []Event {
	events := w.events
	w.events = nil
	return events
}

// ID returns the wallet identifier.
func (w *Wallet) ID() WalletID { return w.id }

// PlayerID returns the owner of the wallet.
func (w *Wallet) PlayerID() PlayerID { return w.playerID }

// Currency returns the only currency the wallet accepts.
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }

// Balance returns the current balance; never negative.
func (w *Wallet) Balance() money.Money { return w.balance }

// Version returns the optimistic-concurrency version of the balance.
func (w *Wallet) Version() int64 { return w.version }

// CreatedAt returns when the wallet was opened.
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt returns when the balance last changed (or the opening time). It
// never moves back, even when the instances' clocks disagree.
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }
