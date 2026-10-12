package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

func validEntryParams(t *testing.T) LedgerEntryParams {
	t.Helper()
	w := mustWallet(t, "0.00")
	return LedgerEntryParams{
		ID:            uuid.Must(uuid.NewV7()),
		WalletID:      w.ID(),
		TransactionID: NewTransactionID(),
		WalletVersion: 2,
		Direction:     DirectionDebit,
		Amount:        mustMoney(t, "30.00", "BRL"),
		BalanceBefore: mustMoney(t, "100.00", "BRL"),
		BalanceAfter:  mustMoney(t, "70.00", "BRL"),
		CreatedAt:     testNow,
	}
}

func TestLedgerEntryValidatesBalanceArithmetic(t *testing.T) {
	if _, err := RehydrateLedgerEntry(validEntryParams(t)); err != nil {
		t.Fatalf("valid debit: %v", err)
	}
	credit := validEntryParams(t)
	credit.Direction = DirectionCredit
	credit.BalanceAfter = mustMoney(t, "130.00", "BRL")
	if _, err := RehydrateLedgerEntry(credit); err != nil {
		t.Fatalf("valid credit: %v", err)
	}

	invalid := map[string]func(*LedgerEntryParams){
		"debit with credit arithmetic": func(p *LedgerEntryParams) { p.BalanceAfter = mustMoney(t, "130.00", "BRL") },
		"credit with debit arithmetic": func(p *LedgerEntryParams) { p.Direction = DirectionCredit },
		"off by one cent":              func(p *LedgerEntryParams) { p.BalanceAfter = mustMoney(t, "70.01", "BRL") },
		"zero amount":                  func(p *LedgerEntryParams) { p.Amount = mustMoney(t, "0.00", "BRL") },
		"negative amount":              func(p *LedgerEntryParams) { p.Amount = mustMoney(t, "-30.00", "BRL") },
		"negative balance after": func(p *LedgerEntryParams) {
			p.BalanceBefore = mustMoney(t, "10.00", "BRL")
			p.BalanceAfter = mustMoney(t, "-20.00", "BRL")
		},
		"mixed currencies":  func(p *LedgerEntryParams) { p.Amount = mustMoney(t, "30.00", "USD") },
		"uninitialized":     func(p *LedgerEntryParams) { p.BalanceBefore = money.Money{} },
		"unknown direction": func(p *LedgerEntryParams) { p.Direction = "SIDEWAYS" },
		"version zero":      func(p *LedgerEntryParams) { p.WalletVersion = 0 },
		"missing id":        func(p *LedgerEntryParams) { p.ID = uuid.Nil },
		"missing wallet":    func(p *LedgerEntryParams) { p.WalletID = WalletID{} },
		"missing time":      func(p *LedgerEntryParams) { p.CreatedAt = time.Time{} },
	}
	for name, edit := range invalid {
		t.Run(name, func(t *testing.T) {
			p := validEntryParams(t)
			edit(&p)
			if _, err := RehydrateLedgerEntry(p); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
		})
	}
}
