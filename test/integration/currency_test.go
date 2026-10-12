//go:build integration

package integration

import (
	"testing"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// A currency mismatch is rejected with the wallet's balance, which is in the
// wallet's currency; the replay and later reads must report that same balance.
func TestCurrencyMismatchReportsTheWalletBalanceInItsOwnCurrency(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	op := newOperation(w, "BET", "usd-bet-"+w.ID, "10.00")
	op.Money = moneyJSON{Amount: "10.00", Currency: "USD"}

	first, err := p.service.Submit(t.Context(), mustRequest(t, op), httpOrigin())
	if err != nil || first.Transaction.FailureCode() != domain.FailureCurrencyMismatch {
		t.Fatalf("USD bet on a BRL wallet: %v, %+v", err, first.Transaction)
	}
	replay, err := p.service.Submit(t.Context(), mustRequest(t, op), httpOrigin())
	if err != nil || !replay.Replayed {
		t.Fatalf("replay: %v, replayed %v", err, replay.Replayed)
	}
	stored, err := p.service.Transaction(t.Context(), first.Transaction.ID(), app.AllProviders())
	if err != nil {
		t.Fatal(err)
	}

	for source, balance := range map[string]money.Money{
		"rejection": first.Transaction.ResultBalance(),
		"replay":    replay.Transaction.ResultBalance(),
		"read":      stored.ResultBalance(),
	} {
		if balance.Amount() != "100.00" || balance.Currency().Code() != "BRL" {
			t.Errorf("%s reports %s, want the wallet's 100.00 BRL", source, balance)
		}
	}
	requireLedgerMatchesBalance(t, w.ID, 10000)
}
