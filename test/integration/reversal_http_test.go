//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

// refusal is what the README's HTTP contract promises for an operation a
// business rule refuses: 422, REJECTED, the failure code with its class from
// the failure code table, and the wallet balance it left unchanged.
type refusal struct {
	code    domain.FailureCode
	class   domain.FailureClass
	balance string
}

// recordedEvent is an outbox row that belongs to one transaction.
type recordedEvent struct {
	Type, FailureCode, FailureClass string
}

func TestReversalRefusalsOverHTTP(t *testing.T) {
	inst := startInstance(t, "reversals", nil)
	token := accessToken(t, "provider-a")

	t.Run("second REFUND of a refunded BET", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		bet := newOperation(w, "BET", "reversed-bet-"+w.ID, "30.00")
		requireProcessed(t, inst.baseURL, token, bet, "70.00")
		requireProcessed(t, inst.baseURL, token, reversalOf(bet, "REFUND", "first-refund-"+w.ID), "100.00")

		requireRefused(t, inst.baseURL, token, reversalOf(bet, "REFUND", "second-refund-"+w.ID),
			refusal{domain.FailureReferenceAlreadyReversed, domain.FailureClassDefinitive, "100.00"})
		requireLedgerMatchesBalance(t, w.ID, 10000)
	})

	t.Run("ROLLBACK of a refunded BET", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		bet := newOperation(w, "BET", "refunded-bet-"+w.ID, "30.00")
		refund := reversalOf(bet, "REFUND", "refund-"+w.ID)
		requireProcessed(t, inst.baseURL, token, bet, "70.00")
		requireProcessed(t, inst.baseURL, token, refund, "100.00")

		requireRefused(t, inst.baseURL, token, reversalOf(bet, "ROLLBACK", "bet-rollback-"+w.ID),
			refusal{domain.FailureReferenceAlreadyReversed, domain.FailureClassDefinitive, "100.00"})
		// Only the BET is reversed: the REFUND itself can still be rolled back.
		requireProcessed(t, inst.baseURL, token, reversalOf(refund, "ROLLBACK", "refund-rollback-"+w.ID), "70.00")
		requireLedgerMatchesBalance(t, w.ID, 7000)
	})

	t.Run("ROLLBACK of a WIN whose funds were spent", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "10.00")
		win := newOperation(w, "WIN", "spent-win-"+w.ID, "50.00")
		requireProcessed(t, inst.baseURL, token, win, "60.00")
		requireProcessed(t, inst.baseURL, token, newOperation(w, "BET", "spending-bet-"+w.ID, "40.00"), "20.00")

		requireRefused(t, inst.baseURL, token, reversalOf(win, "ROLLBACK", "win-rollback-"+w.ID),
			refusal{domain.FailureInsufficientFundsForReversal, domain.FailureClassDefinitive, "20.00"})
		requireLedgerMatchesBalance(t, w.ID, 2000)
	})

	t.Run("REFUND of a rejected BET", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "10.00")
		bet := newOperation(w, "BET", "rejected-bet-"+w.ID, "50.00")
		requireRefused(t, inst.baseURL, token, bet,
			refusal{domain.FailureInsufficientFunds, domain.FailureClassDefinitive, "10.00"})

		requireRefused(t, inst.baseURL, token, reversalOf(bet, "REFUND", "rejected-bet-refund-"+w.ID),
			refusal{domain.FailureReferenceNotProcessed, domain.FailureClassDefinitive, "10.00"})
		requireLedgerMatchesBalance(t, w.ID, 1000)
	})
}

// reversalOf returns the operation of the given kind (REFUND or ROLLBACK)
// that reverses target in full.
func reversalOf(target operation, kind, externalID string) operation {
	op := target
	op.Kind, op.ExternalTransactionID, op.Reference = kind, externalID, target.ExternalTransactionID
	return op
}

func requireProcessed(t *testing.T, baseURL, token string, op operation, balance string) {
	t.Helper()
	status, got := submit(t, baseURL, token, op)
	if status != http.StatusCreated || got.Status != string(domain.StatusProcessed) || got.Balance == nil || *got.Balance != brl(balance) {
		t.Fatalf("%s %s: %s, want 201 PROCESSED with balance %s", op.Kind, op.ExternalTransactionID, describe(status, got), balance)
	}
}

// requireRefused submits op and checks the refusal end to end: the answer,
// a wallet and ledger left as they were, and a WagerTransactionRejected
// event as the only event of the transaction.
func requireRefused(t *testing.T, baseURL, token string, op operation, want refusal) {
	t.Helper()
	walletBefore, entriesBefore := walletState(t, op.WalletID), ledgerEntries(t, op.WalletID)
	status, got := submit(t, baseURL, token, op)
	if got.TransactionID == "" {
		t.Fatalf("%s %s: %s, want a recorded rejection", op.Kind, op.ExternalTransactionID, describe(status, got))
	}
	if status != http.StatusUnprocessableEntity || got.Status != string(domain.StatusRejected) ||
		got.FailureCode != string(want.code) || got.FailureClass != string(want.class) ||
		got.Balance == nil || *got.Balance != brl(want.balance) || got.IdempotentReplay {
		t.Errorf("%s %s: %s, want 422 REJECTED %s %s balance %s",
			op.Kind, op.ExternalTransactionID, describe(status, got), want.code, want.class, want.balance)
	}
	if after := walletState(t, op.WalletID); after != walletBefore {
		t.Errorf("the refusal moved the wallet from %+v to %+v", walletBefore, after)
	}
	if entries := ledgerEntries(t, op.WalletID); entries != entriesBefore {
		t.Errorf("the refusal changed the ledger from %d to %d entries", entriesBefore, entries)
	}

	rows, err := env.owner.Query(t.Context(), `
		SELECT event_type, COALESCE(payload->'data'->>'failureCode', ''), COALESCE(payload->'data'->>'failureClass', '')
		FROM outbox_events WHERE aggregate_id = $1 OR payload->'data'->>'transactionId' = $1`, got.TransactionID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[recordedEvent])
	if err != nil {
		t.Fatal(err)
	}
	wantEvents := []recordedEvent{{"WagerTransactionRejected", string(want.code), string(want.class)}}
	if !slices.Equal(events, wantEvents) {
		t.Errorf("events of the refused transaction = %+v, want only %+v", events, wantEvents)
	}
}

func describe(status int, r submitJSON) string {
	balance := "none"
	if r.Balance != nil {
		balance = r.Balance.Amount + " " + r.Balance.Currency
	}
	return fmt.Sprintf("%d %s %s %s, balance %s, replay %t", status, r.Status, r.FailureCode, r.FailureClass, balance, r.IdempotentReplay)
}
