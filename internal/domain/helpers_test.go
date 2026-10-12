package domain

import (
	"testing"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

const (
	testPlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	testWalletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
	testProvider = "provider-a"
	testRound    = "round-987"
	testGame     = "fortune-chimp"
)

var testNow = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

var testPolicy = ReferenceRetryPolicy{BaseDelay: time.Second, MaxDelay: 8 * time.Second, MaxAttempts: 4}

func mustMoney(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("money.Parse(%q, %q): %v", amount, currency, err)
	}
	return m
}

// betInput returns a valid BET input; tests override fields as needed.
func betInput(externalID, amount string) TransactionRequestInput {
	return TransactionRequestInput{
		ProviderID:            testProvider,
		ExternalTransactionID: externalID,
		IdempotencyKey:        testProvider + ":" + externalID,
		PlayerID:              testPlayerID,
		WalletID:              testWalletID,
		RoundID:               testRound,
		GameID:                testGame,
		Kind:                  string(KindBet),
		Amount:                amount,
		Currency:              "BRL",
	}
}

func operationInput(kind Kind, externalID, amount, reference string) TransactionRequestInput {
	in := betInput(externalID, amount)
	in.Kind = string(kind)
	in.ReferenceExternalTransactionID = reference
	return in
}

func mustRequest(t *testing.T, in TransactionRequestInput) TransactionRequest {
	t.Helper()
	req, err := NewTransactionRequest(in)
	if err != nil {
		t.Fatalf("NewTransactionRequest: %v", err)
	}
	return req
}

func mustTransaction(t *testing.T, in TransactionRequestInput) *WagerTransaction {
	t.Helper()
	tx, err := NewExternalTransaction(NewTransactionID(), mustRequest(t, in), "corr-1", testNow)
	if err != nil {
		t.Fatalf("NewExternalTransaction: %v", err)
	}
	return tx
}

// mustWallet opens the test wallet with balance and drops the opening events.
func mustWallet(t *testing.T, balance string) *Wallet {
	t.Helper()
	walletID, err := ParseWalletID(testWalletID)
	if err != nil {
		t.Fatal(err)
	}
	playerID, err := ParsePlayerID(testPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	opening, err := OpenWallet(OpenWalletParams{
		ID:                   walletID,
		PlayerID:             playerID,
		InitialBalance:       mustMoney(t, balance, "BRL"),
		OpeningTransactionID: NewTransactionID(),
		CorrelationID:        "corr-open",
		Now:                  testNow,
	})
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	opening.Wallet.PullEvents()
	return opening.Wallet
}

// settle runs Settle and fails the test on invariant errors.
func settle(t *testing.T, w *Wallet, tx *WagerTransaction, ref ReferenceState) *LedgerEntry {
	t.Helper()
	entry, err := Settle(w, tx, ref, testPolicy, testNow)
	if err != nil {
		t.Fatalf("Settle(%s): %v", tx.Kind(), err)
	}
	return entry
}

// processed settles in on w and requires it to be PROCESSED.
func processed(t *testing.T, w *Wallet, in TransactionRequestInput, ref ReferenceState) *WagerTransaction {
	t.Helper()
	tx := mustTransaction(t, in)
	settle(t, w, tx, ref)
	if tx.Status() != StatusProcessed {
		t.Fatalf("%s %s: status %s (%s), want PROCESSED", tx.Kind(), in.ExternalTransactionID, tx.Status(), tx.FailureCode())
	}
	tx.PullEvents()
	w.PullEvents()
	return tx
}

func eventTypes(events []Event) []string {
	types := make([]string, len(events))
	for i, e := range events {
		types[i] = e.EventType()
	}
	return types
}
