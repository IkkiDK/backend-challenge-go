package domain

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

func TestNewExternalTransactionStartsPending(t *testing.T) {
	tx := mustTransaction(t, betInput("tx-1", "25.00"))
	if tx.Status() != StatusPending || tx.Kind() != KindBet || tx.ProviderID().String() != testProvider {
		t.Fatalf("new transaction = %s %s %s, want a PENDING BET of provider-a", tx.Status(), tx.Kind(), tx.ProviderID())
	}
	if len(tx.PullEvents()) != 0 {
		t.Fatal("creating a transaction must not emit events")
	}
	if _, err := NewExternalTransaction(NewTransactionID(), TransactionRequest{}, "", testNow); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("a request not built by NewTransactionRequest error = %v, want ErrInvalidValue", err)
	}
}

func TestStateMachineAllowsOnlyDocumentedTransitions(t *testing.T) {
	balance := mustMoney(t, "10.00", "BRL")
	next := testNow.Add(time.Second)

	t.Run("pending reference then processed", func(t *testing.T) {
		tx := mustTransaction(t, operationInput(KindRefund, "tx-2", "1.00", "tx-1"))
		if err := tx.AwaitReference(next, testNow); err != nil {
			t.Fatal(err)
		}
		if err := tx.RetryReferenceLater(next.Add(time.Second), testNow); err != nil {
			t.Fatal(err)
		}
		if tx.Snapshot().Attempts != 1 {
			t.Fatalf("attempts = %d, want 1", tx.Snapshot().Attempts)
		}
		if err := tx.markProcessed(balance, NewTransactionID(), testNow); err != nil {
			t.Fatal(err)
		}
		if !tx.Snapshot().NextAttemptAt.IsZero() {
			t.Fatal("a terminal transaction must drop its retry schedule")
		}
		if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionPendingReference", "WagerTransactionProcessed"}) {
			t.Fatalf("events = %v", got)
		}
	})

	t.Run("only transactions with a reference can wait for one", func(t *testing.T) {
		tx := mustTransaction(t, betInput("tx-1", "1.00"))
		if err := tx.AwaitReference(next, testNow); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})

	t.Run("a pending transaction cannot be rescheduled", func(t *testing.T) {
		tx := mustTransaction(t, operationInput(KindRefund, "tx-2", "1.00", "tx-1"))
		if err := tx.RetryReferenceLater(next, testNow); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})

	terminal := map[Status]func(*WagerTransaction) error{
		StatusProcessed: func(tx *WagerTransaction) error { return tx.markProcessed(balance, TransactionID{}, testNow) },
		StatusRejected:  func(tx *WagerTransaction) error { return tx.reject(FailureInsufficientFunds, balance, testNow) },
		StatusFailed:    func(tx *WagerTransaction) error { return tx.Fail(testNow) },
	}
	for status, finish := range terminal {
		t.Run(string(status)+" is terminal", func(t *testing.T) {
			tx := mustTransaction(t, operationInput(KindRefund, "tx-2", "1.00", "tx-1"))
			if err := finish(tx); err != nil {
				t.Fatal(err)
			}
			if tx.Status() != status {
				t.Fatalf("status = %s, want %s", tx.Status(), status)
			}
			attempts := []func() error{
				func() error { return tx.AwaitReference(next, testNow) },
				func() error { return tx.RetryReferenceLater(next, testNow) },
				func() error { return tx.markProcessed(balance, TransactionID{}, testNow) },
				func() error { return tx.reject(FailureInsufficientFunds, balance, testNow) },
				func() error { return tx.Fail(testNow) },
			}
			for i, attempt := range attempts {
				if err := attempt(); !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("transition %d from %s: error = %v, want ErrInvalidTransition", i, status, err)
				}
			}
			if tx.Status() != status {
				t.Fatal("a refused transition must not change the status")
			}
		})
	}
}

func TestFailRecordsTheProcessingFailureCode(t *testing.T) {
	tx := mustTransaction(t, betInput("tx-1", "1.00"))
	if err := tx.Fail(testNow); err != nil {
		t.Fatal(err)
	}
	if tx.FailureCode() != FailureProcessingFailed || tx.FailureCode().Class() != FailureClassDefinitive {
		t.Fatalf("failure code = %s (%s)", tx.FailureCode(), tx.FailureCode().Class())
	}
}

func TestCheckReplay(t *testing.T) {
	original := betInput("tx-1", "25.00")
	recorded := mustTransaction(t, original)

	if err := recorded.CheckReplay(mustRequest(t, original)); err != nil {
		t.Fatalf("same key and payload: %v, want a replay", err)
	}

	differentPayload := original
	differentPayload.Amount = "26.00"
	if err := recorded.CheckReplay(mustRequest(t, differentPayload)); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("same key, different payload: %v, want ErrIdempotencyKeyReused", err)
	}

	sameKeyOtherOperation := betInput("tx-2", "25.00")
	sameKeyOtherOperation.IdempotencyKey = original.IdempotencyKey
	if err := recorded.CheckReplay(mustRequest(t, sameKeyOtherOperation)); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("same key, other external id: %v, want ErrIdempotencyKeyReused", err)
	}

	otherKey := original
	otherKey.IdempotencyKey = "retry-with-new-key"
	if err := recorded.CheckReplay(mustRequest(t, otherKey)); !errors.Is(err, ErrExternalTransactionConflict) {
		t.Fatalf("same operation under another key: %v, want ErrExternalTransactionConflict", err)
	}

	otherProvider := original
	otherProvider.ProviderID = "provider-b"
	if err := recorded.CheckReplay(mustRequest(t, otherProvider)); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("cross-provider check: %v, want ErrInvalidValue", err)
	}
}

func processedSnapshot(t *testing.T) TransactionSnapshot {
	t.Helper()
	w := mustWallet(t, "100.00")
	return processed(t, w, betInput("tx-1", "25.00"), ReferenceState{}).Snapshot()
}

func TestRehydrateTransactionRoundTripsWithoutEvents(t *testing.T) {
	snapshot := processedSnapshot(t)
	tx, err := RehydrateTransaction(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Snapshot() != snapshot {
		t.Fatalf("round trip changed the snapshot:\n got %+v\nwant %+v", tx.Snapshot(), snapshot)
	}
	if len(tx.PullEvents()) != 0 {
		t.Fatal("rehydration must not emit events")
	}
}

func TestRehydrateTransactionRejectsImpossibleStates(t *testing.T) {
	invalid := map[string]func(*TransactionSnapshot){
		"persisted PENDING":         func(s *TransactionSnapshot) { s.Status = StatusPending },
		"unknown status":            func(s *TransactionSnapshot) { s.Status = "DONE" },
		"unknown kind":              func(s *TransactionSnapshot) { s.Kind = "JACKPOT" },
		"processed without balance": func(s *TransactionSnapshot) { s.ResultBalance = money.Money{} },
		"processed with failure":    func(s *TransactionSnapshot) { s.FailureCode = FailureInsufficientFunds },
		"rejected without code":     func(s *TransactionSnapshot) { s.Status = StatusRejected },
		"failed with business code": func(s *TransactionSnapshot) {
			s.Status = StatusFailed
			s.FailureCode = FailureInsufficientFunds
		},
		"terminal with schedule": func(s *TransactionSnapshot) { s.NextAttemptAt = testNow },
		"pending reference for BET": func(s *TransactionSnapshot) {
			s.Status = StatusPendingReference
			s.ResultBalance = money.Money{}
			s.NextAttemptAt = testNow
		},
		"BET with reference":        func(s *TransactionSnapshot) { s.ReferenceExternalTransactionID = ExternalTransactionID{value: "tx-0"} },
		"external without provider": func(s *TransactionSnapshot) { s.ProviderID = ProviderID{} },
		"malformed payload hash":    func(s *TransactionSnapshot) { s.PayloadHash = "not-a-digest" },
		"OPENING with provider":     func(s *TransactionSnapshot) { s.Kind = KindOpening },
		"LOSS with an amount":       func(s *TransactionSnapshot) { s.Kind = KindLoss },
		"negative attempts":         func(s *TransactionSnapshot) { s.Attempts = -1 },
		"unknown failure code":      func(s *TransactionSnapshot) { s.Status = StatusRejected; s.FailureCode = "OOPS" },
	}
	for name, edit := range invalid {
		t.Run(name, func(t *testing.T) {
			s := processedSnapshot(t)
			edit(&s)
			if _, err := RehydrateTransaction(s); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestFailureClasses(t *testing.T) {
	correctable := []FailureCode{FailureWalletPlayerMismatch, FailureCurrencyMismatch, FailureReferenceKindNotAllowed, FailureReferenceMismatch, FailureReferenceAmountMismatch}
	for _, code := range correctable {
		if code.Class() != FailureClassCorrectable {
			t.Errorf("%s class = %s, want CORRECTABLE", code, code.Class())
		}
	}
	definitive := []FailureCode{FailureInsufficientFunds, FailureInsufficientFundsForReversal, FailureBalanceLimitExceeded, FailureReferenceNotFound, FailureReferenceNotProcessed, FailureReferenceAlreadyReversed, FailureProcessingFailed}
	for _, code := range definitive {
		if code.Class() != FailureClassDefinitive {
			t.Errorf("%s class = %s, want DEFINITIVE", code, code.Class())
		}
	}
	if FailureInsufficientFunds == FailureInsufficientFundsForReversal {
		t.Fatal("a reversal without funds must have its own failure code")
	}
	if _, err := ParseFailureCode("NOPE"); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("ParseFailureCode(NOPE) error = %v", err)
	}
}
