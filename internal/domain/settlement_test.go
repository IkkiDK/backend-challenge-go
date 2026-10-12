package domain

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// requireOutcome checks status, failure code, balance and version after Settle.
func requireOutcome(t *testing.T, w *Wallet, tx *WagerTransaction, status Status, code FailureCode, balance string, version int64) {
	t.Helper()
	if tx.Status() != status || tx.FailureCode() != code {
		t.Fatalf("%s: status %s/%q, want %s/%q", tx.Kind(), tx.Status(), tx.FailureCode(), status, code)
	}
	if w.Balance().Amount() != balance || w.Version() != version {
		t.Fatalf("%s: wallet balance %s version %d, want %s version %d", tx.Kind(), w.Balance(), w.Version(), balance, version)
	}
	if status.IsTerminal() && status != StatusFailed && tx.ResultBalance().Amount() != balance {
		t.Fatalf("%s: result balance %s, want %s", tx.Kind(), tx.ResultBalance(), balance)
	}
}

func TestBet(t *testing.T) {
	t.Run("debits a sufficient balance", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		tx := mustTransaction(t, betInput("tx-1", "25.00"))
		entry := settle(t, w, tx, ReferenceState{})
		requireOutcome(t, w, tx, StatusProcessed, "", "75.00", 2)
		if entry == nil || entry.Direction() != DirectionDebit || entry.Amount().Amount() != "25.00" {
			t.Fatalf("entry = %+v, want a 25.00 debit", entry)
		}
		if got := append(eventTypes(tx.PullEvents()), eventTypes(w.PullEvents())...); !slices.Equal(got, []string{"WagerTransactionProcessed", "WalletBalanceChanged"}) {
			t.Fatalf("events = %v", got)
		}
	})

	t.Run("is rejected without funds and leaves the wallet untouched", func(t *testing.T) {
		w := mustWallet(t, "20.00")
		tx := mustTransaction(t, betInput("tx-1", "20.01"))
		if entry := settle(t, w, tx, ReferenceState{}); entry != nil {
			t.Fatal("a rejected BET must not produce a ledger entry")
		}
		requireOutcome(t, w, tx, StatusRejected, FailureInsufficientFunds, "20.00", 1)
		if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionRejected"}) || len(w.PullEvents()) != 0 {
			t.Fatalf("events = %v, want only WagerTransactionRejected", got)
		}
	})
}

func TestWin(t *testing.T) {
	t.Run("credits without a reference", func(t *testing.T) {
		w := mustWallet(t, "10.00")
		tx := mustTransaction(t, operationInput(KindWin, "tx-1", "40.00", ""))
		entry := settle(t, w, tx, ReferenceState{})
		requireOutcome(t, w, tx, StatusProcessed, "", "50.00", 2)
		if entry.Direction() != DirectionCredit {
			t.Fatal("a WIN must credit the wallet")
		}
	})

	t.Run("resolves the BET of the same round", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		bet := processed(t, w, betInput("tx-1", "10.00"), ReferenceState{})
		win := mustTransaction(t, operationInput(KindWin, "tx-2", "30.00", "tx-1"))
		settle(t, w, win, ReferenceState{Transaction: bet})
		requireOutcome(t, w, win, StatusProcessed, "", "120.00", 3)
		if win.Snapshot().ReferenceTransactionID != bet.ID() {
			t.Fatal("the resolved reference must be recorded")
		}
	})

	t.Run("rejects a BET of another round", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		otherRound := betInput("tx-1", "10.00")
		otherRound.RoundID = "round-1"
		bet := processed(t, w, otherRound, ReferenceState{})
		win := mustTransaction(t, operationInput(KindWin, "tx-2", "30.00", "tx-1"))
		settle(t, w, win, ReferenceState{Transaction: bet})
		requireOutcome(t, w, win, StatusRejected, FailureReferenceMismatch, "90.00", 2)
	})

	t.Run("cannot settle a reversed BET", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		bet := processed(t, w, betInput("tx-1", "10.00"), ReferenceState{})
		win := mustTransaction(t, operationInput(KindWin, "tx-2", "30.00", "tx-1"))
		settle(t, w, win, ReferenceState{Transaction: bet, Reversed: true})
		requireOutcome(t, w, win, StatusRejected, FailureReferenceAlreadyReversed, "90.00", 2)
	})
}

func TestLossMovesNothingButCompletes(t *testing.T) {
	w := mustWallet(t, "100.00")
	tx := mustTransaction(t, operationInput(KindLoss, "tx-1", "0.00", ""))
	if entry := settle(t, w, tx, ReferenceState{}); entry != nil {
		t.Fatal("a LOSS must not produce a ledger entry")
	}
	requireOutcome(t, w, tx, StatusProcessed, "", "100.00", 1)
	if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionProcessed"}) || len(w.PullEvents()) != 0 {
		t.Fatalf("events = %v, want only WagerTransactionProcessed and no WalletBalanceChanged", got)
	}
}

func TestRefund(t *testing.T) {
	setup := func(t *testing.T) (*Wallet, *WagerTransaction) {
		w := mustWallet(t, "100.00")
		return w, processed(t, w, betInput("bet-1", "30.00"), ReferenceState{})
	}

	t.Run("returns the full amount of a processed BET", func(t *testing.T) {
		w, bet := setup(t)
		refund := mustTransaction(t, operationInput(KindRefund, "refund-1", "30.00", "bet-1"))
		entry := settle(t, w, refund, ReferenceState{Transaction: bet})
		requireOutcome(t, w, refund, StatusProcessed, "", "100.00", 3)
		if entry.Direction() != DirectionCredit || refund.Snapshot().ReferenceTransactionID != bet.ID() {
			t.Fatal("a REFUND must credit and record the BET it returns")
		}
	})

	rejections := map[string]struct {
		amount   string
		ref      func(bet *WagerTransaction) ReferenceState
		wantCode FailureCode
	}{
		"partial amount":    {"10.00", func(b *WagerTransaction) ReferenceState { return ReferenceState{Transaction: b} }, FailureReferenceAmountMismatch},
		"already reversed":  {"30.00", func(b *WagerTransaction) ReferenceState { return ReferenceState{Transaction: b, Reversed: true} }, FailureReferenceAlreadyReversed},
		"of a rejected BET": {"30.00", func(b *WagerTransaction) ReferenceState { return ReferenceState{Transaction: rejectedCopy(t, b)} }, FailureReferenceNotProcessed},
		"of a WIN":          {"30.00", func(b *WagerTransaction) ReferenceState { return ReferenceState{Transaction: withKind(t, b, KindWin)} }, FailureReferenceKindNotAllowed},
		"of another player": {"30.00", func(b *WagerTransaction) ReferenceState { return ReferenceState{Transaction: withPlayer(t, b)} }, FailureReferenceMismatch},
		"in another currency": {"30.00", func(b *WagerTransaction) ReferenceState {
			return ReferenceState{Transaction: withCurrency(t, b, "USD")}
		}, FailureReferenceMismatch},
	}
	for name, tt := range rejections {
		t.Run("rejects "+name, func(t *testing.T) {
			w, bet := setup(t)
			refund := mustTransaction(t, operationInput(KindRefund, "refund-1", tt.amount, "bet-1"))
			settle(t, w, refund, tt.ref(bet))
			requireOutcome(t, w, refund, StatusRejected, tt.wantCode, "70.00", 2)
		})
	}
}

func TestRollback(t *testing.T) {
	t.Run("of a BET credits it back", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		bet := processed(t, w, betInput("bet-1", "30.00"), ReferenceState{})
		rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "30.00", "bet-1"))
		entry := settle(t, w, rollback, ReferenceState{Transaction: bet})
		requireOutcome(t, w, rollback, StatusProcessed, "", "100.00", 3)
		if entry.Direction() != DirectionCredit {
			t.Fatal("rolling back a BET must credit")
		}
	})

	t.Run("of a WIN debits it back", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		win := processed(t, w, operationInput(KindWin, "win-1", "50.00", ""), ReferenceState{})
		rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "50.00", "win-1"))
		entry := settle(t, w, rollback, ReferenceState{Transaction: win})
		requireOutcome(t, w, rollback, StatusProcessed, "", "100.00", 3)
		if entry.Direction() != DirectionDebit {
			t.Fatal("rolling back a WIN must debit")
		}
	})

	t.Run("of a REFUND debits it back", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		bet := processed(t, w, betInput("bet-1", "30.00"), ReferenceState{})
		refund := processed(t, w, operationInput(KindRefund, "refund-1", "30.00", "bet-1"), ReferenceState{Transaction: bet})
		rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "30.00", "refund-1"))
		settle(t, w, rollback, ReferenceState{Transaction: refund})
		requireOutcome(t, w, rollback, StatusProcessed, "", "70.00", 4)
	})

	t.Run("without funds has its own failure code", func(t *testing.T) {
		w := mustWallet(t, "0.00")
		win := processed(t, w, operationInput(KindWin, "win-1", "50.00", ""), ReferenceState{})
		processed(t, w, betInput("bet-1", "40.00"), ReferenceState{}) // spends most of the WIN
		rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "50.00", "win-1"))
		settle(t, w, rollback, ReferenceState{Transaction: win})
		requireOutcome(t, w, rollback, StatusRejected, FailureInsufficientFundsForReversal, "10.00", 3)
	})

	for _, kind := range []Kind{KindLoss, KindRollback} {
		t.Run("cannot undo a "+string(kind), func(t *testing.T) {
			w := mustWallet(t, "100.00")
			target := processed(t, w, betInput("bet-1", "30.00"), ReferenceState{})
			rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "30.00", "bet-1"))
			settle(t, w, rollback, ReferenceState{Transaction: withKind(t, target, kind)})
			requireOutcome(t, w, rollback, StatusRejected, FailureReferenceKindNotAllowed, "70.00", 2)
		})
	}
}

func TestWalletMismatchesAreRejected(t *testing.T) {
	t.Run("player", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		in := betInput("tx-1", "10.00")
		in.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2"
		tx := mustTransaction(t, in)
		settle(t, w, tx, ReferenceState{})
		requireOutcome(t, w, tx, StatusRejected, FailureWalletPlayerMismatch, "100.00", 1)
	})
	t.Run("currency", func(t *testing.T) {
		w := mustWallet(t, "100.00")
		in := betInput("tx-1", "10.00")
		in.Currency = "USD"
		tx := mustTransaction(t, in)
		settle(t, w, tx, ReferenceState{})
		requireOutcome(t, w, tx, StatusRejected, FailureCurrencyMismatch, "100.00", 1)
	})
}

func TestMissingReferenceIsRetriedWithBackoffThenRejected(t *testing.T) {
	w := mustWallet(t, "100.00")
	refund := mustTransaction(t, operationInput(KindRefund, "refund-1", "30.00", "bet-1"))

	settle(t, w, refund, ReferenceState{})
	snapshot := refund.Snapshot()
	if refund.Status() != StatusPendingReference || !snapshot.NextAttemptAt.Equal(testNow.Add(testPolicy.BaseDelay)) {
		t.Fatalf("first wait: %s next %v, want PENDING_REFERENCE at now+%v", refund.Status(), snapshot.NextAttemptAt, testPolicy.BaseDelay)
	}
	if got := eventTypes(refund.PullEvents()); !slices.Equal(got, []string{"WagerTransactionPendingReference"}) {
		t.Fatalf("events = %v", got)
	}

	wantDelays := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	for attempt := 1; attempt < testPolicy.MaxAttempts; attempt++ {
		settle(t, w, refund, ReferenceState{})
		snapshot = refund.Snapshot()
		if refund.Status() != StatusPendingReference || snapshot.Attempts != attempt ||
			!snapshot.NextAttemptAt.Equal(testNow.Add(wantDelays[attempt-1])) {
			t.Fatalf("attempt %d: %s attempts %d next %v", attempt, refund.Status(), snapshot.Attempts, snapshot.NextAttemptAt)
		}
		if len(refund.PullEvents()) != 0 {
			t.Fatal("rescheduling must not emit events")
		}
	}

	settle(t, w, refund, ReferenceState{})
	requireOutcome(t, w, refund, StatusRejected, FailureReferenceNotFound, "100.00", 1)
	if got := eventTypes(refund.PullEvents()); !slices.Equal(got, []string{"WagerTransactionRejected"}) {
		t.Fatalf("events = %v", got)
	}
}

func TestReferenceThatExistsButIsStillPending(t *testing.T) {
	w := mustWallet(t, "100.00")
	pendingRefund := mustTransaction(t, operationInput(KindRefund, "refund-1", "30.00", "bet-1"))
	settle(t, w, pendingRefund, ReferenceState{})

	rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "30.00", "refund-1"))
	settle(t, w, rollback, ReferenceState{Transaction: pendingRefund})
	if rollback.Status() != StatusPendingReference || rollback.Snapshot().ReferenceTransactionID != pendingRefund.ID() {
		t.Fatalf("status %s, want PENDING_REFERENCE while the reference is unresolved", rollback.Status())
	}
	for range testPolicy.MaxAttempts {
		settle(t, w, rollback, ReferenceState{Transaction: pendingRefund})
	}
	requireOutcome(t, w, rollback, StatusRejected, FailureReferenceNotProcessed, "100.00", 1)
}

func TestMismatchingReferenceIsRejectedWithoutWaitingForIt(t *testing.T) {
	w := mustWallet(t, "100.00")
	pendingRefund := mustTransaction(t, operationInput(KindRefund, "refund-1", "30.00", "bet-1"))
	settle(t, w, pendingRefund, ReferenceState{})

	rollback := mustTransaction(t, operationInput(KindRollback, "rb-1", "29.99", "refund-1"))
	settle(t, w, rollback, ReferenceState{Transaction: pendingRefund})
	requireOutcome(t, w, rollback, StatusRejected, FailureReferenceAmountMismatch, "100.00", 1)
}

func TestSettleRefusesInvariantViolations(t *testing.T) {
	w := mustWallet(t, "100.00")
	done := processed(t, w, betInput("tx-1", "10.00"), ReferenceState{})
	if _, err := Settle(w, done, ReferenceState{}, testPolicy, testNow); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("settling a terminal transaction: %v, want ErrInvalidTransition", err)
	}

	foreign := betInput("tx-2", "10.00")
	foreign.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef38"
	if _, err := Settle(w, mustTransaction(t, foreign), ReferenceState{}, testPolicy, testNow); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("settling another wallet's transaction: %v, want ErrInvalidValue", err)
	}
}

func TestReferenceRetryPolicy(t *testing.T) {
	p := ReferenceRetryPolicy{BaseDelay: 500 * time.Millisecond, MaxDelay: 3 * time.Second, MaxAttempts: 5}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}
	for attempt, delay := range want {
		if got := p.Delay(attempt); got != delay {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, delay)
		}
	}
	if got := p.Delay(1000); got != p.MaxDelay {
		t.Errorf("Delay(1000) = %v, want the cap", got)
	}
	for _, invalid := range []ReferenceRetryPolicy{
		{BaseDelay: 0, MaxDelay: time.Second, MaxAttempts: 1},
		{BaseDelay: time.Second, MaxDelay: time.Millisecond, MaxAttempts: 1},
		{BaseDelay: time.Second, MaxDelay: time.Second, MaxAttempts: 0},
	} {
		if err := invalid.Validate(); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("Validate(%+v) = %v, want ErrInvalidValue", invalid, err)
		}
	}
}

// The helpers below derive references with one property changed, through the
// persisted form, as the application would load them.

func rehydrated(t *testing.T, s TransactionSnapshot) *WagerTransaction {
	t.Helper()
	tx, err := RehydrateTransaction(s)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func rejectedCopy(t *testing.T, tx *WagerTransaction) *WagerTransaction {
	s := tx.Snapshot()
	s.Status, s.FailureCode = StatusRejected, FailureInsufficientFunds
	return rehydrated(t, s)
}

func withKind(t *testing.T, tx *WagerTransaction, kind Kind) *WagerTransaction {
	s := tx.Snapshot()
	s.Kind = kind
	if kind == KindLoss {
		s.Money = mustMoney(t, "0.00", "BRL")
	}
	if kind == KindRollback {
		s.ReferenceExternalTransactionID = ExternalTransactionID{value: "bet-0"}
		s.ReferenceTransactionID = NewTransactionID()
	}
	return rehydrated(t, s)
}

func withPlayer(t *testing.T, tx *WagerTransaction) *WagerTransaction {
	s := tx.Snapshot()
	other, err := ParsePlayerID("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2")
	if err != nil {
		t.Fatal(err)
	}
	s.PlayerID = other
	return rehydrated(t, s)
}

func withCurrency(t *testing.T, tx *WagerTransaction, currency string) *WagerTransaction {
	s := tx.Snapshot()
	s.Money = mustMoney(t, s.Money.Amount(), currency)
	return rehydrated(t, s)
}
