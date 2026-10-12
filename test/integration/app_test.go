//go:build integration

package integration

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

func TestOpeningCommitsWalletTransactionLedgerAndEventsTogether(t *testing.T) {
	p := newInProcess(t, testPolicy)

	funded := openWalletInProcess(t, p, "1000.00")
	if got := walletState(t, funded.ID); got.balanceMinor != 100000 || got.version != 1 {
		t.Fatalf("wallet = %+v, want 1000.00 at version 1", got)
	}
	if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL'`, funded.ID); n != 1 {
		t.Fatalf("OPENING transactions = %d, want 1", n)
	}
	if ledgerEntries(t, funded.ID) != 1 {
		t.Fatal("a funded opening needs its credit entry")
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND event_type IN ('WalletBalanceChanged', 'WagerTransactionProcessed')`, funded.ID); n != 2 {
		t.Fatalf("opening events = %d, want 2", n)
	}

	empty := openWalletInProcess(t, p, "0.00")
	if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1`, empty.ID) +
		ledgerEntries(t, empty.ID) +
		countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1`, empty.ID); n != 0 {
		t.Fatalf("a zero opening created %d financial records", n)
	}
}

func TestReplayReturnsTheBalanceObservedByTheOriginalRequest(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	first := newOperation(w, "BET", "replay-1-"+w.ID, "10.00")
	if _, err := p.service.Submit(t.Context(), mustRequest(t, first), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.service.Submit(t.Context(), mustRequest(t, newOperation(w, "BET", "replay-2-"+w.ID, "20.00")), httpOrigin()); err != nil {
		t.Fatal(err)
	}

	replay, err := p.service.Submit(t.Context(), mustRequest(t, first), httpOrigin())
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.Transaction.ResultBalance().Amount() != "90.00" {
		t.Fatalf("replay = replayed %t balance %s, want the original 90.00", replay.Replayed, replay.Transaction.ResultBalance())
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}

func TestIdempotencyConflictsAndProviderScopedKeys(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	op := newOperation(w, "BET", "conflict-"+w.ID, "10.00")
	if _, err := p.service.Submit(t.Context(), mustRequest(t, op), httpOrigin()); err != nil {
		t.Fatal(err)
	}

	changed := op
	changed.Money = brl("11.00")
	if _, err := p.service.Submit(t.Context(), mustRequest(t, changed), httpOrigin()); !errors.Is(err, domain.ErrIdempotencyKeyReused) {
		t.Fatalf("same key, different payload: %v, want ErrIdempotencyKeyReused", err)
	}

	otherKey := mustRequestWithKey(t, op, "another-key-"+w.ID)
	if _, err := p.service.Submit(t.Context(), otherKey, httpOrigin()); !errors.Is(err, domain.ErrExternalTransactionConflict) {
		t.Fatalf("same operation, other key: %v, want ErrExternalTransactionConflict", err)
	}

	// The same key text from another provider is another provider's key: it
	// neither replays nor conflicts with provider-a's transaction.
	otherProvider := op
	otherProvider.ProviderID = "provider-b"
	result, err := p.service.Submit(t.Context(), mustRequestWithKey(t, otherProvider, op.key()), httpOrigin())
	if err != nil || result.Replayed || result.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("provider-b with provider-a's key text: %+v, %v", result, err)
	}
	requireLedgerMatchesBalance(t, w.ID, 8000)
}

func mustRequestWithKey(t *testing.T, op operation, key string) domain.TransactionRequest {
	t.Helper()
	req, err := domain.NewTransactionRequest(domain.TransactionRequestInput{
		ProviderID: op.ProviderID, ExternalTransactionID: op.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: op.PlayerID, WalletID: op.WalletID, RoundID: op.RoundID, GameID: op.GameID, Kind: op.Kind,
		Amount: op.Money.Amount, Currency: op.Money.Currency, ReferenceExternalTransactionID: op.Reference,
	})
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestReversalArrivingBeforeItsReferenceIsResolvedLater(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	bet := newOperation(w, "BET", "late-bet-"+w.ID, "30.00")
	refund := newOperation(w, "REFUND", "early-refund-"+w.ID, "30.00")
	refund.Reference = bet.ExternalTransactionID

	pending, err := p.service.Submit(t.Context(), mustRequest(t, refund), httpOrigin())
	if err != nil || pending.Transaction.Status() != domain.StatusPendingReference {
		t.Fatalf("early REFUND: %+v, %v; want PENDING_REFERENCE", pending, err)
	}
	if _, err := p.service.Submit(t.Context(), mustRequest(t, bet), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the pending REFUND to be resolved", func() bool {
		if _, err := p.service.ResolveNextPendingReference(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, _ := transactionStatus(t, "provider-a", refund.ExternalTransactionID)
		return status == "PROCESSED"
	})
	requireLedgerMatchesBalance(t, w.ID, 10000)
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionPendingReference'`, pending.Transaction.ID().String()); n != 1 {
		t.Fatalf("pending events = %d, want 1", n)
	}
}

func TestReversalWhoseReferenceNeverArrivesExpires(t *testing.T) {
	policy := domain.ReferenceRetryPolicy{BaseDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond, MaxAttempts: 3}
	p := newInProcess(t, policy)
	w := openWalletInProcess(t, p, "100.00")
	rollback := newOperation(w, "ROLLBACK", "orphan-rollback-"+w.ID, "30.00")
	rollback.Reference = "never-sent-" + w.ID
	if _, err := p.service.Submit(t.Context(), mustRequest(t, rollback), httpOrigin()); err != nil {
		t.Fatal(err)
	}

	eventually(t, 10*time.Second, "the orphan ROLLBACK to expire", func() bool {
		if _, err := p.service.ResolveNextPendingReference(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, _ := transactionStatus(t, "provider-a", rollback.ExternalTransactionID)
		return status == "REJECTED"
	})
	if _, code := transactionStatus(t, "provider-a", rollback.ExternalTransactionID); code != string(domain.FailureReferenceNotFound) {
		t.Fatalf("failure code = %s, want REFERENCE_NOT_FOUND", code)
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND event_type = 'WagerTransactionRejected'`, w.ID); n != 1 {
		t.Fatalf("rejection events = %d, want 1", n)
	}
	requireLedgerMatchesBalance(t, w.ID, 10000)
}

func TestRefundAndRollbackCannotReturnTheSameDebitTwice(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	bet := newOperation(w, "BET", "double-bet-"+w.ID, "40.00")
	refund := newOperation(w, "REFUND", "double-refund-"+w.ID, "40.00")
	refund.Reference = bet.ExternalTransactionID
	rollback := newOperation(w, "ROLLBACK", "double-rollback-"+w.ID, "40.00")
	rollback.Reference = bet.ExternalTransactionID

	// The REFUND and the ROLLBACK of the same BET race; at most one may succeed.
	if _, err := p.service.Submit(t.Context(), mustRequest(t, bet), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, op := range []operation{refund, rollback} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.service.Submit(t.Context(), mustRequest(t, op), httpOrigin()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	refundStatus, _ := transactionStatus(t, "provider-a", refund.ExternalTransactionID)
	rollbackStatus, rollbackCode := transactionStatus(t, "provider-a", rollback.ExternalTransactionID)
	if refundStatus == "PROCESSED" && rollbackStatus == "PROCESSED" {
		t.Fatal("the BET was returned twice")
	}
	if refundStatus != "PROCESSED" && rollbackStatus != "PROCESSED" {
		t.Fatalf("neither reversal succeeded: %s / %s %s", refundStatus, rollbackStatus, rollbackCode)
	}
	requireLedgerMatchesBalance(t, w.ID, 10000)
}

func TestIndependentWalletsDoNotWaitForEachOther(t *testing.T) {
	p := newInProcess(t, testPolicy)
	locked := openWalletInProcess(t, p, "100.00")
	free := openWalletInProcess(t, p, "100.00")
	ctx := t.Context()

	// A slow writer holds the row lock of one wallet.
	tx, err := env.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, locked.ID); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	if _, err := p.service.Submit(ctx, mustRequest(t, newOperation(free, "BET", "free-bet-"+free.ID, "10.00")), httpOrigin()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("a bet on another wallet waited %v: there must be no global lock", elapsed)
	}

	blocked := make(chan error, 1)
	go func() {
		_, err := p.service.Submit(ctx, mustRequest(t, newOperation(locked, "BET", "locked-bet-"+locked.ID, "10.00")), httpOrigin())
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("a bet on the locked wallet must wait for its lock, finished with %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err != nil {
		t.Fatalf("bet after the lock was released: %v", err)
	}
	requireLedgerMatchesBalance(t, locked.ID, 9000)
	requireLedgerMatchesBalance(t, free.ID, 9000)
}

func TestCorruptedBalanceIsCaughtByTheDatabaseAndByReconciliation(t *testing.T) {
	p := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, p, "100.00")
	ctx := t.Context()

	// Corrupt the balance behind every guard: a superuser session with
	// triggers disabled, as a manual fix gone wrong would.
	err := pgx.BeginFunc(ctx, env.owner, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor = 50000 WHERE id = $1`, w.ID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	walletID, err := domain.ParseWalletID(w.ID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := p.service.Reconcile(ctx, walletID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Consistent || report.Difference.Amount() != "400.00" || report.CalculatedBalance.Amount() != "100.00" {
		t.Fatalf("reconciliation = %+v, want a 400.00 divergence", report)
	}
	if testutil.ToFloat64(p.metrics.ReconciliationDivergences) != 1 {
		t.Fatal("the divergence must be counted in the metrics")
	}

	// The next movement cannot chain onto the ledger: the database refuses it
	// and the operation is recorded as FAILED instead of moving money.
	bet := newOperation(w, "BET", "corrupted-bet-"+w.ID, "10.00")
	result, err := p.service.Submit(ctx, mustRequest(t, bet), httpOrigin())
	if err != nil || result.Transaction.Status() != domain.StatusFailed || result.Transaction.FailureCode() != domain.FailureProcessingFailed {
		t.Fatalf("bet on a corrupted wallet: %+v, %v; want FAILED", result, err)
	}
	if got := walletState(t, w.ID); got.balanceMinor != 50000 || got.version != 1 || ledgerEntries(t, w.ID) != 1 {
		t.Fatalf("the failed operation moved the wallet: %+v", got)
	}
	replay, err := p.service.Submit(ctx, mustRequest(t, bet), httpOrigin())
	if err != nil || !replay.Replayed || replay.Transaction.Status() != domain.StatusFailed {
		t.Fatalf("replay of a FAILED operation: %+v, %v", replay, err)
	}
}
