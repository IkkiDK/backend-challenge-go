package domain

import (
	"errors"
	"fmt"
	"time"
)

// ReferenceRetryPolicy bounds how long a transaction waits for its reference:
// resolution is retried with exponential backoff from BaseDelay, capped at
// MaxDelay, and the transaction is rejected on the MaxAttempts-th failed try.
type ReferenceRetryPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

// Validate reports a policy that could never expire or never wait.
func (p ReferenceRetryPolicy) Validate() error {
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.MaxAttempts < 1 {
		return fmt.Errorf("%w: reference retry policy needs 0 < base delay <= max delay and at least one attempt", ErrInvalidValue)
	}
	return nil
}

// Delay returns the wait before resolution attempt number attempt+1.
func (p ReferenceRetryPolicy) Delay(attempt int) time.Duration {
	delay := p.BaseDelay
	for range attempt {
		delay *= 2
		if delay >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	return delay
}

// ReferenceState is what is currently recorded for a transaction's reference.
type ReferenceState struct {
	// Transaction is the transaction found by (providerId,
	// referenceExternalTransactionId), or nil when none exists yet.
	Transaction *WagerTransaction
	// Reversed reports whether a PROCESSED REFUND or ROLLBACK already targets
	// Transaction, so it can be reversed at most once and never be settled by
	// a WIN afterwards.
	Reversed bool
}

// Settle applies the pending external transaction t to its locked wallet w.
//
// Business outcomes are not errors: t ends PROCESSED (with the ledger entry of
// the movement returned, or nil for LOSS), REJECTED with a failure code, or
// waiting for its reference per policy. An error means the inputs broke an
// invariant, such as t being terminal or belonging to another wallet.
func Settle(w *Wallet, t *WagerTransaction, ref ReferenceState, policy ReferenceRetryPolicy, now time.Time) (*LedgerEntry, error) {
	switch {
	case t.status.IsTerminal():
		return nil, t.transitionError(StatusProcessed)
	case t.external == nil:
		return nil, fmt.Errorf("%w: only external transactions are settled", ErrInvalidValue)
	case t.walletID != w.id:
		return nil, fmt.Errorf("%w: transaction %s belongs to wallet %s, not %s", ErrInvalidValue, t.id, t.walletID, w.id)
	}

	if t.playerID != w.playerID {
		return nil, t.reject(FailureWalletPlayerMismatch, w.balance, now)
	}
	if t.money.Currency() != w.Currency() {
		return nil, t.reject(FailureCurrencyMismatch, w.balance, now)
	}

	var target *WagerTransaction
	if !t.external.referenceExternalID.IsZero() {
		if ref.Transaction != nil {
			t.referenceID = ref.Transaction.id
		}
		code, wait := checkReference(t, ref)
		switch {
		case wait:
			return nil, t.waitForReference(ref.Transaction != nil, w, policy, now)
		case code != "":
			return nil, t.reject(code, w.balance, now)
		}
		target = ref.Transaction
	}

	entry, code, err := applyMovement(w, t, target, now)
	switch {
	case err != nil:
		return nil, err
	case code != "":
		return nil, t.reject(code, w.balance, now)
	}
	return entry, t.markProcessed(w.balance, t.referenceID, now)
}

// checkReference validates the transaction t references. wait reports that t
// must wait for the reference to arrive or settle; otherwise a non-empty code
// rejects t. Immutable properties are checked first, so a mismatching
// reference is rejected at once even while it is still pending itself.
func checkReference(t *WagerTransaction, ref ReferenceState) (code FailureCode, wait bool) {
	target := ref.Transaction
	switch {
	case target == nil:
		return "", true
	case !allowedReferenceKinds[t.kind][target.kind]:
		return FailureReferenceKindNotAllowed, false
	case target.ProviderID() != t.ProviderID(),
		target.walletID != t.walletID,
		target.playerID != t.playerID,
		target.money.Currency() != t.money.Currency(),
		target.roundID() != t.roundID():
		return FailureReferenceMismatch, false
	case t.kind.IsReversal() && !target.money.Equal(t.money):
		return FailureReferenceAmountMismatch, false
	case !target.status.IsTerminal():
		return "", true
	case target.status != StatusProcessed:
		return FailureReferenceNotProcessed, false
	case ref.Reversed:
		// Neither a second reversal nor a WIN may build on an undone transaction.
		return FailureReferenceAlreadyReversed, false
	}
	return "", false
}

// waitForReference parks a new transaction, or counts a failed retry and either
// reschedules it or, once the attempts are exhausted, rejects it.
func (t *WagerTransaction) waitForReference(referenceExists bool, w *Wallet, policy ReferenceRetryPolicy, now time.Time) error {
	if t.status == StatusPending {
		return t.AwaitReference(now.Add(policy.Delay(0)), now)
	}
	attempt := t.attempts + 1
	if attempt < policy.MaxAttempts {
		return t.RetryReferenceLater(now.Add(policy.Delay(attempt)), now)
	}
	t.attempts = attempt
	code := FailureReferenceNotFound
	if referenceExists {
		code = FailureReferenceNotProcessed
	}
	return t.reject(code, w.balance, now)
}

// applyMovement moves the wallet as t's kind requires. A ROLLBACK moves money
// opposite to the transaction it undoes, and running out of funds while
// reversing has its own failure code, distinct from a BET without funds.
func applyMovement(w *Wallet, t *WagerTransaction, target *WagerTransaction, now time.Time) (*LedgerEntry, FailureCode, error) {
	var (
		entry          LedgerEntry
		err            error
		noFundsFailure = FailureInsufficientFunds
	)
	switch {
	case t.kind == KindLoss:
		return nil, "", nil
	case t.kind == KindBet:
		entry, err = w.Debit(t.id, t.money, now)
	case t.kind == KindWin, t.kind == KindRefund:
		entry, err = w.Credit(t.id, t.money, now)
	case t.kind == KindRollback && target.kind == KindBet:
		entry, err = w.Credit(t.id, t.money, now)
	case t.kind == KindRollback:
		entry, err = w.Debit(t.id, t.money, now)
		noFundsFailure = FailureInsufficientFundsForReversal
	default:
		return nil, "", fmt.Errorf("%w: cannot settle %s", ErrInvalidValue, t.kind)
	}
	switch {
	case errors.Is(err, ErrInsufficientFunds):
		return nil, noFundsFailure, nil
	case errors.Is(err, ErrBalanceLimitExceeded):
		return nil, FailureBalanceLimitExceeded, nil
	case err != nil:
		return nil, "", err
	}
	return &entry, "", nil
}
