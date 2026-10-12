package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

// ResolveNextPendingReference retries the most overdue PENDING_REFERENCE
// transaction and reports whether one was due. Workers in any number of
// processes may call it concurrently: SKIP LOCKED hands each a different
// transaction, and the retry schedule lives in the database, so it survives
// restarts.
func (s *Service) ResolveNextPendingReference(ctx context.Context) (bool, error) {
	var claimed *domain.WagerTransaction
	err := s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		claimed = nil
		now := s.clock()
		t, err := tx.Transactions().ClaimDuePendingReference(ctx, now)
		if err != nil || t == nil {
			return err
		}
		claimed = t
		return s.resolve(ctx, tx, t, now)
	})
	if claimed == nil {
		return false, err
	}
	ctx = observability.WithLogAttrs(ctx,
		slog.String(observability.KeyTransactionID, claimed.ID().String()),
		slog.String(observability.KeyWalletID, claimed.WalletID().String()),
		slog.String(observability.KeyProviderID, claimed.ProviderID().String()),
		slog.String(observability.KeyCorrelationID, claimed.CorrelationID()))
	switch {
	case errors.Is(err, ErrIntegrity):
		s.metrics.PendingReferenceResolutions.WithLabelValues("failed").Inc()
		return true, s.failPending(ctx, claimed.ID(), err)
	case err != nil:
		s.metrics.Retries.WithLabelValues("reference_worker").Inc()
		return true, err
	}

	outcome := strings.ToLower(string(claimed.Status()))
	if claimed.Status() == domain.StatusPendingReference {
		outcome = "rescheduled"
	}
	s.metrics.PendingReferenceResolutions.WithLabelValues(outcome).Inc()
	if claimed.Status().IsTerminal() {
		s.metrics.TransactionResults.WithLabelValues("reference_worker", string(claimed.Kind()), string(claimed.Status())).Inc()
		s.logger.InfoContext(ctx, "pending reference resolved", "status", claimed.Status(), "failureCode", claimed.FailureCode())
	}
	return true, nil
}

func (s *Service) resolve(ctx context.Context, tx Tx, t *domain.WagerTransaction, now time.Time) error {
	// Lock order is transaction row, then wallet. The submit path locks the
	// wallet and never waits on a pending row (it reads them without locks,
	// and the claim's FOR NO KEY UPDATE does not block foreign-key checks),
	// so the two paths cannot deadlock.
	wallet, err := tx.Wallets().Lock(ctx, t.WalletID())
	if err != nil {
		return err
	}
	ref, err := referenceState(ctx, tx.Transactions(), t.ProviderID(), t.ReferenceExternalTransactionID())
	if err != nil {
		return err
	}
	previousVersion := wallet.Version()
	entry, err := domain.Settle(wallet, t, ref, s.policy, now)
	if err != nil {
		return err
	}
	if err := tx.Transactions().Update(ctx, t); err != nil {
		return err
	}
	if err := persistMovement(ctx, tx, wallet, entry, previousVersion); err != nil {
		return err
	}
	events := append(wallet.PullEvents(), t.PullEvents()...)
	return appendEvents(ctx, tx, events, t.CorrelationID(), "")
}

// failPending records a pending transaction that hit a database invariant as
// FAILED, so the worker stops retrying it.
func (s *Service) failPending(ctx context.Context, id domain.TransactionID, cause error) error {
	s.logger.ErrorContext(ctx, "pending transaction violates a database invariant; recording it as FAILED", "error", cause)
	return s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		t, err := tx.Transactions().Lock(ctx, id)
		if err != nil {
			return err
		}
		if t.Status().IsTerminal() {
			return nil
		}
		if err := t.Fail(s.clock()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t)
	})
}
