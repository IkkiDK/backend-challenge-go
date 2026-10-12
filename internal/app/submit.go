package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

// Channel names the entry point of a request.
type Channel string

// Entry points of external operations.
const (
	ChannelHTTP Channel = "http"
	ChannelSQS  Channel = "sqs"
)

// Origin describes where an external operation came from.
type Origin struct {
	Channel       Channel
	CorrelationID string
	// Message identifies the SQS delivery being handled; nil for HTTP. Its
	// inbox record commits in the same SQL transaction as the outcome.
	Message *InboundMessage
}

// InboundMessage identifies one consumed message.
type InboundMessage struct {
	Consumer   string
	MessageID  string
	Hash       string
	ReceivedAt time.Time
}

// completedAt dates the end of handling m at now, unless a wall clock that
// stepped back would date it before the message was received.
func (m *InboundMessage) completedAt(now time.Time) time.Time {
	if now.Before(m.ReceivedAt) {
		return m.ReceivedAt
	}
	return now
}

// SubmitResult is the outcome of an external operation.
type SubmitResult struct {
	Transaction *domain.WagerTransaction
	// Replayed reports that the outcome was recorded by an earlier request or
	// delivery and is returned unchanged, including its original balance.
	Replayed bool
}

// Submit records and settles an external wagering operation exactly once.
//
// Repeated requests (same provider and idempotency key, same payload) and
// redelivered messages get the stored outcome back. A key reused for another
// payload fails with domain.ErrIdempotencyKeyReused; an operation resubmitted
// under another key fails with domain.ErrExternalTransactionConflict.
// Business refusals are not errors: they come back as REJECTED transactions.
func (s *Service) Submit(ctx context.Context, req domain.TransactionRequest, origin Origin) (SubmitResult, error) {
	started := time.Now()
	result, err := s.submit(ctx, req, origin)
	s.metrics.ProcessingDuration.WithLabelValues(string(origin.Channel)).Observe(time.Since(started).Seconds())
	s.observeSubmit(ctx, req, origin, result, err)
	return result, err
}

func (s *Service) submit(ctx context.Context, req domain.TransactionRequest, origin Origin) (SubmitResult, error) {
	// Fast path: duplicates are answered without taking the wallet lock.
	reader := s.store.Reader()
	if origin.Message != nil {
		if result, found, err := consumedMessage(ctx, reader.Inbox(), reader.Transactions(), origin.Message); found || err != nil {
			return result, err
		}
	} else if result, found, err := recordedOperation(ctx, reader.Transactions(), req); found || err != nil {
		return result, err
	}

	result, err := s.submitInUnitOfWork(ctx, req, origin)
	if errors.Is(err, ErrDuplicate) {
		// A concurrent request or delivery committed the same key, operation
		// or message first; a new unit of work finds and replays it.
		s.metrics.ConcurrencyConflicts.WithLabelValues("concurrent_duplicate").Inc()
		result, err = s.submitInUnitOfWork(ctx, req, origin)
	}
	if errors.Is(err, ErrIntegrity) {
		return s.recordFailure(ctx, req, origin, err)
	}
	return result, err
}

func (s *Service) submitInUnitOfWork(ctx context.Context, req domain.TransactionRequest, origin Origin) (SubmitResult, error) {
	var result SubmitResult
	err := s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		lockStarted := time.Now()
		wallet, err := tx.Wallets().Lock(ctx, req.WalletID())
		s.metrics.WalletLockWait.Observe(time.Since(lockStarted).Seconds())
		if err != nil {
			return err
		}
		// Under the wallet lock, the checks below cannot race with another
		// writer of this wallet: whatever committed before is visible now.
		var found bool
		if result, found, err = consumedMessage(ctx, tx.Inbox(), tx.Transactions(), origin.Message); found || err != nil {
			return err
		}
		if result, found, err = recordedOperation(ctx, tx.Transactions(), req); err != nil {
			return err
		}
		if !found {
			if result, err = s.settle(ctx, tx, wallet, req, origin); err != nil {
				return err
			}
		}
		if m := origin.Message; m != nil {
			return tx.Inbox().Insert(ctx, InboxRecord{
				Consumer:      m.Consumer,
				MessageID:     m.MessageID,
				Hash:          m.Hash,
				TransactionID: result.Transaction.ID(),
				ReceivedAt:    m.ReceivedAt,
				CompletedAt:   m.completedAt(s.clock()),
			})
		}
		return nil
	})
	return result, err
}

// settle creates the transaction of req and applies it to the locked wallet:
// transaction row, then balance, then ledger entry, then outbox events.
func (s *Service) settle(ctx context.Context, tx Tx, wallet *domain.Wallet, req domain.TransactionRequest, origin Origin) (SubmitResult, error) {
	now := s.clock()
	t, err := domain.NewExternalTransaction(domain.NewTransactionID(), req, origin.CorrelationID, now)
	if err != nil {
		return SubmitResult{}, err
	}
	ref, err := referenceState(ctx, tx.Transactions(), req.ProviderID(), req.ReferenceExternalTransactionID())
	if err != nil {
		return SubmitResult{}, err
	}
	previousVersion := wallet.Version()
	entry, err := domain.Settle(wallet, t, ref, s.policy, now)
	if err != nil {
		return SubmitResult{}, err
	}
	if err := tx.Transactions().Insert(ctx, t); err != nil {
		return SubmitResult{}, err
	}
	if err := persistMovement(ctx, tx, wallet, entry, previousVersion); err != nil {
		return SubmitResult{}, err
	}
	events := append(wallet.PullEvents(), t.PullEvents()...)
	if err := appendEvents(ctx, tx, events, origin.CorrelationID, causationID(origin)); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: t}, nil
}

// recordFailure records an operation that hit a database invariant as FAILED,
// so it has a terminal, auditable outcome instead of failing on every retry.
func (s *Service) recordFailure(ctx context.Context, req domain.TransactionRequest, origin Origin, cause error) (SubmitResult, error) {
	s.logger.ErrorContext(ctx, "operation violates a database invariant; recording it as FAILED", "error", cause)
	var result SubmitResult
	err := s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		now := s.clock()
		t, err := domain.NewExternalTransaction(domain.NewTransactionID(), req, origin.CorrelationID, now)
		if err != nil {
			return err
		}
		if err := t.Fail(now); err != nil {
			return err
		}
		if err := tx.Transactions().Insert(ctx, t); err != nil {
			return err
		}
		if m := origin.Message; m != nil {
			if err := tx.Inbox().Insert(ctx, InboxRecord{
				Consumer: m.Consumer, MessageID: m.MessageID, Hash: m.Hash,
				TransactionID: t.ID(), ReceivedAt: m.ReceivedAt, CompletedAt: m.completedAt(now),
			}); err != nil {
				return err
			}
		}
		result = SubmitResult{Transaction: t}
		return nil
	})
	if err != nil {
		return SubmitResult{}, fmt.Errorf("record failed operation: %w (cause: %w)", err, cause)
	}
	return result, nil
}

// consumedMessage returns the outcome of a message the consumer already
// handled. A redelivery must carry the same content hash.
func consumedMessage(ctx context.Context, inbox InboxReader, txs TransactionReader, m *InboundMessage) (SubmitResult, bool, error) {
	if m == nil {
		return SubmitResult{}, false, nil
	}
	record, err := inbox.Find(ctx, m.Consumer, m.MessageID)
	if err != nil || record == nil {
		return SubmitResult{}, false, err
	}
	if record.Hash != m.Hash {
		return SubmitResult{}, true, ErrMessageConflict
	}
	t, err := txs.ByID(ctx, record.TransactionID)
	if err != nil {
		return SubmitResult{}, true, err
	}
	return SubmitResult{Transaction: t, Replayed: true}, true, nil
}

// recordedOperation returns the outcome already recorded for req's
// idempotency key or for its (providerId, externalTransactionId).
func recordedOperation(ctx context.Context, txs TransactionReader, req domain.TransactionRequest) (SubmitResult, bool, error) {
	existing, err := txs.ByIdempotencyKey(ctx, req.ProviderID(), req.IdempotencyKey())
	if err == nil && existing == nil {
		existing, err = txs.ByExternalID(ctx, req.ProviderID(), req.ExternalTransactionID())
	}
	if err != nil || existing == nil {
		return SubmitResult{}, false, err
	}
	if err := existing.CheckReplay(req); err != nil {
		return SubmitResult{}, true, err
	}
	return SubmitResult{Transaction: existing, Replayed: true}, true, nil
}

// referenceState looks up the transaction a request references, by
// (providerId, referenceExternalTransactionId), and whether it was reversed.
func referenceState(ctx context.Context, txs TransactionReader, provider domain.ProviderID, reference domain.ExternalTransactionID) (domain.ReferenceState, error) {
	if reference.IsZero() {
		return domain.ReferenceState{}, nil
	}
	target, err := txs.ByExternalID(ctx, provider, reference)
	if err != nil || target == nil {
		return domain.ReferenceState{}, err
	}
	reversed, err := txs.IsReversed(ctx, target.ID())
	if err != nil {
		return domain.ReferenceState{}, err
	}
	return domain.ReferenceState{Transaction: target, Reversed: reversed}, nil
}

// persistMovement writes a balance change: the version-checked wallet update
// first, then the ledger entry the database verifies against it.
func persistMovement(ctx context.Context, tx Tx, wallet *domain.Wallet, entry *domain.LedgerEntry, previousVersion int64) error {
	if entry == nil {
		return nil
	}
	if err := tx.Wallets().UpdateBalance(ctx, wallet, previousVersion); err != nil {
		return err
	}
	return tx.Ledger().Append(ctx, *entry)
}

func causationID(origin Origin) string {
	if origin.Message == nil {
		return ""
	}
	return origin.Message.MessageID
}

func (s *Service) observeSubmit(ctx context.Context, req domain.TransactionRequest, origin Origin, result SubmitResult, err error) {
	channel := string(origin.Channel)
	switch {
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		s.metrics.IdempotencyConflicts.WithLabelValues("idempotency_key_reused").Inc()
	case errors.Is(err, domain.ErrExternalTransactionConflict):
		s.metrics.IdempotencyConflicts.WithLabelValues("external_transaction_conflict").Inc()
	case errors.Is(err, ErrMessageConflict):
		s.metrics.IdempotencyConflicts.WithLabelValues("message_id_reused").Inc()
	}
	if err != nil {
		s.logger.WarnContext(ctx, "operation not settled", observability.KeyWalletID, req.WalletID().String(), "error", err)
		return
	}
	t := result.Transaction
	attrs := []any{
		observability.KeyTransactionID, t.ID().String(),
		observability.KeyWalletID, t.WalletID().String(),
		"kind", t.Kind(),
		"status", t.Status(),
	}
	if t.FailureCode() != "" {
		attrs = append(attrs, "failureCode", t.FailureCode())
	}
	if result.Replayed {
		s.metrics.IdempotentReplays.WithLabelValues(channel).Inc()
		s.logger.InfoContext(ctx, "duplicate answered with the stored outcome", attrs...)
		return
	}
	s.metrics.TransactionResults.WithLabelValues(channel, string(t.Kind()), string(t.Status())).Inc()
	level := slog.LevelInfo
	if t.Status() == domain.StatusFailed {
		level = slog.LevelError
	}
	s.logger.Log(ctx, level, "operation settled", attrs...)
}
