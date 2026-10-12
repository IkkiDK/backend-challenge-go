package domain

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// payloadHashLength is the length of a hex-encoded SHA-256 digest.
const payloadHashLength = 64

// WagerTransaction is one financial operation on a wallet and its processing
// state. External operations come from providers; the internal OPENING credit
// comes from wallet opening and carries no provider metadata.
//
// State machine (PENDING is never persisted):
//
//	PENDING ──► PROCESSED | REJECTED | FAILED
//	PENDING ──► PENDING_REFERENCE ──► PENDING_REFERENCE (retry scheduled)
//	                              └─► PROCESSED | REJECTED | FAILED
type WagerTransaction struct {
	id            TransactionID
	kind          Kind
	status        Status
	walletID      WalletID
	playerID      PlayerID
	money         money.Money
	external      *externalOrigin
	referenceID   TransactionID
	failureCode   FailureCode
	resultBalance money.Money
	attempts      int
	nextAttemptAt time.Time
	correlationID string
	createdAt     time.Time
	updatedAt     time.Time
	events        []Event
}

// externalOrigin holds what only provider-submitted operations have.
type externalOrigin struct {
	providerID          ProviderID
	externalID          ExternalTransactionID
	idempotencyKey      IdempotencyKey
	payloadHash         string
	roundID             RoundID
	gameID              GameID
	referenceExternalID ExternalTransactionID
}

// NewExternalTransaction starts processing req as a PENDING transaction.
func NewExternalTransaction(id TransactionID, req TransactionRequest, correlationID string, now time.Time) (*WagerTransaction, error) {
	if err := requireSet(requiredValue{"transactionId", id}, requiredValue{"creation time", now}); err != nil {
		return nil, err
	}
	if req.fingerprint == "" {
		return nil, fmt.Errorf("%w: requests must be built by NewTransactionRequest", ErrInvalidValue)
	}
	return &WagerTransaction{
		id:       id,
		kind:     req.kind,
		status:   StatusPending,
		walletID: req.walletID,
		playerID: req.playerID,
		money:    req.money,
		external: &externalOrigin{
			providerID:          req.providerID,
			externalID:          req.externalID,
			idempotencyKey:      req.idempotencyKey,
			payloadHash:         req.fingerprint,
			roundID:             req.roundID,
			gameID:              req.gameID,
			referenceExternalID: req.referenceExternalID,
		},
		correlationID: correlationID,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

func newOpeningTransaction(id TransactionID, w *Wallet, amount money.Money, correlationID string, now time.Time) (*WagerTransaction, error) {
	if err := requireSet(requiredValue{"opening transactionId", id}); err != nil {
		return nil, err
	}
	return &WagerTransaction{
		id:            id,
		kind:          KindOpening,
		status:        StatusPending,
		walletID:      w.id,
		playerID:      w.playerID,
		money:         amount,
		correlationID: correlationID,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

// CheckReplay decides whether req repeats the already recorded transaction t,
// found by req's idempotency key or by its (providerId, externalTransactionId).
// It returns nil when the stored outcome must be replayed,
// ErrIdempotencyKeyReused when the key was used for a different payload, and
// ErrExternalTransactionConflict when the operation was recorded under another
// key.
func (t *WagerTransaction) CheckReplay(req TransactionRequest) error {
	if t.external == nil || t.external.providerID != req.providerID {
		return fmt.Errorf("%w: replay check across providers or of an internal transaction", ErrInvalidValue)
	}
	switch {
	case t.external.idempotencyKey != req.idempotencyKey:
		return ErrExternalTransactionConflict
	case t.external.payloadHash != req.fingerprint:
		return ErrIdempotencyKeyReused
	}
	return nil
}

// AwaitReference parks a new transaction whose reference is not available yet
// (PENDING → PENDING_REFERENCE) until nextAttemptAt.
func (t *WagerTransaction) AwaitReference(nextAttemptAt, now time.Time) error {
	if t.status != StatusPending || t.ReferenceExternalTransactionID().IsZero() {
		return t.transitionError(StatusPendingReference)
	}
	t.status = StatusPendingReference
	t.nextAttemptAt = nextAttemptAt
	t.updatedAt = now
	t.events = append(t.events, newWagerTransactionPendingReference(t))
	return nil
}

// RetryReferenceLater records one more unsuccessful resolution attempt and
// reschedules the next (PENDING_REFERENCE → PENDING_REFERENCE).
func (t *WagerTransaction) RetryReferenceLater(nextAttemptAt, now time.Time) error {
	if t.status != StatusPendingReference {
		return t.transitionError(StatusPendingReference)
	}
	t.attempts++
	t.nextAttemptAt = nextAttemptAt
	t.updatedAt = now
	return nil
}

// Fail records a permanent infrastructure failure for audit (→ FAILED).
func (t *WagerTransaction) Fail(now time.Time) error {
	if err := t.finish(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = FailureProcessingFailed
	return nil
}

// markProcessed completes t (→ PROCESSED) with the balance returned to the
// provider and the resolved reference, if any.
func (t *WagerTransaction) markProcessed(balance money.Money, reference TransactionID, now time.Time) error {
	if err := t.finish(StatusProcessed, now); err != nil {
		return err
	}
	t.resultBalance = balance
	t.referenceID = reference
	t.events = append(t.events, newWagerTransactionProcessed(t))
	return nil
}

// reject refuses t for a business rule (→ REJECTED); balance is the unchanged
// wallet balance returned to the provider.
func (t *WagerTransaction) reject(code FailureCode, balance money.Money, now time.Time) error {
	if err := t.finish(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	t.resultBalance = balance
	t.events = append(t.events, newWagerTransactionRejected(t))
	return nil
}

func (t *WagerTransaction) finish(to Status, now time.Time) error {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return t.transitionError(to)
	}
	t.status = to
	t.nextAttemptAt = time.Time{}
	t.updatedAt = now
	return nil
}

func (t *WagerTransaction) transitionError(to Status) error {
	return fmt.Errorf("%w: transaction %s cannot go from %s to %s", ErrInvalidTransition, t.id, t.status, to)
}

// PullEvents returns the events recorded since the last call and forgets them.
func (t *WagerTransaction) PullEvents() []Event {
	events := t.events
	t.events = nil
	return events
}

// TransactionSnapshot holds the persisted state of a transaction. Provider
// fields are zero for the internal OPENING kind.
type TransactionSnapshot struct {
	ID                             TransactionID
	Kind                           Kind
	Status                         Status
	WalletID                       WalletID
	PlayerID                       PlayerID
	Money                          money.Money
	ProviderID                     ProviderID
	ExternalTransactionID          ExternalTransactionID
	IdempotencyKey                 IdempotencyKey
	PayloadHash                    string
	RoundID                        RoundID
	GameID                         GameID
	ReferenceExternalTransactionID ExternalTransactionID
	ReferenceTransactionID         TransactionID
	FailureCode                    FailureCode
	// ResultBalance is the balance returned to the provider; invalid (zero
	// value) while the outcome is unknown.
	ResultBalance money.Money
	Attempts      int
	NextAttemptAt time.Time
	CorrelationID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Snapshot returns the state of t for persistence and presentation.
func (t *WagerTransaction) Snapshot() TransactionSnapshot {
	s := TransactionSnapshot{
		ID:                     t.id,
		Kind:                   t.kind,
		Status:                 t.status,
		WalletID:               t.walletID,
		PlayerID:               t.playerID,
		Money:                  t.money,
		ReferenceTransactionID: t.referenceID,
		FailureCode:            t.failureCode,
		ResultBalance:          t.resultBalance,
		Attempts:               t.attempts,
		NextAttemptAt:          t.nextAttemptAt,
		CorrelationID:          t.correlationID,
		CreatedAt:              t.createdAt,
		UpdatedAt:              t.updatedAt,
	}
	if e := t.external; e != nil {
		s.ProviderID = e.providerID
		s.ExternalTransactionID = e.externalID
		s.IdempotencyKey = e.idempotencyKey
		s.PayloadHash = e.payloadHash
		s.RoundID = e.roundID
		s.GameID = e.gameID
		s.ReferenceExternalTransactionID = e.referenceExternalID
	}
	return s
}

// RehydrateTransaction restores a stored transaction without replaying
// transitions or emitting events. It rejects states that can never be
// persisted, such as PENDING or a terminal transaction without its outcome.
func RehydrateTransaction(s TransactionSnapshot) (*WagerTransaction, error) {
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("transaction %s: %w", s.ID, err)
	}
	t := &WagerTransaction{
		id:            s.ID,
		kind:          s.Kind,
		status:        s.Status,
		walletID:      s.WalletID,
		playerID:      s.PlayerID,
		money:         s.Money,
		referenceID:   s.ReferenceTransactionID,
		failureCode:   s.FailureCode,
		resultBalance: s.ResultBalance,
		attempts:      s.Attempts,
		nextAttemptAt: s.NextAttemptAt,
		correlationID: s.CorrelationID,
		createdAt:     s.CreatedAt,
		updatedAt:     s.UpdatedAt,
	}
	if s.Kind != KindOpening {
		t.external = &externalOrigin{
			providerID:          s.ProviderID,
			externalID:          s.ExternalTransactionID,
			idempotencyKey:      s.IdempotencyKey,
			payloadHash:         s.PayloadHash,
			roundID:             s.RoundID,
			gameID:              s.GameID,
			referenceExternalID: s.ReferenceExternalTransactionID,
		}
	}
	return t, nil
}

func (s TransactionSnapshot) validate() error {
	if err := requireSet(
		requiredValue{"id", s.ID},
		requiredValue{"walletId", s.WalletID},
		requiredValue{"playerId", s.PlayerID},
		requiredValue{"createdAt", s.CreatedAt},
		requiredValue{"updatedAt", s.UpdatedAt},
	); err != nil {
		return err
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return err
	}
	if _, err := ParseStatus(string(s.Status)); err != nil || s.Status == StatusPending {
		return fmt.Errorf("%w: status %q cannot be persisted", ErrInvalidValue, s.Status)
	}
	if !s.Money.IsValid() || (s.Kind == KindLoss) != s.Money.IsZero() || s.Money.IsNegative() {
		return fmt.Errorf("%w: amount %s is not allowed for %s", ErrInvalidValue, s.Money, s.Kind)
	}
	if s.Attempts < 0 {
		return fmt.Errorf("%w: negative attempt count", ErrInvalidValue)
	}
	if err := s.validateOrigin(); err != nil {
		return err
	}
	return s.validateOutcome()
}

func (s TransactionSnapshot) validateOrigin() error {
	hasReference := !s.ReferenceExternalTransactionID.IsZero()
	if s.Kind == KindOpening {
		if !s.ProviderID.IsZero() || !s.ExternalTransactionID.IsZero() || !s.IdempotencyKey.IsZero() ||
			s.PayloadHash != "" || !s.RoundID.IsZero() || !s.GameID.IsZero() || hasReference {
			return fmt.Errorf("%w: OPENING carries no provider metadata", ErrInvalidValue)
		}
		return nil
	}
	if err := requireSet(
		requiredValue{"providerId", s.ProviderID},
		requiredValue{"externalTransactionId", s.ExternalTransactionID},
		requiredValue{"idempotencyKey", s.IdempotencyKey},
		requiredValue{"roundId", s.RoundID},
		requiredValue{"gameId", s.GameID},
	); err != nil {
		return err
	}
	if _, err := hex.DecodeString(s.PayloadHash); err != nil || len(s.PayloadHash) != payloadHashLength {
		return fmt.Errorf("%w: payload hash must be a hex SHA-256 digest", ErrInvalidValue)
	}
	switch {
	case s.Kind.requiresReference() && !hasReference:
		return fmt.Errorf("%w: %s requires a reference", ErrInvalidValue, s.Kind)
	case hasReference && !s.Kind.acceptsReference():
		return fmt.Errorf("%w: %s cannot carry a reference", ErrInvalidValue, s.Kind)
	}
	return nil
}

func (s TransactionSnapshot) validateOutcome() error {
	hasOutcome := s.ResultBalance.IsValid()
	switch s.Status {
	case StatusPendingReference:
		if s.NextAttemptAt.IsZero() || s.ReferenceExternalTransactionID.IsZero() || hasOutcome || s.FailureCode != "" {
			return fmt.Errorf("%w: a PENDING_REFERENCE transaction needs a schedule and no outcome", ErrInvalidValue)
		}
	case StatusProcessed:
		if !hasOutcome || s.FailureCode != "" || (s.Kind.IsReversal() && s.ReferenceTransactionID.IsZero()) {
			return fmt.Errorf("%w: a PROCESSED transaction needs its result balance and resolved reference", ErrInvalidValue)
		}
	case StatusRejected:
		if !hasOutcome || s.FailureCode == "" || s.FailureCode == FailureProcessingFailed {
			return fmt.Errorf("%w: a REJECTED transaction needs its balance and a business failure code", ErrInvalidValue)
		}
	case StatusFailed:
		if s.FailureCode != FailureProcessingFailed {
			return fmt.Errorf("%w: a FAILED transaction carries %s", ErrInvalidValue, FailureProcessingFailed)
		}
	}
	if s.FailureCode != "" {
		if _, err := ParseFailureCode(string(s.FailureCode)); err != nil {
			return err
		}
	}
	if s.Status.IsTerminal() && !s.NextAttemptAt.IsZero() {
		return fmt.Errorf("%w: a terminal transaction has no retry schedule", ErrInvalidValue)
	}
	return nil
}

// ID returns the internal transaction identifier.
func (t *WagerTransaction) ID() TransactionID { return t.id }

// Kind returns the transaction kind.
func (t *WagerTransaction) Kind() Kind { return t.kind }

// Status returns the processing state.
func (t *WagerTransaction) Status() Status { return t.status }

// WalletID returns the wallet the transaction moves.
func (t *WagerTransaction) WalletID() WalletID { return t.walletID }

// PlayerID returns the player named by the transaction.
func (t *WagerTransaction) PlayerID() PlayerID { return t.playerID }

// Money returns the transaction amount.
func (t *WagerTransaction) Money() money.Money { return t.money }

// FailureCode returns why a REJECTED or FAILED transaction did not complete.
func (t *WagerTransaction) FailureCode() FailureCode { return t.failureCode }

// ResultBalance returns the balance returned to the provider; invalid while the
// outcome is unknown.
func (t *WagerTransaction) ResultBalance() money.Money { return t.resultBalance }

// CorrelationID returns the correlation identifier of the originating request.
func (t *WagerTransaction) CorrelationID() string { return t.correlationID }

// ProviderID returns the submitting provider; zero for OPENING.
func (t *WagerTransaction) ProviderID() ProviderID {
	if t.external == nil {
		return ProviderID{}
	}
	return t.external.providerID
}

// ExternalTransactionID returns the provider's identifier; zero for OPENING.
func (t *WagerTransaction) ExternalTransactionID() ExternalTransactionID {
	if t.external == nil {
		return ExternalTransactionID{}
	}
	return t.external.externalID
}

// ReferenceExternalTransactionID returns the referenced provider transaction;
// zero when there is none.
func (t *WagerTransaction) ReferenceExternalTransactionID() ExternalTransactionID {
	if t.external == nil {
		return ExternalTransactionID{}
	}
	return t.external.referenceExternalID
}

func (t *WagerTransaction) roundID() RoundID {
	if t.external == nil {
		return RoundID{}
	}
	return t.external.roundID
}
