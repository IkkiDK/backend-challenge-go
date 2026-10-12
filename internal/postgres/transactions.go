package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

const transactionColumns = `id, kind, status, wallet_id, player_id, currency, amount_minor,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	attempts, next_attempt_at, correlation_id, created_at, updated_at`

type transactions struct {
	q querier
}

func (r transactions) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	origin := "EXTERNAL"
	if s.Kind == domain.KindOpening {
		origin = "INTERNAL"
	}
	_, err := r.q.Exec(ctx, `
		INSERT INTO wagering_transactions (origin, `+transactionColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`,
		origin, s.ID.String(), s.Kind, s.Status, s.WalletID.String(), s.PlayerID.String(),
		s.Money.Currency().Code(), s.Money.MinorUnits(),
		nullable(s.ProviderID.String()), nullable(s.ExternalTransactionID.String()),
		nullable(s.IdempotencyKey.String()), nullable(s.PayloadHash),
		nullable(s.RoundID.String()), nullable(s.GameID.String()),
		nullable(s.ReferenceExternalTransactionID.String()), nullableTransactionID(s.ReferenceTransactionID),
		nullable(string(s.FailureCode)), nullableMinorUnits(s.ResultBalance),
		s.Attempts, nullableTime(s.NextAttemptAt), s.CorrelationID, s.CreatedAt, s.UpdatedAt)
	return dbError(err)
}

// Update writes the fields the state machine moves; the database refuses
// changes to terminal transactions and to every business field.
func (r transactions) Update(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	tag, err := r.q.Exec(ctx, `
		UPDATE wagering_transactions
		SET status = $2, reference_transaction_id = $3, failure_code = $4, result_balance_minor = $5,
			attempts = $6, next_attempt_at = $7, updated_at = $8
		WHERE id = $1`,
		s.ID.String(), s.Status, nullableTransactionID(s.ReferenceTransactionID), nullable(string(s.FailureCode)),
		nullableMinorUnits(s.ResultBalance), s.Attempts, nullableTime(s.NextAttemptAt), s.UpdatedAt)
	if err != nil {
		return dbError(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", app.ErrTransactionNotFound, s.ID)
	}
	return nil
}

func (r transactions) ByID(ctx context.Context, id domain.TransactionID) (*domain.WagerTransaction, error) {
	t, err := scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wagering_transactions WHERE id = $1`, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

func (r transactions) ByIdempotencyKey(ctx context.Context, provider domain.ProviderID, key domain.IdempotencyKey) (*domain.WagerTransaction, error) {
	return optional(scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wagering_transactions
		WHERE provider_id = $1 AND idempotency_key = $2`, provider.String(), key.String())))
}

func (r transactions) ByExternalID(ctx context.Context, provider domain.ProviderID, id domain.ExternalTransactionID) (*domain.WagerTransaction, error) {
	return optional(scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wagering_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, provider.String(), id.String())))
}

func (r transactions) IsReversed(ctx context.Context, id domain.TransactionID) (bool, error) {
	var reversed bool
	err := r.q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM wagering_transactions
			WHERE reference_transaction_id = $1 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`,
		id.String()).Scan(&reversed)
	return reversed, dbError(err)
}

// Lock uses FOR NO KEY UPDATE, like the claim below, so it never blocks the
// foreign-key checks of transactions that reference this one.
func (r transactions) Lock(ctx context.Context, id domain.TransactionID) (*domain.WagerTransaction, error) {
	t, err := scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wagering_transactions WHERE id = $1 FOR NO KEY UPDATE`, id.String()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

// ClaimDuePendingReference skips rows other workers hold, so concurrent
// workers never block each other or process the same transaction twice.
func (r transactions) ClaimDuePendingReference(ctx context.Context, now time.Time) (*domain.WagerTransaction, error) {
	return optional(scanTransaction(r.q.QueryRow(ctx, `
		SELECT `+transactionColumns+` FROM wagering_transactions
		WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= $1
		ORDER BY next_attempt_at
		LIMIT 1
		FOR NO KEY UPDATE SKIP LOCKED`, now)))
}

// optional turns "no row" into a nil transaction.
func optional(t *domain.WagerTransaction, err error) (*domain.WagerTransaction, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// scanTransaction returns pgx.ErrNoRows unwrapped so callers can decide what
// absence means.
func scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id, kind, status, walletID, playerID, currencyCode, correlationID string
		amountMinor                                                       int64
		providerID, externalID, idempotencyKey, payloadHash               *string
		roundID, gameID, referenceExternalID, referenceID, failureCode    *string
		resultBalanceMinor                                                *int64
		attempts                                                          int
		nextAttemptAt                                                     *time.Time
		createdAt, updatedAt                                              time.Time
	)
	err := row.Scan(&id, &kind, &status, &walletID, &playerID, &currencyCode, &amountMinor,
		&providerID, &externalID, &idempotencyKey, &payloadHash, &roundID, &gameID,
		&referenceExternalID, &referenceID, &failureCode, &resultBalanceMinor,
		&attempts, &nextAttemptAt, &correlationID, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err != nil {
		return nil, dbError(err)
	}

	var (
		s       = domain.TransactionSnapshot{Attempts: attempts, CorrelationID: correlationID, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC()}
		parsing = storedValueParser{}
	)
	s.ID = parse(&parsing, domain.ParseTransactionID, id)
	s.Kind = parse(&parsing, domain.ParseKind, kind)
	s.Status = parse(&parsing, domain.ParseStatus, status)
	s.WalletID = parse(&parsing, domain.ParseWalletID, walletID)
	s.PlayerID = parse(&parsing, domain.ParsePlayerID, playerID)
	s.Money, err = storedMoney(amountMinor, currencyCode)
	parsing.keep(err)
	s.ProviderID = parseOptional(&parsing, domain.ParseProviderID, providerID)
	s.ExternalTransactionID = parseOptional(&parsing, domain.ParseExternalTransactionID, externalID)
	s.IdempotencyKey = parseOptional(&parsing, domain.ParseIdempotencyKey, idempotencyKey)
	s.RoundID = parseOptional(&parsing, domain.ParseRoundID, roundID)
	s.GameID = parseOptional(&parsing, domain.ParseGameID, gameID)
	s.ReferenceExternalTransactionID = parseOptional(&parsing, domain.ParseExternalTransactionID, referenceExternalID)
	s.ReferenceTransactionID = parseOptional(&parsing, domain.ParseTransactionID, referenceID)
	s.FailureCode = parseOptional(&parsing, domain.ParseFailureCode, failureCode)
	if payloadHash != nil {
		s.PayloadHash = *payloadHash
	}
	if resultBalanceMinor != nil {
		s.ResultBalance, err = storedMoney(*resultBalanceMinor, currencyCode)
		parsing.keep(err)
	}
	if nextAttemptAt != nil {
		s.NextAttemptAt = nextAttemptAt.UTC()
	}
	if parsing.err != nil {
		return nil, fmt.Errorf("stored transaction %s: %w", id, parsing.err)
	}
	return domain.RehydrateTransaction(s)
}

// storedValueParser keeps the first error met while parsing stored columns.
type storedValueParser struct {
	err error
}

func (p *storedValueParser) keep(err error) {
	if p.err == nil {
		p.err = err
	}
}

func parse[T any](p *storedValueParser, parseFn func(string) (T, error), value string) T {
	parsed, err := parseFn(value)
	p.keep(err)
	return parsed
}

func parseOptional[T any](p *storedValueParser, parseFn func(string) (T, error), value *string) T {
	var zero T
	if value == nil {
		return zero
	}
	return parse(p, parseFn, *value)
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableTransactionID(id domain.TransactionID) *string {
	if id.IsZero() {
		return nil
	}
	return nullable(id.String())
}

func nullableMinorUnits(m money.Money) *int64 {
	if !m.IsValid() {
		return nil
	}
	minorUnits := m.MinorUnits()
	return &minorUnits
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
