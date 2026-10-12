package domain

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidValue reports an uninitialized or malformed domain value.
	ErrInvalidValue = errors.New("domain: invalid value")
	// ErrInvalidTransition reports a state change the transaction state machine forbids.
	ErrInvalidTransition = errors.New("domain: invalid state transition")
	// ErrInsufficientFunds reports a debit larger than the wallet balance.
	ErrInsufficientFunds = errors.New("domain: insufficient funds")
	// ErrBalanceLimitExceeded reports a credit that would overflow the balance.
	ErrBalanceLimitExceeded = errors.New("domain: balance limit exceeded")
	// ErrCurrencyMismatch reports a movement in a currency other than the wallet's.
	ErrCurrencyMismatch = errors.New("domain: currency mismatch")

	// ErrIdempotencyKeyReused reports an idempotency key already recorded for a
	// different payload.
	ErrIdempotencyKeyReused = errors.New("domain: idempotency key reused with a different payload")
	// ErrExternalTransactionConflict reports a (providerId, externalTransactionId)
	// pair already recorded under another idempotency key.
	ErrExternalTransactionConflict = errors.New("domain: external transaction already recorded under another idempotency key")
)

// ProblemCode is the stable, machine-readable reason a request field is invalid.
type ProblemCode string

// Problem codes of invalid request fields. Requests failing these checks are
// never persisted, so the caller may correct them and retry with the same key.
const (
	ProblemRequired             ProblemCode = "REQUIRED"
	ProblemInvalidFormat        ProblemCode = "INVALID_FORMAT"
	ProblemTooLong              ProblemCode = "TOO_LONG"
	ProblemInvalidAmount        ProblemCode = "INVALID_AMOUNT"
	ProblemUnsupportedCurrency  ProblemCode = "UNSUPPORTED_CURRENCY"
	ProblemAmountOutOfRange     ProblemCode = "AMOUNT_OUT_OF_RANGE"
	ProblemNegativeAmount       ProblemCode = "NEGATIVE_AMOUNT"
	ProblemAmountMustBePositive ProblemCode = "AMOUNT_MUST_BE_POSITIVE"
	ProblemAmountMustBeZero     ProblemCode = "AMOUNT_MUST_BE_ZERO"
	ProblemUnsupportedKind      ProblemCode = "UNSUPPORTED_KIND"
	ProblemReferenceRequired    ProblemCode = "REFERENCE_REQUIRED"
	ProblemReferenceNotAllowed  ProblemCode = "REFERENCE_NOT_ALLOWED"
	ProblemSelfReference        ProblemCode = "SELF_REFERENCE"
)

// FieldProblem describes one invalid field of an external request.
type FieldProblem struct {
	Field   string
	Code    ProblemCode
	Message string
}

func (p FieldProblem) Error() string {
	return p.Field + ": " + p.Message
}

// Unwrap lets errors.Is(err, ErrInvalidValue) match every field problem.
func (p FieldProblem) Unwrap() error {
	return ErrInvalidValue
}

func problem(field string, code ProblemCode, format string, args ...any) FieldProblem {
	return FieldProblem{Field: field, Code: code, Message: fmt.Sprintf(format, args...)}
}

// ValidationError lists every problem found in an external request.
type ValidationError struct {
	Problems []FieldProblem
}

func (e *ValidationError) Error() string {
	messages := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		messages[i] = p.Error()
	}
	return "invalid request: " + strings.Join(messages, "; ")
}

// Unwrap lets errors.Is(err, ErrInvalidValue) match validation errors.
func (e *ValidationError) Unwrap() error {
	return ErrInvalidValue
}

// FailureCode is the stable reason recorded on a REJECTED or FAILED transaction.
type FailureCode string

// Failure codes of persisted, terminal transactions.
const (
	FailureWalletPlayerMismatch         FailureCode = "WALLET_PLAYER_MISMATCH"
	FailureCurrencyMismatch             FailureCode = "CURRENCY_MISMATCH"
	FailureReferenceKindNotAllowed      FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	FailureReferenceMismatch            FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceAmountMismatch      FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureInsufficientFunds            FailureCode = "INSUFFICIENT_FUNDS"
	FailureInsufficientFundsForReversal FailureCode = "INSUFFICIENT_FUNDS_FOR_REVERSAL"
	FailureBalanceLimitExceeded         FailureCode = "BALANCE_LIMIT_EXCEEDED"
	FailureReferenceNotFound            FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed        FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceAlreadyReversed     FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureProcessingFailed             FailureCode = "PROCESSING_FAILED"
)

// FailureClass tells a provider whether a failure stems from its input or is a
// definitive outcome.
type FailureClass string

const (
	// FailureClassCorrectable means the request contradicts the recorded state
	// (wrong player, currency, reference or amount); the provider can correct
	// it and submit it again as a new transaction.
	FailureClassCorrectable FailureClass = "CORRECTABLE"
	// FailureClassDefinitive means the outcome is final: resubmitting the same
	// operation cannot succeed.
	FailureClassDefinitive FailureClass = "DEFINITIVE"
)

var failureClasses = map[FailureCode]FailureClass{
	FailureWalletPlayerMismatch:         FailureClassCorrectable,
	FailureCurrencyMismatch:             FailureClassCorrectable,
	FailureReferenceKindNotAllowed:      FailureClassCorrectable,
	FailureReferenceMismatch:            FailureClassCorrectable,
	FailureReferenceAmountMismatch:      FailureClassCorrectable,
	FailureInsufficientFunds:            FailureClassDefinitive,
	FailureInsufficientFundsForReversal: FailureClassDefinitive,
	FailureBalanceLimitExceeded:         FailureClassDefinitive,
	FailureReferenceNotFound:            FailureClassDefinitive,
	FailureReferenceNotProcessed:        FailureClassDefinitive,
	FailureReferenceAlreadyReversed:     FailureClassDefinitive,
	FailureProcessingFailed:             FailureClassDefinitive,
}

// ParseFailureCode returns the failure code named s.
func ParseFailureCode(s string) (FailureCode, error) {
	code := FailureCode(s)
	if _, ok := failureClasses[code]; !ok {
		return "", fmt.Errorf("%w: unknown failure code %q", ErrInvalidValue, s)
	}
	return code, nil
}

// Class returns whether the failure is correctable by the provider or definitive.
func (c FailureCode) Class() FailureClass {
	return failureClasses[c]
}
