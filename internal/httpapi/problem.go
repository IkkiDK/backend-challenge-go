package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

// Stable codes of error responses. Business rejections are not errors: they
// are transactions with a failureCode (see domain.FailureCode).
const (
	codeMalformedRequest            = "MALFORMED_REQUEST"
	codeUnsupportedMediaType        = "UNSUPPORTED_MEDIA_TYPE"
	codeValidationFailed            = "VALIDATION_FAILED"
	codeUnauthenticated             = "UNAUTHENTICATED"
	codeForbidden                   = "FORBIDDEN"
	codeWalletNotFound              = "WALLET_NOT_FOUND"
	codeTransactionNotFound         = "TRANSACTION_NOT_FOUND"
	codeWalletAlreadyExists         = "WALLET_ALREADY_EXISTS"
	codeIdempotencyKeyReused        = "IDEMPOTENCY_KEY_REUSED"
	codeExternalTransactionConflict = "EXTERNAL_TRANSACTION_CONFLICT"
	codeServiceUnavailable          = "SERVICE_UNAVAILABLE"
	codeInternalError               = "INTERNAL_ERROR"
)

// retryAfterSeconds is suggested to clients after a transient failure.
const retryAfterSeconds = "1"

// problem is an RFC 9457 problem details document with a machine-readable code.
type problem struct {
	Title         string         `json:"title"`
	Status        int            `json:"status"`
	Code          string         `json:"code"`
	Detail        string         `json:"detail,omitempty"`
	CorrelationID string         `json:"correlationId,omitempty"`
	Errors        []fieldProblem `json:"errors,omitempty"`
}

type fieldProblem struct {
	Field   string `json:"field"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string, fields ...fieldProblem) {
	w.Header().Set("Content-Type", "application/problem+json")
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", retryAfterSeconds)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Title:         http.StatusText(status),
		Status:        status,
		Code:          code,
		Detail:        detail,
		CorrelationID: correlationID(r.Context()),
		Errors:        fields,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError maps application errors to problem responses. Transient failures
// become 503 with Retry-After: the request can be repeated with the same
// idempotency key.
func writeError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	var validation *domain.ValidationError
	switch {
	case errors.As(err, &validation):
		writeProblem(w, r, http.StatusBadRequest, codeValidationFailed, "the request has invalid fields", fieldProblems(validation)...)
	case errors.Is(err, app.ErrWalletNotFound):
		writeProblem(w, r, http.StatusNotFound, codeWalletNotFound, err.Error())
	case errors.Is(err, app.ErrTransactionNotFound):
		writeProblem(w, r, http.StatusNotFound, codeTransactionNotFound, err.Error())
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeProblem(w, r, http.StatusConflict, codeWalletAlreadyExists, app.ErrWalletAlreadyExists.Error())
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		writeProblem(w, r, http.StatusConflict, codeIdempotencyKeyReused,
			"this Idempotency-Key was already used for a request with different content")
	case errors.Is(err, domain.ErrExternalTransactionConflict):
		writeProblem(w, r, http.StatusConflict, codeExternalTransactionConflict,
			"this provider transaction was already recorded under another Idempotency-Key")
	case errors.Is(err, app.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
		logger.WarnContext(r.Context(), "request failed transiently", "error", err)
		writeProblem(w, r, http.StatusServiceUnavailable, codeServiceUnavailable, "a dependency is temporarily unavailable; retry later")
	case errors.Is(err, context.Canceled):
		// The client went away; nobody reads the response.
		logger.InfoContext(r.Context(), "request cancelled by the client")
	default:
		logger.ErrorContext(r.Context(), "request failed", "error", err)
		writeProblem(w, r, http.StatusInternalServerError, codeInternalError, "unexpected error")
	}
}

func fieldProblems(validation *domain.ValidationError) []fieldProblem {
	fields := make([]fieldProblem, len(validation.Problems))
	for i, p := range validation.Problems {
		fields[i] = fieldProblem{Field: p.Field, Code: string(p.Code), Message: p.Message}
	}
	return fields
}
