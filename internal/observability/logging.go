// Package observability provides the structured logger and Prometheus metrics
// shared by every component.
package observability

import (
	"context"
	"io"
	"log/slog"
)

// Log attribute keys of the identifiers that trace an operation end to end.
const (
	KeyCorrelationID = "correlationId"
	KeyMessageID     = "messageId"
	KeyTransactionID = "transactionId"
	KeyWalletID      = "walletId"
	KeyProviderID    = "providerId"
)

// maxTraceableIDLength bounds identifiers accepted from clients and producers.
const maxTraceableIDLength = 128

// IsTraceableID reports whether an identifier received from outside (a
// correlation id, a message id) is safe to log and propagate: 1 to 128
// visible ASCII characters, so it cannot inject control characters into logs.
func IsTraceableID(id string) bool {
	if id == "" || len(id) > maxTraceableIDLength {
		return false
	}
	for i := range len(id) {
		if id[i] < '!' || id[i] > '~' {
			return false
		}
	}
	return true
}

type logAttrsKey struct{}

// WithLogAttrs returns a context whose log records also carry attrs. Entry
// points (HTTP middleware, SQS consumer, workers) attach the identifiers they
// know, so deeper code logs them without passing them around.
func WithLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	existing, _ := ctx.Value(logAttrsKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, logAttrsKey{}, merged)
}

// NewLogger returns a JSON logger that adds the context's attributes to every
// record. Callers must never log credentials, tokens or full request payloads.
func NewLogger(w io.Writer, level slog.Level, instanceID string) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		// Instances may run in different time zones; logs always use UTC.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				a.Value = slog.TimeValue(a.Value.Time().UTC())
			}
			return a
		},
	})
	return slog.New(contextHandler{Handler: handler}).With("service", "wallet", "instance", instanceID)
}

type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if attrs, ok := ctx.Value(logAttrsKey{}).([]slog.Attr); ok {
		record.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}
