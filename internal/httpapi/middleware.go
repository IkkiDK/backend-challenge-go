package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/auth"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

const (
	headerCorrelationID     = "X-Correlation-ID"
	unmatchedRouteLabel     = "unmatched"
	bearerScheme            = "bearer"
	wwwAuthenticateRealm    = `Bearer realm="wallet"`
	wwwAuthenticateInvalid  = `Bearer realm="wallet", error="invalid_token"`
	wwwAuthenticateNoScopes = `Bearer realm="wallet", error="insufficient_scope", scope=`
)

type correlationKey struct{}
type principalKey struct{}

func correlationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

func principalFrom(ctx context.Context) auth.Principal {
	p, _ := ctx.Value(principalKey{}).(auth.Principal)
	return p
}

// observe assigns the correlation id (the client's X-Correlation-ID when it
// is safe, a new UUID otherwise), echoes it, applies the request deadline, and
// logs and measures every request. Bodies and credentials are never logged.
func observe(next http.Handler, logger *slog.Logger, metrics *observability.Metrics, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		id := r.Header.Get(headerCorrelationID)
		if !observability.IsTraceableID(id) {
			id = uuid.Must(uuid.NewV7()).String()
		}
		w.Header().Set(headerCorrelationID, id)

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		ctx = context.WithValue(ctx, correlationKey{}, id)
		ctx = observability.WithLogAttrs(ctx, slog.String(observability.KeyCorrelationID, id))
		r = r.WithContext(ctx)

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.ErrorContext(ctx, "handler panicked", "panic", recovered)
				writeProblem(recorder, r, http.StatusInternalServerError, codeInternalError, "unexpected error")
			}
			route := r.Pattern
			if route == "" {
				route = unmatchedRouteLabel
			}
			elapsed := time.Since(started)
			metrics.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(recorder.status)).Inc()
			metrics.HTTPRequestDuration.WithLabelValues(route, r.Method).Observe(elapsed.Seconds())
			logger.InfoContext(ctx, "http request",
				"method", r.Method, "route", route, "status", recorder.status, "durationMs", elapsed.Milliseconds())
		}()
		next.ServeHTTP(recorder, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// authenticate admits requests whose bearer token is valid and grants at
// least one of scopes, and stores the principal in the request context.
func authenticate(verifier *auth.Verifier, logger *slog.Logger, scopes ...string) func(http.HandlerFunc) http.Handler {
	return func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scheme, token, found := strings.Cut(r.Header.Get("Authorization"), " ")
			if !found || !strings.EqualFold(scheme, bearerScheme) || token == "" {
				w.Header().Set("WWW-Authenticate", wwwAuthenticateRealm)
				writeProblem(w, r, http.StatusUnauthorized, codeUnauthenticated, "a bearer access token is required")
				return
			}
			principal, err := verifier.Verify(r.Context(), token)
			if err != nil {
				logger.InfoContext(r.Context(), "access token refused", "error", err)
				w.Header().Set("WWW-Authenticate", wwwAuthenticateInvalid)
				writeProblem(w, r, http.StatusUnauthorized, codeUnauthenticated, "the access token is invalid or expired")
				return
			}
			ctx := observability.WithLogAttrs(r.Context(), slog.String("clientId", principal.ClientID))
			if !principal.ProviderID.IsZero() {
				ctx = observability.WithLogAttrs(ctx, slog.String(observability.KeyProviderID, principal.ProviderID.String()))
			}
			r = r.WithContext(context.WithValue(ctx, principalKey{}, principal))
			for _, scope := range scopes {
				if principal.HasScope(scope) {
					next(w, r)
					return
				}
			}
			w.Header().Set("WWW-Authenticate", wwwAuthenticateNoScopes+strconv.Quote(strings.Join(scopes, " ")))
			writeProblem(w, r, http.StatusForbidden, codeForbidden, "the access token lacks the required scope")
		})
	}
}
