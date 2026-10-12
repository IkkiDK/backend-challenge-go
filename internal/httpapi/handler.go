// Package httpapi exposes the wallet use cases over HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/auth"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

const (
	maxBodyBytes       = 64 << 10
	defaultLedgerLimit = 50
	maxLedgerLimit     = 200
	headerIdempotency  = "Idempotency-Key"
)

// Dependencies are what the HTTP handlers need.
type Dependencies struct {
	Service        *app.Service
	Verifier       *auth.Verifier
	Readiness      *Readiness
	Logger         *slog.Logger
	Metrics        *observability.Metrics
	RequestTimeout time.Duration
}

type handlers struct {
	service *app.Service
	logger  *slog.Logger
}

// NewHandler routes every endpoint. Health and metrics are public; every
// business endpoint requires a bearer token with the listed scopes.
func NewHandler(deps Dependencies) http.Handler {
	h := handlers{service: deps.Service, logger: deps.Logger}
	authorized := func(scopes ...string) func(http.HandlerFunc) http.Handler {
		return authenticate(deps.Verifier, deps.Logger, scopes...)
	}

	mux := http.NewServeMux()
	mux.Handle("POST /wallets", authorized(auth.ScopeWalletsWrite)(h.openWallet))
	mux.Handle("GET /wallets/{walletId}", authorized(auth.ScopeWalletsRead)(h.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", authorized(auth.ScopeWalletsRead)(h.getLedger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", authorized(auth.ScopeWalletsReconcile)(h.reconcile))
	mux.Handle("POST /wagering/transactions", authorized(auth.ScopeWageringWrite)(h.submitTransaction))
	mux.Handle("GET /wagering/transactions/{transactionId}",
		authorized(auth.ScopeWageringRead, auth.ScopeWageringReadAll)(h.getTransaction))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		authorized(auth.ScopeWageringRead, auth.ScopeWageringReadAll)(h.getProviderTransaction))
	mux.HandleFunc("GET /health/live", live)
	mux.Handle("GET /health/ready", deps.Readiness)
	mux.Handle("GET /metrics", promhttp.HandlerFor(deps.Metrics.Registry, promhttp.HandlerOpts{}))
	return observe(mux, deps.Logger, deps.Metrics, deps.RequestTimeout)
}

func (h handlers) openWallet(w http.ResponseWriter, r *http.Request) {
	var body openWalletRequest
	if !decodeBody(w, r, &body) {
		return
	}
	req, err := domain.NewWalletOpeningRequest(domain.WalletOpeningInput{
		PlayerID: body.PlayerID, Amount: body.InitialBalance.Amount, Currency: body.InitialBalance.Currency,
	})
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	wallet, err := h.service.OpenWallet(r.Context(), req, correlationID(r.Context()))
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+wallet.ID().String())
	writeJSON(w, http.StatusCreated, newWalletResponse(wallet))
}

func (h handlers) getWallet(w http.ResponseWriter, r *http.Request) {
	walletID, ok := walletIDFromPath(w, r)
	if !ok {
		return
	}
	wallet, err := h.service.Wallet(r.Context(), walletID)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newWalletResponse(wallet))
}

func (h handlers) getLedger(w http.ResponseWriter, r *http.Request) {
	walletID, ok := walletIDFromPath(w, r)
	if !ok {
		return
	}
	query := r.URL.Query()
	afterVersion, err := decodeCursor(query.Get("cursor"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, codeValidationFailed, "invalid query parameters",
			fieldProblem{Field: "cursor", Code: string(domain.ProblemInvalidFormat), Message: "must be a cursor returned by a previous page"})
		return
	}
	limit := defaultLedgerLimit
	if raw := query.Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxLedgerLimit {
			writeProblem(w, r, http.StatusBadRequest, codeValidationFailed, "invalid query parameters",
				fieldProblem{Field: "limit", Code: string(domain.ProblemInvalidFormat), Message: fmt.Sprintf("must be an integer from 1 to %d", maxLedgerLimit)})
			return
		}
	}
	page, err := h.service.Ledger(r.Context(), walletID, afterVersion, limit)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newLedgerPageResponse(walletID, page))
}

func (h handlers) reconcile(w http.ResponseWriter, r *http.Request) {
	walletID, ok := walletIDFromPath(w, r)
	if !ok {
		return
	}
	result, err := h.service.Reconcile(r.Context(), walletID)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newReconciliationResponse(result))
}

// submitTransaction records a provider operation. The provider comes from
// the token: a body naming another provider is refused before anything runs.
func (h handlers) submitTransaction(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r.Context())
	if principal.ProviderID.IsZero() {
		writeProblem(w, r, http.StatusForbidden, codeForbidden, "the access token is not bound to a provider")
		return
	}
	var body submitTransactionRequest
	if !decodeBody(w, r, &body) {
		return
	}
	// A missing providerId is a validation error below; another provider's is forbidden.
	if body.ProviderID != "" && body.ProviderID != principal.ProviderID.String() {
		writeProblem(w, r, http.StatusForbidden, codeForbidden, "providerId must be the provider of the access token")
		return
	}
	req, err := domain.NewTransactionRequest(domain.TransactionRequestInput{
		ProviderID:                     body.ProviderID,
		ExternalTransactionID:          body.ExternalTransactionID,
		IdempotencyKey:                 r.Header.Get(headerIdempotency),
		PlayerID:                       body.PlayerID,
		WalletID:                       body.WalletID,
		RoundID:                        body.RoundID,
		GameID:                         body.GameID,
		Kind:                           body.Kind,
		Amount:                         body.Money.Amount,
		Currency:                       body.Money.Currency,
		ReferenceExternalTransactionID: body.ReferenceExternalTransactionID,
	})
	if err != nil {
		writeError(w, r, h.logger, renameIdempotencyField(err))
		return
	}
	result, err := h.service.Submit(r.Context(), req, app.Origin{
		Channel:       app.ChannelHTTP,
		CorrelationID: correlationID(r.Context()),
	})
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	status := submitStatus(result)
	if status != http.StatusOK {
		w.Header().Set("Location", "/wagering/transactions/"+result.Transaction.ID().String())
	}
	writeJSON(w, status, newSubmitResponse(result))
}

// submitStatus makes every outcome distinguishable: 201 settled now, 200
// replay of a settled operation, 202 waiting for a reference, 422 rejected by
// a business rule, 500 recorded as a permanent processing failure.
func submitStatus(result app.SubmitResult) int {
	switch result.Transaction.Status() {
	case domain.StatusProcessed:
		if result.Replayed {
			return http.StatusOK
		}
		return http.StatusCreated
	case domain.StatusRejected:
		return http.StatusUnprocessableEntity
	case domain.StatusFailed:
		return http.StatusInternalServerError
	default:
		return http.StatusAccepted
	}
}

func (h handlers) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := domain.ParseTransactionID(r.PathValue("transactionId"))
	if err != nil {
		writeError(w, r, h.logger, &domain.ValidationError{Problems: []domain.FieldProblem{asProblem(err)}})
		return
	}
	scope, ok := readScope(w, r)
	if !ok {
		return
	}
	t, err := h.service.Transaction(r.Context(), id, scope)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(t))
}

func (h handlers) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	provider, providerErr := domain.ParseProviderID(r.PathValue("providerId"))
	externalID, externalErr := domain.ParseExternalTransactionID(r.PathValue("externalTransactionId"))
	if err := errors.Join(providerErr, externalErr); err != nil {
		var problems []domain.FieldProblem
		for _, e := range []error{providerErr, externalErr} {
			if e != nil {
				problems = append(problems, asProblem(e))
			}
		}
		writeError(w, r, h.logger, &domain.ValidationError{Problems: problems})
		return
	}
	principal := principalFrom(r.Context())
	if !principal.HasScope(auth.ScopeWageringReadAll) && principal.ProviderID != provider {
		writeProblem(w, r, http.StatusForbidden, codeForbidden, "providers may only read their own transactions")
		return
	}
	t, err := h.service.TransactionByExternalID(r.Context(), provider, externalID)
	if err != nil {
		writeError(w, r, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(t))
}

// readScope returns whose transactions the caller may read: every provider's
// with wagering:read-all, otherwise only its own.
func readScope(w http.ResponseWriter, r *http.Request) (app.ReadScope, bool) {
	principal := principalFrom(r.Context())
	switch {
	case principal.HasScope(auth.ScopeWageringReadAll):
		return app.AllProviders(), true
	case !principal.ProviderID.IsZero():
		return app.OnlyProvider(principal.ProviderID), true
	default:
		writeProblem(w, r, http.StatusForbidden, codeForbidden, "the access token is not bound to a provider")
		return app.ReadScope{}, false
	}
}

func walletIDFromPath(w http.ResponseWriter, r *http.Request) (domain.WalletID, bool) {
	id, err := domain.ParseWalletID(r.PathValue("walletId"))
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, codeValidationFailed, "invalid path parameter", fieldProblemOf(asProblem(err)))
		return domain.WalletID{}, false
	}
	return id, true
}

// decodeBody accepts exactly one JSON object with known fields only, so a
// misspelled field is reported instead of silently ignored.
func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeProblem(w, r, http.StatusUnsupportedMediaType, codeUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeProblem(w, r, http.StatusBadRequest, codeMalformedRequest, "malformed JSON body: "+err.Error())
		return false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		writeProblem(w, r, http.StatusBadRequest, codeMalformedRequest, "the body must contain a single JSON object")
		return false
	}
	return true
}

// renameIdempotencyField reports the key under its HTTP header name.
func renameIdempotencyField(err error) error {
	var validation *domain.ValidationError
	if errors.As(err, &validation) {
		for i := range validation.Problems {
			if validation.Problems[i].Field == "idempotencyKey" {
				validation.Problems[i].Field = headerIdempotency
			}
		}
	}
	return err
}

func asProblem(err error) domain.FieldProblem {
	var p domain.FieldProblem
	if errors.As(err, &p) {
		return p
	}
	return domain.FieldProblem{Field: "request", Code: domain.ProblemInvalidFormat, Message: err.Error()}
}

func fieldProblemOf(p domain.FieldProblem) fieldProblem {
	return fieldProblem{Field: p.Field, Code: string(p.Code), Message: p.Message}
}
