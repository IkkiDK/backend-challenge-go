package httpapi

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// moneyBody is the external money contract. Amounts are JSON strings: a JSON
// number fails to decode instead of passing through a binary float.
type moneyBody struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type openWalletRequest struct {
	PlayerID       string    `json:"playerId"`
	InitialBalance moneyBody `json:"initialBalance"`
}

type submitTransactionRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          moneyBody `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
}

type walletResponse struct {
	ID        domain.WalletID `json:"id"`
	PlayerID  domain.PlayerID `json:"playerId"`
	Balance   money.Money     `json:"balance"`
	Version   int64           `json:"version"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

func newWalletResponse(w *domain.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: w.CreatedAt(), UpdatedAt: w.UpdatedAt(),
	}
}

type ledgerEntryResponse struct {
	ID            string               `json:"id"`
	TransactionID domain.TransactionID `json:"transactionId"`
	Direction     domain.Direction     `json:"direction"`
	Amount        money.Money          `json:"amount"`
	BalanceBefore money.Money          `json:"balanceBefore"`
	BalanceAfter  money.Money          `json:"balanceAfter"`
	WalletVersion int64                `json:"walletVersion"`
	CreatedAt     time.Time            `json:"createdAt"`
}

type ledgerPageResponse struct {
	WalletID   domain.WalletID       `json:"walletId"`
	Items      []ledgerEntryResponse `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func newLedgerPageResponse(walletID domain.WalletID, page app.LedgerPage) ledgerPageResponse {
	items := make([]ledgerEntryResponse, len(page.Entries))
	for i, e := range page.Entries {
		items[i] = ledgerEntryResponse{
			ID: e.ID().String(), TransactionID: e.TransactionID(), Direction: e.Direction(), Amount: e.Amount(),
			BalanceBefore: e.BalanceBefore(), BalanceAfter: e.BalanceAfter(), WalletVersion: e.WalletVersion(),
			CreatedAt: e.CreatedAt(),
		}
	}
	response := ledgerPageResponse{WalletID: walletID, Items: items}
	if page.NextAfterVersion > 0 {
		response.NextCursor = encodeCursor(page.NextAfterVersion)
	}
	return response
}

// cursorPrefix versions the opaque cursor format.
const cursorPrefix = "v1:"

var errInvalidCursor = errors.New("invalid cursor")

// encodeCursor hides the wallet version a page ends at; clients must treat
// the cursor as opaque.
func encodeCursor(afterVersion int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(afterVersion, 10)))
}

func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, errInvalidCursor
	}
	version, found := strings.CutPrefix(string(raw), cursorPrefix)
	if !found {
		return 0, errInvalidCursor
	}
	afterVersion, err := strconv.ParseInt(version, 10, 64)
	if err != nil || afterVersion < 0 {
		return 0, errInvalidCursor
	}
	return afterVersion, nil
}

// submitResponse is the outcome of POST /wagering/transactions, identical
// for the first request and its replays except for idempotentReplay.
type submitResponse struct {
	TransactionID    domain.TransactionID `json:"transactionId"`
	Status           domain.Status        `json:"status"`
	Balance          *money.Money         `json:"balance,omitempty"`
	FailureCode      domain.FailureCode   `json:"failureCode,omitempty"`
	FailureClass     domain.FailureClass  `json:"failureClass,omitempty"`
	IdempotentReplay bool                 `json:"idempotentReplay"`
}

func newSubmitResponse(result app.SubmitResult) submitResponse {
	t := result.Transaction
	response := submitResponse{
		TransactionID:    t.ID(),
		Status:           t.Status(),
		FailureCode:      t.FailureCode(),
		IdempotentReplay: result.Replayed,
	}
	if t.FailureCode() != "" {
		response.FailureClass = t.FailureCode().Class()
	}
	if balance := t.ResultBalance(); balance.IsValid() {
		response.Balance = &balance
	}
	return response
}

// transactionResponse is the full view of a transaction. Provider fields are
// omitted for the internal OPENING transaction.
type transactionResponse struct {
	TransactionID                  domain.TransactionID         `json:"transactionId"`
	Origin                         string                       `json:"origin"`
	Kind                           domain.Kind                  `json:"kind"`
	Status                         domain.Status                `json:"status"`
	WalletID                       domain.WalletID              `json:"walletId"`
	PlayerID                       domain.PlayerID              `json:"playerId"`
	Money                          money.Money                  `json:"money"`
	ProviderID                     domain.ProviderID            `json:"providerId,omitzero"`
	ExternalTransactionID          domain.ExternalTransactionID `json:"externalTransactionId,omitzero"`
	IdempotencyKey                 string                       `json:"idempotencyKey,omitempty"`
	RoundID                        domain.RoundID               `json:"roundId,omitzero"`
	GameID                         domain.GameID                `json:"gameId,omitzero"`
	ReferenceExternalTransactionID domain.ExternalTransactionID `json:"referenceExternalTransactionId,omitzero"`
	ReferenceTransactionID         domain.TransactionID         `json:"referenceTransactionId,omitzero"`
	FailureCode                    domain.FailureCode           `json:"failureCode,omitempty"`
	FailureClass                   domain.FailureClass          `json:"failureClass,omitempty"`
	Balance                        *money.Money                 `json:"balance,omitempty"`
	Attempts                       int                          `json:"attempts"`
	NextAttemptAt                  *time.Time                   `json:"nextAttemptAt,omitempty"`
	CorrelationID                  string                       `json:"correlationId"`
	CreatedAt                      time.Time                    `json:"createdAt"`
	UpdatedAt                      time.Time                    `json:"updatedAt"`
}

func newTransactionResponse(t *domain.WagerTransaction) transactionResponse {
	s := t.Snapshot()
	response := transactionResponse{
		TransactionID:                  s.ID,
		Origin:                         "EXTERNAL",
		Kind:                           s.Kind,
		Status:                         s.Status,
		WalletID:                       s.WalletID,
		PlayerID:                       s.PlayerID,
		Money:                          s.Money,
		ProviderID:                     s.ProviderID,
		ExternalTransactionID:          s.ExternalTransactionID,
		IdempotencyKey:                 s.IdempotencyKey.String(),
		RoundID:                        s.RoundID,
		GameID:                         s.GameID,
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID,
		ReferenceTransactionID:         s.ReferenceTransactionID,
		FailureCode:                    s.FailureCode,
		Attempts:                       s.Attempts,
		CorrelationID:                  s.CorrelationID,
		CreatedAt:                      s.CreatedAt,
		UpdatedAt:                      s.UpdatedAt,
	}
	if s.Kind == domain.KindOpening {
		response.Origin = "INTERNAL"
	}
	if s.FailureCode != "" {
		response.FailureClass = s.FailureCode.Class()
	}
	if s.ResultBalance.IsValid() {
		response.Balance = &s.ResultBalance
	}
	if !s.NextAttemptAt.IsZero() {
		response.NextAttemptAt = &s.NextAttemptAt
	}
	return response
}

type reconciliationResponse struct {
	WalletID          domain.WalletID `json:"walletId"`
	StoredBalance     money.Money     `json:"storedBalance"`
	CalculatedBalance money.Money     `json:"calculatedBalance"`
	Difference        money.Money     `json:"difference"`
	Consistent        bool            `json:"consistent"`
	CheckedEntries    int64           `json:"checkedEntries"`
}

func newReconciliationResponse(r app.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID: r.WalletID, StoredBalance: r.StoredBalance, CalculatedBalance: r.CalculatedBalance,
		Difference: r.Difference, Consistent: r.Consistent, CheckedEntries: r.CheckedEntries,
	}
}
