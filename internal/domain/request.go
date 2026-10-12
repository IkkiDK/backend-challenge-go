package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// TransactionRequestInput carries the raw fields of an external wagering
// operation as received over HTTP or SQS.
type TransactionRequestInput struct {
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              string
	WalletID              string
	RoundID               string
	GameID                string
	Kind                  string
	Amount                string
	Currency              string
	// ReferenceExternalTransactionID is optional; "" means no reference.
	ReferenceExternalTransactionID string
}

// TransactionRequest is a validated external wagering operation. HTTP and SQS
// both build it with NewTransactionRequest, so the two channels share the same
// validation, normalization and idempotency fingerprint.
type TransactionRequest struct {
	providerID          ProviderID
	externalID          ExternalTransactionID
	idempotencyKey      IdempotencyKey
	playerID            PlayerID
	walletID            WalletID
	roundID             RoundID
	gameID              GameID
	kind                Kind
	money               money.Money
	referenceExternalID ExternalTransactionID
	fingerprint         string
}

// NewTransactionRequest validates in and reports every invalid field at once
// in a *ValidationError.
func NewTransactionRequest(in TransactionRequestInput) (TransactionRequest, error) {
	var (
		problems problemList
		req      TransactionRequest
		err      error
	)
	req.providerID, err = ParseProviderID(in.ProviderID)
	problems.add(err)
	req.externalID, err = ParseExternalTransactionID(in.ExternalTransactionID)
	problems.add(err)
	req.idempotencyKey, err = ParseIdempotencyKey(in.IdempotencyKey)
	problems.add(err)
	req.playerID, err = ParsePlayerID(in.PlayerID)
	problems.add(err)
	req.walletID, err = ParseWalletID(in.WalletID)
	problems.add(err)
	req.roundID, err = ParseRoundID(in.RoundID)
	problems.add(err)
	req.gameID, err = ParseGameID(in.GameID)
	problems.add(err)
	if in.ReferenceExternalTransactionID != "" {
		req.referenceExternalID, err = parseExternalTransactionID("referenceExternalTransactionId", in.ReferenceExternalTransactionID)
		problems.add(err)
	}

	kind, kindErr := parseExternalKind(in.Kind)
	problems.add(kindErr)
	req.kind = kind
	amount, moneyErr := parseRequestMoney("money", in.Amount, in.Currency)
	problems.add(moneyErr)
	req.money = amount

	if kindErr == nil {
		if moneyErr == nil {
			problems.add(checkAmountForKind(kind, amount))
		}
		problems.add(checkReferenceForKind(kind, in.ReferenceExternalTransactionID, in.ExternalTransactionID))
	}

	if len(problems) > 0 {
		return TransactionRequest{}, &ValidationError{Problems: problems}
	}
	req.fingerprint = fingerprint(req)
	return req, nil
}

type problemList []FieldProblem

func (l *problemList) add(err error) {
	if err == nil {
		return
	}
	var p FieldProblem
	if !errors.As(err, &p) {
		p = FieldProblem{Field: "request", Code: ProblemInvalidFormat, Message: err.Error()}
	}
	*l = append(*l, p)
}

// parseRequestMoney parses a non-negative {amount, currency} object reported
// under field (e.g. "money.amount").
func parseRequestMoney(field, amount, currency string) (money.Money, error) {
	var (
		amountField   = field + ".amount"
		currencyField = field + ".currency"
	)
	switch {
	case currency == "":
		return money.Money{}, problem(currencyField, ProblemRequired, "is required")
	case amount == "":
		return money.Money{}, problem(amountField, ProblemRequired, "is required")
	}
	m, err := money.Parse(amount, currency)
	switch {
	case errors.Is(err, money.ErrUnsupportedCurrency):
		return money.Money{}, problem(currencyField, ProblemUnsupportedCurrency, "must be a supported upper-case ISO 4217 code with two decimal places")
	case errors.Is(err, money.ErrOverflow):
		return money.Money{}, problem(amountField, ProblemAmountOutOfRange, "exceeds the representable range")
	case err != nil:
		return money.Money{}, problem(amountField, ProblemInvalidAmount, `must be a decimal string with exactly two fraction digits, such as "25.00"`)
	case m.IsNegative():
		return money.Money{}, problem(amountField, ProblemNegativeAmount, "must not be negative")
	}
	return m, nil
}

// WalletOpeningInput carries the raw fields of a wallet opening request.
type WalletOpeningInput struct {
	PlayerID string
	Amount   string
	Currency string
}

// WalletOpeningRequest is a validated wallet opening: a player and a
// non-negative initial balance, whose currency becomes the wallet's.
type WalletOpeningRequest struct {
	playerID       PlayerID
	initialBalance money.Money
}

// NewWalletOpeningRequest validates in and reports every invalid field at once
// in a *ValidationError.
func NewWalletOpeningRequest(in WalletOpeningInput) (WalletOpeningRequest, error) {
	var (
		problems problemList
		req      WalletOpeningRequest
		err      error
	)
	req.playerID, err = ParsePlayerID(in.PlayerID)
	problems.add(err)
	req.initialBalance, err = parseRequestMoney("initialBalance", in.Amount, in.Currency)
	problems.add(err)
	if len(problems) > 0 {
		return WalletOpeningRequest{}, &ValidationError{Problems: problems}
	}
	return req, nil
}

// PlayerID returns the owner of the wallet to open.
func (r WalletOpeningRequest) PlayerID() PlayerID { return r.playerID }

// InitialBalance returns the opening balance; zero is allowed.
func (r WalletOpeningRequest) InitialBalance() money.Money { return r.initialBalance }

// checkAmountForKind applies the zero-value policy: LOSS moves no money and
// must be exactly 0.00, every other external kind needs a positive amount.
func checkAmountForKind(kind Kind, amount money.Money) error {
	const field = "money.amount"
	switch {
	case kind == KindLoss && !amount.IsZero():
		return problem(field, ProblemAmountMustBeZero, "must be 0.00 for LOSS")
	case kind != KindLoss && !amount.IsPositive():
		return problem(field, ProblemAmountMustBePositive, "must be greater than zero for %s", kind)
	}
	return nil
}

func checkReferenceForKind(kind Kind, reference, externalID string) error {
	const field = "referenceExternalTransactionId"
	switch {
	case reference == "" && kind.requiresReference():
		return problem(field, ProblemReferenceRequired, "is required for %s", kind)
	case reference != "" && !kind.acceptsReference():
		return problem(field, ProblemReferenceNotAllowed, "is not accepted for %s", kind)
	case reference != "" && reference == externalID:
		return problem(field, ProblemSelfReference, "must differ from externalTransactionId")
	}
	return nil
}

// fingerprintV1 is the canonical document hashed into the payload fingerprint.
// Fields are declared in lexicographic key order, so encoding/json emits sorted
// keys, and every value is already canonical: lower-case UUIDs, upper-case
// currency and a two-decimal amount. The idempotency key and transport metadata
// are deliberately excluded.
type fingerprintV1 struct {
	ExternalTransactionID          string      `json:"externalTransactionId"`
	GameID                         string      `json:"gameId"`
	Kind                           Kind        `json:"kind"`
	Money                          money.Money `json:"money"`
	PlayerID                       string      `json:"playerId"`
	ProviderID                     string      `json:"providerId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	RoundID                        string      `json:"roundId"`
	WalletID                       string      `json:"walletId"`
}

// fingerprint returns the hex SHA-256 of the canonical JSON (no whitespace, no
// HTML escaping) of the request's business fields.
func fingerprint(r TransactionRequest) string {
	var canonical bytes.Buffer
	encoder := json.NewEncoder(&canonical)
	encoder.SetEscapeHTML(false)
	// Every field is a string or a validated Money, so encoding cannot fail.
	_ = encoder.Encode(fingerprintV1{
		ExternalTransactionID:          r.externalID.String(),
		GameID:                         r.gameID.String(),
		Kind:                           r.kind,
		Money:                          r.money,
		PlayerID:                       r.playerID.String(),
		ProviderID:                     r.providerID.String(),
		ReferenceExternalTransactionID: r.referenceExternalID.String(),
		RoundID:                        r.roundID.String(),
		WalletID:                       r.walletID.String(),
	})
	sum := sha256.Sum256(bytes.TrimSuffix(canonical.Bytes(), []byte("\n")))
	return hex.EncodeToString(sum[:])
}

// ProviderID returns the provider submitting the operation.
func (r TransactionRequest) ProviderID() ProviderID { return r.providerID }

// ExternalTransactionID returns the provider's identifier of the operation.
func (r TransactionRequest) ExternalTransactionID() ExternalTransactionID { return r.externalID }

// IdempotencyKey returns the key supplied with the request.
func (r TransactionRequest) IdempotencyKey() IdempotencyKey { return r.idempotencyKey }

// PlayerID returns the player the operation belongs to.
func (r TransactionRequest) PlayerID() PlayerID { return r.playerID }

// WalletID returns the wallet the operation moves.
func (r TransactionRequest) WalletID() WalletID { return r.walletID }

// RoundID returns the game round of the operation.
func (r TransactionRequest) RoundID() RoundID { return r.roundID }

// GameID returns the game of the operation.
func (r TransactionRequest) GameID() GameID { return r.gameID }

// Kind returns the operation kind.
func (r TransactionRequest) Kind() Kind { return r.kind }

// Money returns the operation amount.
func (r TransactionRequest) Money() money.Money { return r.money }

// ReferenceExternalTransactionID returns the referenced provider transaction, or
// the zero value when the request has none.
func (r TransactionRequest) ReferenceExternalTransactionID() ExternalTransactionID {
	return r.referenceExternalID
}

// Fingerprint returns the hex SHA-256 identifying the request's business content.
func (r TransactionRequest) Fingerprint() string { return r.fingerprint }
