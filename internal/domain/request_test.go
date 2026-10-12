package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"
)

// problemCodes returns the field → code pairs of a validation error.
func problemCodes(t *testing.T, err error) map[string]ProblemCode {
	t.Helper()
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %v, want *ValidationError", err)
	}
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatal("a validation error must match ErrInvalidValue")
	}
	codes := make(map[string]ProblemCode, len(validation.Problems))
	for _, p := range validation.Problems {
		codes[p.Field] = p.Code
	}
	return codes
}

func requireProblem(t *testing.T, in TransactionRequestInput, field string, want ProblemCode) {
	t.Helper()
	_, err := NewTransactionRequest(in)
	if got := problemCodes(t, err)[field]; got != want {
		t.Fatalf("problem for %s = %q, want %q (error: %v)", field, got, want, err)
	}
}

func TestNewTransactionRequestAcceptsTheFiveExternalKinds(t *testing.T) {
	inputs := map[string]TransactionRequestInput{
		"BET":                operationInput(KindBet, "tx-1", "25.00", ""),
		"WIN":                operationInput(KindWin, "tx-2", "50.00", ""),
		"WIN with reference": operationInput(KindWin, "tx-3", "50.00", "tx-1"),
		"LOSS":               operationInput(KindLoss, "tx-4", "0.00", ""),
		"REFUND":             operationInput(KindRefund, "tx-5", "25.00", "tx-1"),
		"ROLLBACK":           operationInput(KindRollback, "tx-6", "25.00", "tx-1"),
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			req := mustRequest(t, in)
			if string(req.Kind()) != in.Kind || req.Money().Amount() != in.Amount ||
				req.ReferenceExternalTransactionID().String() != in.ReferenceExternalTransactionID {
				t.Fatalf("request does not carry its input: %+v", req)
			}
		})
	}
}

func TestNewTransactionRequestRejectsInternalAndUnknownKinds(t *testing.T) {
	for _, kind := range []string{"OPENING", "DEPOSIT", "bet", ""} {
		t.Run(kind, func(t *testing.T) {
			in := betInput("tx-1", "25.00")
			in.Kind = kind
			want := ProblemUnsupportedKind
			if kind == "" {
				want = ProblemRequired
			}
			requireProblem(t, in, "kind", want)
		})
	}
}

func TestZeroValuePolicyPerKind(t *testing.T) {
	tests := []struct {
		kind      Kind
		amount    string
		reference string
		want      ProblemCode
	}{
		{KindBet, "0.00", "", ProblemAmountMustBePositive},
		{KindWin, "0.00", "", ProblemAmountMustBePositive},
		{KindRefund, "0.00", "tx-0", ProblemAmountMustBePositive},
		{KindRollback, "0.00", "tx-0", ProblemAmountMustBePositive},
		{KindLoss, "0.01", "", ProblemAmountMustBeZero},
		{KindLoss, "10.00", "", ProblemAmountMustBeZero},
		{KindBet, "-5.00", "", ProblemNegativeAmount},
		{KindLoss, "-0.01", "", ProblemNegativeAmount},
	}
	for _, tt := range tests {
		t.Run(string(tt.kind)+" "+tt.amount, func(t *testing.T) {
			requireProblem(t, operationInput(tt.kind, "tx-1", tt.amount, tt.reference), "money.amount", tt.want)
		})
	}
	mustRequest(t, operationInput(KindLoss, "tx-1", "0.00", ""))
}

func TestNewTransactionRequestRejectsInvalidMoney(t *testing.T) {
	tests := []struct {
		amount, currency string
		field            string
		want             ProblemCode
	}{
		{"", "BRL", "money.amount", ProblemRequired},
		{"25", "BRL", "money.amount", ProblemInvalidAmount},
		{"25.000", "BRL", "money.amount", ProblemInvalidAmount},
		{"2.5e1", "BRL", "money.amount", ProblemInvalidAmount},
		{"NaN", "BRL", "money.amount", ProblemInvalidAmount},
		{"Infinity", "BRL", "money.amount", ProblemInvalidAmount},
		{"99999999999999999999.00", "BRL", "money.amount", ProblemAmountOutOfRange},
		{"25.00", "", "money.currency", ProblemRequired},
		{"25.00", "brl", "money.currency", ProblemUnsupportedCurrency},
		{"25.00", "JPY", "money.currency", ProblemUnsupportedCurrency},
	}
	for _, tt := range tests {
		t.Run(tt.amount+" "+tt.currency, func(t *testing.T) {
			in := betInput("tx-1", tt.amount)
			in.Currency = tt.currency
			requireProblem(t, in, tt.field, tt.want)
		})
	}
}

func TestReferenceRulesPerKind(t *testing.T) {
	const field = "referenceExternalTransactionId"
	requireProblem(t, operationInput(KindRefund, "tx-1", "1.00", ""), field, ProblemReferenceRequired)
	requireProblem(t, operationInput(KindRollback, "tx-1", "1.00", ""), field, ProblemReferenceRequired)
	requireProblem(t, operationInput(KindBet, "tx-1", "1.00", "tx-0"), field, ProblemReferenceNotAllowed)
	requireProblem(t, operationInput(KindLoss, "tx-1", "0.00", "tx-0"), field, ProblemReferenceNotAllowed)
	requireProblem(t, operationInput(KindRollback, "tx-1", "1.00", "tx-1"), field, ProblemSelfReference)
}

func TestNewTransactionRequestRejectsMalformedIdentifiers(t *testing.T) {
	tests := []struct {
		name  string
		edit  func(*TransactionRequestInput)
		field string
		want  ProblemCode
	}{
		{"braced player UUID", func(in *TransactionRequestInput) { in.PlayerID = "{" + testPlayerID + "}" }, "playerId", ProblemInvalidFormat},
		{"urn wallet UUID", func(in *TransactionRequestInput) { in.WalletID = "urn:uuid:" + testWalletID }, "walletId", ProblemInvalidFormat},
		{"undashed wallet UUID", func(in *TransactionRequestInput) { in.WalletID = strings.ReplaceAll(testWalletID, "-", "") }, "walletId", ProblemInvalidFormat},
		{"nil player UUID", func(in *TransactionRequestInput) { in.PlayerID = "00000000-0000-0000-0000-000000000000" }, "playerId", ProblemInvalidFormat},
		{"missing provider", func(in *TransactionRequestInput) { in.ProviderID = "" }, "providerId", ProblemRequired},
		{"provider with spaces", func(in *TransactionRequestInput) { in.ProviderID = "provider a" }, "providerId", ProblemInvalidFormat},
		{"provider starting with a dash", func(in *TransactionRequestInput) { in.ProviderID = "-provider" }, "providerId", ProblemInvalidFormat},
		{"external id too long", func(in *TransactionRequestInput) { in.ExternalTransactionID = strings.Repeat("x", 129) }, "externalTransactionId", ProblemTooLong},
		{"round with control character", func(in *TransactionRequestInput) { in.RoundID = "round\n1" }, "roundId", ProblemInvalidFormat},
		{"non-ASCII game", func(in *TransactionRequestInput) { in.GameID = "fortune-chimp-é" }, "gameId", ProblemInvalidFormat},
		{"missing idempotency key", func(in *TransactionRequestInput) { in.IdempotencyKey = "" }, "idempotencyKey", ProblemRequired},
		{"idempotency key too long", func(in *TransactionRequestInput) { in.IdempotencyKey = strings.Repeat("k", 256) }, "idempotencyKey", ProblemTooLong},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := betInput("tx-1", "25.00")
			tt.edit(&in)
			requireProblem(t, in, tt.field, tt.want)
		})
	}
}

func TestNewTransactionRequestReportsEveryProblem(t *testing.T) {
	_, err := NewTransactionRequest(TransactionRequestInput{Kind: "BET", Amount: "1", Currency: "BRL"})
	codes := problemCodes(t, err)
	for _, field := range []string{"providerId", "externalTransactionId", "idempotencyKey", "playerId", "walletId", "roundId", "gameId", "money.amount"} {
		if _, ok := codes[field]; !ok {
			t.Errorf("missing problem for %s in %v", field, codes)
		}
	}
}

func TestFingerprintFollowsTheDocumentedCanonicalForm(t *testing.T) {
	req := mustRequest(t, betInput("transaction-123", "25.00"))
	canonical := `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",` +
		`"providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	sum := sha256.Sum256([]byte(canonical))
	if want := hex.EncodeToString(sum[:]); req.Fingerprint() != want {
		t.Fatalf("Fingerprint() = %s, want sha256 of %s = %s", req.Fingerprint(), canonical, want)
	}

	refund := mustRequest(t, operationInput(KindRefund, "transaction-124", "25.00", "transaction-<&>"))
	withReference := strings.Replace(canonical, `"kind":"BET"`, `"kind":"REFUND"`, 1)
	withReference = strings.Replace(withReference, "transaction-123", "transaction-124", 1)
	withReference = strings.Replace(withReference, `"providerId":"provider-a",`, `"providerId":"provider-a","referenceExternalTransactionId":"transaction-<&>",`, 1)
	sum = sha256.Sum256([]byte(withReference))
	if want := hex.EncodeToString(sum[:]); refund.Fingerprint() != want {
		t.Fatalf("REFUND fingerprint = %s, want sha256 of %s (no HTML escaping)", refund.Fingerprint(), withReference)
	}
}

func TestFingerprintIgnoresTheIdempotencyKeyAndNormalizesUUIDCase(t *testing.T) {
	base := betInput("tx-1", "25.00")
	variant := base
	variant.IdempotencyKey = "another-key"
	variant.PlayerID = strings.ToUpper(base.PlayerID)
	variant.WalletID = strings.ToUpper(base.WalletID)

	a, b := mustRequest(t, base), mustRequest(t, variant)
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("fingerprint must ignore the idempotency key and UUID letter case")
	}
	if b.PlayerID().String() != base.PlayerID {
		t.Fatalf("PlayerID() = %s, want lower-case %s", b.PlayerID(), base.PlayerID)
	}
}

func TestFingerprintChangesWithEveryBusinessField(t *testing.T) {
	base := operationInput(KindWin, "tx-1", "25.00", "tx-0")
	edits := map[string]func(*TransactionRequestInput){
		"providerId":            func(in *TransactionRequestInput) { in.ProviderID = "provider-b" },
		"externalTransactionId": func(in *TransactionRequestInput) { in.ExternalTransactionID = "tx-2" },
		"playerId":              func(in *TransactionRequestInput) { in.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2" },
		"walletId":              func(in *TransactionRequestInput) { in.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef38" },
		"roundId":               func(in *TransactionRequestInput) { in.RoundID = "round-988" },
		"gameId":                func(in *TransactionRequestInput) { in.GameID = "other-game" },
		"kind":                  func(in *TransactionRequestInput) { in.Kind = string(KindRollback) },
		"money.amount":          func(in *TransactionRequestInput) { in.Amount = "25.01" },
		"money.currency":        func(in *TransactionRequestInput) { in.Currency = "USD" },
		"reference":             func(in *TransactionRequestInput) { in.ReferenceExternalTransactionID = "tx-9" },
	}
	baseFingerprint := mustRequest(t, base).Fingerprint()
	seen := []string{baseFingerprint}
	for field, edit := range edits {
		in := base
		edit(&in)
		fp := mustRequest(t, in).Fingerprint()
		if slices.Contains(seen, fp) {
			t.Errorf("changing %s did not produce a new fingerprint", field)
		}
		seen = append(seen, fp)
	}
}
