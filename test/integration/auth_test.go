//go:build integration

package integration

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// forgeProvider rewrites the provider_id claim of a token without
// re-signing it, as an attacker holding provider-a's token would.
func forgeProvider(t *testing.T, token, from, to string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("not a JWT: %q", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	forged := strings.Replace(string(payload), `"provider_id":"`+from+`"`, `"provider_id":"`+to+`"`, 1)
	if forged == string(payload) {
		t.Fatal("the token carries no provider_id claim to forge")
	}
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(forged))
	return strings.Join(parts, ".")
}

func TestInvalidCredentialsAreRejectedWithoutEffects(t *testing.T) {
	inst := startInstance(t, "authentication", nil)
	w := openWallet(t, inst.baseURL, "100.00")
	shortLived := accessToken(t, "provider-a-short-lived")

	// The 3-second token works while it is fresh...
	fresh := newOperation(w, "BET", "fresh-token-"+w.ID, "10.00")
	if status, _ := submit(t, inst.baseURL, shortLived, fresh); status != http.StatusCreated {
		t.Fatalf("fresh short-lived token: %d, want 201", status)
	}

	cases := map[string]string{
		"missing token":    "",
		"garbage token":    "not-a-jwt",
		"forged claim":     forgeProvider(t, accessToken(t, "provider-b"), "provider-b", "provider-a"),
		"foreign audience": accessToken(t, "foreign-service"),
	}
	time.Sleep(4 * time.Second)
	// ...and is refused once expired.
	cases["expired token"] = shortLived

	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			op := newOperation(w, "BET", "rejected-"+strings.ReplaceAll(name, " ", "-")+"-"+w.ID, "10.00")
			resp := call(t, http.MethodPost, inst.baseURL+"/wagering/transactions", token, map[string]string{"Idempotency-Key": op.key()}, op)
			if resp.status != http.StatusUnauthorized || !strings.Contains(string(resp.body), "UNAUTHENTICATED") {
				t.Fatalf("status %d %s, want 401 UNAUTHENTICATED", resp.status, resp.body)
			}
			if status, _ := transactionStatus(t, "provider-a", op.ExternalTransactionID); status != "" {
				t.Fatalf("a refused request was recorded as %s", status)
			}
		})
	}
	requireLedgerMatchesBalance(t, w.ID, 9000)
}

func TestProvidersOnlyReachTheirOwnTransactionsAndWalletsStayInternal(t *testing.T) {
	inst := startInstance(t, "authorization", nil)
	var (
		backoffice = accessToken(t, "wallet-backoffice")
		tokenA     = accessToken(t, "provider-a")
		tokenB     = accessToken(t, "provider-b")
		w          = openWallet(t, inst.baseURL, "100.00")
		op         = newOperation(w, "BET", "owned-"+w.ID, "10.00")
	)
	status, ownA := submit(t, inst.baseURL, tokenA, op)
	if status != http.StatusCreated {
		t.Fatalf("provider-a bet: %d", status)
	}

	t.Run("a provider cannot act for another", func(t *testing.T) {
		if status, _ := submit(t, inst.baseURL, tokenB, op); status != http.StatusForbidden {
			t.Fatalf("provider-b submitting as provider-a: %d, want 403", status)
		}
		if status, _ := submit(t, inst.baseURL, backoffice, op); status != http.StatusForbidden {
			t.Fatalf("an internal client submitting operations: %d, want 403", status)
		}
	})

	t.Run("idempotency keys never cross providers", func(t *testing.T) {
		opB := op
		opB.ProviderID = "provider-b"
		resp := call(t, http.MethodPost, inst.baseURL+"/wagering/transactions", tokenB, map[string]string{"Idempotency-Key": op.key()}, opB)
		var ownB submitJSON
		resp.decode(t, &ownB)
		if resp.status != http.StatusCreated || ownB.IdempotentReplay || ownB.TransactionID == ownA.TransactionID {
			t.Fatalf("provider-b reusing provider-a's key text: %d %+v, want its own new transaction", resp.status, ownB)
		}
	})

	t.Run("reads are scoped to the provider", func(t *testing.T) {
		byID := inst.baseURL + "/wagering/transactions/" + ownA.TransactionID
		if s := call(t, http.MethodGet, byID, tokenB, nil, nil).status; s != http.StatusNotFound {
			t.Fatalf("provider-b reading provider-a's transaction by id: %d, want 404", s)
		}
		for _, token := range []string{tokenA, backoffice} {
			if s := call(t, http.MethodGet, byID, token, nil, nil).status; s != http.StatusOK {
				t.Fatalf("owner or back office reading by id: %d, want 200", s)
			}
		}
		byExternal := inst.baseURL + "/providers/provider-a/wagering/transactions/" + op.ExternalTransactionID
		if s := call(t, http.MethodGet, byExternal, tokenB, nil, nil).status; s != http.StatusForbidden {
			t.Fatalf("provider-b reading provider-a's namespace: %d, want 403", s)
		}
		if s := call(t, http.MethodGet, byExternal, tokenA, nil, nil).status; s != http.StatusOK {
			t.Fatalf("provider-a reading its own namespace: %d, want 200", s)
		}
	})

	t.Run("wallet operations are internal", func(t *testing.T) {
		requests := map[string]func(token string) int{
			"open": func(token string) int {
				return call(t, http.MethodPost, inst.baseURL+"/wallets", token, nil, map[string]any{"playerId": w.PlayerID, "initialBalance": brl("1.00")}).status
			},
			"read": func(token string) int {
				return call(t, http.MethodGet, inst.baseURL+"/wallets/"+w.ID, token, nil, nil).status
			},
			"ledger": func(token string) int {
				return call(t, http.MethodGet, inst.baseURL+"/wallets/"+w.ID+"/ledger", token, nil, nil).status
			},
			"reconcile": func(token string) int {
				return call(t, http.MethodPost, inst.baseURL+"/wallets/"+w.ID+"/reconciliation", token, nil, nil).status
			},
		}
		for name, request := range requests {
			if status := request(tokenA); status != http.StatusForbidden {
				t.Errorf("provider token on %s: %d, want 403", name, status)
			}
		}
		if status := requests["read"](backoffice); status != http.StatusOK {
			t.Errorf("back office reading a wallet: %d, want 200", status)
		}
	})

	requireLedgerMatchesBalance(t, w.ID, 8000)
}

// A provider that knows a walletId but names a player who does not own it is
// refused without learning the wallet's balance, then or on any later read.
func TestAPlayerMismatchDoesNotRevealTheWalletBalance(t *testing.T) {
	inst := startInstance(t, "player-mismatch", nil)
	w := openWallet(t, inst.baseURL, "100.00")
	token := accessToken(t, "provider-a")
	op := newOperation(w, "LOSS", "stranger-"+w.ID, "0.00")
	op.PlayerID = uuid.NewString()

	for _, attempt := range []string{"first answer", "replay"} {
		resp := call(t, http.MethodPost, inst.baseURL+"/wagering/transactions", token, map[string]string{"Idempotency-Key": op.key()}, op)
		if resp.status != http.StatusUnprocessableEntity || !strings.Contains(string(resp.body), "WALLET_PLAYER_MISMATCH") {
			t.Fatalf("%s: %d %s, want 422 WALLET_PLAYER_MISMATCH", attempt, resp.status, resp.body)
		}
		if strings.Contains(string(resp.body), `"balance"`) {
			t.Fatalf("%s reveals the wallet balance: %s", attempt, resp.body)
		}
	}
	read := call(t, http.MethodGet, inst.baseURL+"/providers/provider-a/wagering/transactions/"+op.ExternalTransactionID, token, nil, nil)
	if read.status != http.StatusOK || strings.Contains(string(read.body), `"balance"`) {
		t.Fatalf("read: %d %s, want 200 without the balance", read.status, read.body)
	}
	if status, failure := transactionStatus(t, "provider-a", op.ExternalTransactionID); status != "REJECTED" || failure != "WALLET_PLAYER_MISMATCH" {
		t.Fatalf("stored outcome %s %s, want the audited rejection", status, failure)
	}
}
