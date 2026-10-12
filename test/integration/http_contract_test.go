//go:build integration

package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

// contractRequestTimeout is the instance's HTTP_REQUEST_TIMEOUT, short so a
// request stuck on a paused database is answered within seconds.
const contractRequestTimeout = 2 * time.Second

// problemClient bounds requests that must fail, so a response that never
// comes fails the test instead of stalling the run.
var problemClient = &http.Client{Timeout: 3 * contractRequestTimeout}

// problemJSON is an RFC 9457 problem document as the API writes it.
type problemJSON struct {
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Code          string `json:"code"`
	CorrelationID string `json:"correlationId"`
	Errors        []struct {
		Field string `json:"field"`
		Code  string `json:"code"`
	} `json:"errors"`
}

// requireProblem sends a request that must fail with status and code, and
// checks what every problem response carries: the problem media type, the
// status repeated in the body and the correlation id echoed in the header.
func requireProblem(t *testing.T, status int, code, method, endpoint, token string, headers map[string]string, body any) (problemJSON, http.Header) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := problemClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var problem problemJSON
	if err := json.Unmarshal(raw, &problem); err != nil {
		t.Fatalf("%s %s: status %d without a problem document: %s", method, endpoint, resp.StatusCode, raw)
	}
	correlationID := resp.Header.Get("X-Correlation-ID")
	switch {
	case resp.StatusCode != status || problem.Code != code:
		t.Fatalf("%s %s: %d %s, want %d %s", method, endpoint, resp.StatusCode, raw, status, code)
	case resp.Header.Get("Content-Type") != "application/problem+json":
		t.Fatalf("Content-Type = %q, want application/problem+json", resp.Header.Get("Content-Type"))
	case problem.Status != status || problem.Title != http.StatusText(status):
		t.Fatalf("problem status %d and title %q, want %d and %q", problem.Status, problem.Title, status, http.StatusText(status))
	case correlationID == "" || problem.CorrelationID != correlationID:
		t.Fatalf("problem correlationId %q, X-Correlation-ID %q: want the same id", problem.CorrelationID, correlationID)
	}
	return problem, resp.Header
}

// requireInvalidParameter checks a VALIDATION_FAILED problem about one field.
func requireInvalidParameter(t *testing.T, problem problemJSON, field string) {
	t.Helper()
	if len(problem.Errors) != 1 || problem.Errors[0].Field != field || problem.Errors[0].Code != "INVALID_FORMAT" {
		t.Fatalf("errors = %+v, want one INVALID_FORMAT problem for %s", problem.Errors, field)
	}
}

type ledgerEntryJSON struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Amount        moneyJSON `json:"amount"`
	BalanceBefore moneyJSON `json:"balanceBefore"`
	BalanceAfter  moneyJSON `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

type ledgerPageJSON struct {
	WalletID   string            `json:"walletId"`
	Items      []ledgerEntryJSON `json:"items"`
	NextCursor string            `json:"nextCursor"`
}

// ledgerPage reads one page of the ledger of walletID with query.
func ledgerPage(t *testing.T, baseURL, token, walletID string, query url.Values) ledgerPageJSON {
	t.Helper()
	resp := call(t, http.MethodGet, baseURL+"/wallets/"+walletID+"/ledger?"+query.Encode(), token, nil, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("ledger page %v: %d %s", query, resp.status, resp.body)
	}
	var page ledgerPageJSON
	resp.decode(t, &page)
	if page.WalletID != walletID {
		t.Fatalf("ledger page of wallet %q, want %s", page.WalletID, walletID)
	}
	return page
}

func TestHTTPConflictNotFoundOutageAndLedgerPaginationContract(t *testing.T) {
	inst := startInstance(t, "http-contract-outcomes", map[string]string{"HTTP_REQUEST_TIMEOUT": contractRequestTimeout.String()})
	var (
		backoffice   = accessToken(t, "wallet-backoffice")
		provider     = accessToken(t, "provider-a")
		transactions = inst.baseURL + "/wagering/transactions"
	)

	t.Run("a second wallet for the same player and currency is a conflict", func(t *testing.T) {
		opening := map[string]any{"playerId": uuid.NewString(), "initialBalance": brl("100.00")}
		first := call(t, http.MethodPost, inst.baseURL+"/wallets", backoffice, nil, opening)
		if first.status != http.StatusCreated {
			t.Fatalf("first opening: %d %s", first.status, first.body)
		}
		var w walletJSON
		first.decode(t, &w)

		requireProblem(t, http.StatusConflict, "WALLET_ALREADY_EXISTS", http.MethodPost, inst.baseURL+"/wallets", backoffice, nil, opening)
		if n := countRows(t, `SELECT count(*) FROM wallets WHERE player_id = $1`, w.PlayerID); n != 1 {
			t.Fatalf("wallets of the player = %d, want 1", n)
		}
		if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE player_id = $1`, w.PlayerID); n != 1 {
			t.Fatalf("transactions of the player = %d, want only the first OPENING", n)
		}
		requireLedgerMatchesBalance(t, w.ID, 10000)
	})

	t.Run("idempotency conflicts are refused without a second transaction", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		op := newOperation(w, "BET", "conflict-"+w.ID, "10.00")
		if status, result := submit(t, inst.baseURL, provider, op); status != http.StatusCreated || result.Status != "PROCESSED" {
			t.Fatalf("original bet: %d %+v", status, result)
		}

		otherAmount := op
		otherAmount.Money = brl("11.00")
		requireProblem(t, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", http.MethodPost, transactions, provider,
			map[string]string{"Idempotency-Key": op.key()}, otherAmount)
		requireProblem(t, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", http.MethodPost, transactions, provider,
			map[string]string{"Idempotency-Key": "other-key-" + w.ID}, op)

		if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, w.ID); n != 1 {
			t.Fatalf("provider transactions = %d, want only the original", n)
		}
		requireLedgerMatchesBalance(t, w.ID, 9000)
	})

	t.Run("an unknown wallet is not found and nothing is recorded", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		unknown := uuid.NewString()
		for _, path := range []string{"/wallets/" + unknown, "/wallets/" + unknown + "/ledger"} {
			requireProblem(t, http.StatusNotFound, "WALLET_NOT_FOUND", http.MethodGet, inst.baseURL+path, backoffice, nil, nil)
		}

		op := newOperation(w, "BET", "unknown-wallet-"+w.ID, "10.00")
		op.WalletID = unknown
		requireProblem(t, http.StatusNotFound, "WALLET_NOT_FOUND", http.MethodPost, transactions, provider,
			map[string]string{"Idempotency-Key": op.key()}, op)
		// Nothing was recorded, so the same key and operation work once the
		// wallet is corrected, as a new transaction rather than a replay.
		op.WalletID = w.ID
		if status, result := submit(t, inst.baseURL, provider, op); status != http.StatusCreated || result.IdempotentReplay {
			t.Fatalf("corrected request with the same key: %d %+v", status, result)
		}
		requireLedgerMatchesBalance(t, w.ID, 9000)
	})

	t.Run("the ledger pages in wallet version order", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		var openingID string
		if err := env.owner.QueryRow(t.Context(), `SELECT id FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.ID).
			Scan(&openingID); err != nil {
			t.Fatal(err)
		}
		want := []ledgerEntryJSON{{
			TransactionID: openingID, Direction: "CREDIT", Amount: brl("100.00"),
			BalanceBefore: brl("0.00"), BalanceAfter: brl("100.00"), WalletVersion: 1,
		}}
		before := "100.00"
		for i, bet := range []struct{ amount, after string }{{"1.00", "99.00"}, {"2.00", "97.00"}, {"3.00", "94.00"}, {"4.00", "90.00"}} {
			status, result := submit(t, inst.baseURL, provider, newOperation(w, "BET", fmt.Sprintf("ledger-%d-%s", i, w.ID), bet.amount))
			if status != http.StatusCreated {
				t.Fatalf("bet %d: %d %+v", i, status, result)
			}
			want = append(want, ledgerEntryJSON{
				TransactionID: result.TransactionID, Direction: "DEBIT", Amount: brl(bet.amount),
				BalanceBefore: brl(before), BalanceAfter: brl(bet.after), WalletVersion: int64(len(want) + 1),
			})
			before = bet.after
		}

		// Following nextCursor returns every entry once, in version order, in
		// full pages until the last; the last page has no cursor.
		const pageSize = 2
		var (
			got    []ledgerEntryJSON
			sizes  []int
			cursor string
		)
		for {
			if len(sizes) > len(want) {
				t.Fatalf("still paging after %d pages: %v", len(sizes), sizes)
			}
			query := url.Values{"limit": {strconv.Itoa(pageSize)}}
			if cursor != "" {
				query.Set("cursor", cursor)
			}
			page := ledgerPage(t, inst.baseURL, backoffice, w.ID, query)
			got, sizes = append(got, page.Items...), append(sizes, len(page.Items))
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		if !slices.Equal(sizes, []int{2, 2, 1}) {
			t.Fatalf("page sizes %v, want [2 2 1] for %d entries", sizes, len(want))
		}
		ids := map[string]bool{}
		for i, entry := range got {
			if entry.ID == "" || ids[entry.ID] {
				t.Fatalf("entry %d has a missing or repeated id %q", i, entry.ID)
			}
			ids[entry.ID] = true
			entry.ID = ""
			if entry != want[i] {
				t.Fatalf("entry %d = %+v, want %+v", i, entry, want[i])
			}
		}

		// A page that reaches the last entry, exactly or with room to spare (the
		// default limit), has no cursor to an empty page.
		for _, query := range []url.Values{{"limit": {strconv.Itoa(len(want))}}, {}} {
			if page := ledgerPage(t, inst.baseURL, backoffice, w.ID, query); len(page.Items) != len(want) || page.NextCursor != "" {
				t.Fatalf("ledger %v: %d entries, cursor %q; want all %d entries and no cursor", query, len(page.Items), page.NextCursor, len(want))
			}
		}

		ledger := inst.baseURL + "/wallets/" + w.ID + "/ledger?"
		invalidCursors := map[string]string{
			"not base64url":         "not a cursor!",
			"padded base64":         base64.URLEncoding.EncodeToString([]byte("v1:2")),
			"unknown format":        base64.RawURLEncoding.EncodeToString([]byte("v2:2")),
			"negative position":     base64.RawURLEncoding.EncodeToString([]byte("v1:-1")),
			"position not a number": base64.RawURLEncoding.EncodeToString([]byte("v1:two")),
		}
		for name, invalid := range invalidCursors {
			t.Run("cursor "+name, func(t *testing.T) {
				problem, _ := requireProblem(t, http.StatusBadRequest, "VALIDATION_FAILED", http.MethodGet,
					ledger+url.Values{"cursor": {invalid}}.Encode(), backoffice, nil, nil)
				requireInvalidParameter(t, problem, "cursor")
			})
		}
		// Limits run from 1 to 200; anything else is refused rather than clamped.
		for _, limit := range []string{"0", "-1", "201", "1.5", "two"} {
			t.Run("limit "+limit, func(t *testing.T) {
				problem, _ := requireProblem(t, http.StatusBadRequest, "VALIDATION_FAILED", http.MethodGet,
					ledger+url.Values{"limit": {limit}}.Encode(), backoffice, nil, nil)
				requireInvalidParameter(t, problem, "limit")
			})
		}
		if page := ledgerPage(t, inst.baseURL, backoffice, w.ID, url.Values{"limit": {"1"}}); len(page.Items) != 1 || page.NextCursor == "" {
			t.Fatalf("limit 1: %d entries, cursor %q", len(page.Items), page.NextCursor)
		}
		if page := ledgerPage(t, inst.baseURL, backoffice, w.ID, url.Values{"limit": {"200"}}); len(page.Items) != len(want) {
			t.Fatalf("limit 200: %d entries, want %d", len(page.Items), len(want))
		}
	})

	// Last: it pauses the database the instance shares with every other subtest.
	t.Run("a database outage answers 503 and the same key succeeds after it", func(t *testing.T) {
		w := openWallet(t, inst.baseURL, "100.00")
		op := newOperation(w, "BET", "outage-"+w.ID, "10.00")

		resume := pause(t, env.postgres)
		_, header := requireProblem(t, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", http.MethodPost, transactions, provider,
			map[string]string{"Idempotency-Key": op.key()}, op)
		if seconds, err := strconv.Atoi(header.Get("Retry-After")); err != nil || seconds < 1 {
			t.Fatalf("Retry-After = %q, want a delay in whole seconds", header.Get("Retry-After"))
		}
		resume()

		// The client resends with the same key, as Retry-After invites it to.
		var (
			status int
			result submitJSON
		)
		eventually(t, 30*time.Second, "the resend to be answered after the outage", func() bool {
			status, result = submit(t, inst.baseURL, provider, op)
			return status != http.StatusServiceUnavailable
		})
		if status != http.StatusCreated || result.Status != "PROCESSED" || result.IdempotentReplay || result.Balance == nil || result.Balance.Amount != "90.00" {
			t.Fatalf("resend after the outage: %d %+v, want the bet processed now", status, result)
		}
		if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
			t.Fatalf("debits = %d, want exactly 1", n)
		}
		if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, w.ID); n != 1 {
			t.Fatalf("provider transactions = %d, want 1", n)
		}
		requireLedgerMatchesBalance(t, w.ID, 9000)
	})
}
