//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

func TestHTTPErrorContract(t *testing.T) {
	inst := startInstance(t, "http-contract", nil)
	token := accessToken(t, "provider-a")
	w := openWallet(t, inst.baseURL, "100.00")
	valid := newOperation(w, "BET", "contract-"+w.ID, "10.00")

	send := func(t *testing.T, contentType, key string, body []byte) (int, map[string]any) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, inst.baseURL+"/wagering/transactions", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", contentType)
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var problem map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&problem); err != nil {
			t.Fatalf("status %d without a JSON body: %v", resp.StatusCode, err)
		}
		return resp.StatusCode, problem
	}
	marshal := func(v any) []byte {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	withoutProvider := valid
	withoutProvider.ProviderID = ""

	cases := []struct {
		name        string
		contentType string
		key         string
		body        []byte
		status      int
		code        string
		field       string
	}{
		{"missing providerId", "application/json", valid.key(), marshal(withoutProvider), http.StatusBadRequest, "VALIDATION_FAILED", "providerId"},
		{"missing idempotency key", "application/json", "", marshal(valid), http.StatusBadRequest, "VALIDATION_FAILED", "Idempotency-Key"},
		{"amount as a JSON number", "application/json", valid.key(), []byte(`{"providerId":"provider-a","money":{"amount":10.00,"currency":"BRL"}}`), http.StatusBadRequest, "MALFORMED_REQUEST", ""},
		{"unknown field", "application/json", valid.key(), []byte(`{"providerId":"provider-a","amount":"10.00"}`), http.StatusBadRequest, "MALFORMED_REQUEST", ""},
		{"two JSON documents", "application/json", valid.key(), append(marshal(valid), marshal(valid)...), http.StatusBadRequest, "MALFORMED_REQUEST", ""},
		{"wrong content type", "text/plain", valid.key(), marshal(valid), http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, problem := send(t, c.contentType, c.key, c.body)
			if status != c.status || problem["code"] != c.code {
				t.Fatalf("status %d %v, want %d %s", status, problem, c.status, c.code)
			}
			if c.field == "" {
				return
			}
			fields, _ := problem["errors"].([]any)
			for _, f := range fields {
				if f.(map[string]any)["field"] == c.field {
					return
				}
			}
			t.Fatalf("no problem reported for %s: %v", c.field, problem)
		})
	}
	// None of them was recorded, so the same key still works once corrected.
	if status, result := submit(t, inst.baseURL, token, valid); status != http.StatusCreated || result.Status != "PROCESSED" {
		t.Fatalf("corrected request: %d %+v", status, result)
	}
	requireLedgerMatchesBalance(t, w.ID, 9000)
}
