package domain

import (
	"encoding/json"
	"maps"
	"reflect"
	"testing"
	"time"
)

// Fixed identities of the wallet history whose published events are pinned
// below. Event ids are the only random values.
const (
	contractOpeningID  = "0192f2a0-6c1e-7a3b-9d4e-1f2a3b4c5d01"
	contractBetID      = "0192f2a0-6c1e-7a3b-9d4e-1f2a3b4c5d02"
	contractWinID      = "0192f2a0-6c1e-7a3b-9d4e-1f2a3b4c5d03"
	contractRejectedID = "0192f2a0-6c1e-7a3b-9d4e-1f2a3b4c5d04"
	contractPendingID  = "0192f2a0-6c1e-7a3b-9d4e-1f2a3b4c5d05"
)

// contractClock is the fixed time of every event, with the microsecond
// precision of the production clock.
var contractClock = time.Date(2026, 10, 11, 12, 0, 0, 547226000, time.UTC)

const (
	contractOccurredAt = "2026-10-11T12:00:00.547226Z"
	// contractNextAttemptAt is contractClock plus the first delay of testPolicy.
	contractNextAttemptAt = "2026-10-11T12:00:01.547226Z"
)

// TestPublishedEventsKeepTheirContract marshals every event of a wallet
// history exactly as the outbox stores and publishes it, and compares each
// envelope, key by key and value by value, with the documented contract.
// Renaming or dropping a key, or publishing money as a number, breaks it.
func TestPublishedEventsKeepTheirContract(t *testing.T) {
	// The opening is recorded by a clock in another zone; its events must still
	// be published in UTC.
	otherZone := time.FixedZone("UTC-3", int((-3 * time.Hour).Seconds()))
	opening, err := OpenWallet(OpenWalletParams{
		ID:                   mustParse(t, ParseWalletID, testWalletID),
		PlayerID:             mustParse(t, ParsePlayerID, testPlayerID),
		InitialBalance:       mustMoney(t, "100.00", "BRL"),
		OpeningTransactionID: mustParse(t, ParseTransactionID, contractOpeningID),
		CorrelationID:        "corr-open",
		Now:                  contractClock.In(otherZone),
	})
	if err != nil {
		t.Fatal(err)
	}
	w := opening.Wallet
	openingEvents := append(w.PullEvents(), opening.Opening.PullEvents()...)

	bet := contractSettle(t, w, contractBetID, betInput("bet-1", "10.00"), ReferenceState{})
	betEvents := append(w.PullEvents(), bet.PullEvents()...)

	win := contractSettle(t, w, contractWinID, operationInput(KindWin, "win-1", "30.00", "bet-1"), ReferenceState{Transaction: bet})
	winEvents := append(w.PullEvents(), win.PullEvents()...)

	rejected := contractSettle(t, w, contractRejectedID, operationInput(KindRefund, "refund-1", "10.00", "bet-1"),
		ReferenceState{Transaction: bet, Reversed: true})
	rejectedEvents := append(w.PullEvents(), rejected.PullEvents()...)

	pending := contractSettle(t, w, contractPendingID, operationInput(KindRefund, "refund-2", "5.00", "bet-2"), ReferenceState{})
	pendingEvents := append(w.PullEvents(), pending.PullEvents()...)

	fromProvider := func(externalID string) map[string]any {
		return map[string]any{"providerId": testProvider, "externalTransactionId": externalID, "roundId": testRound, "gameId": testGame}
	}
	cases := []struct {
		name          string
		events        []Event
		correlationID string
		causationID   string
		// want holds the envelopes in emission order, without their random eventId.
		want []map[string]any
	}{
		{
			name: "wallet opening", events: openingEvents, correlationID: "corr-open",
			want: []map[string]any{
				{
					"eventType": "WalletBalanceChanged", "aggregateType": "Wallet", "aggregateId": testWalletID,
					"correlationId": "corr-open", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": map[string]any{
						"walletId": testWalletID, "transactionId": contractOpeningID, "direction": "CREDIT",
						"money": brlJSON("100.00"), "balanceBefore": brlJSON("0.00"), "balanceAfter": brlJSON("100.00"),
						"walletVersion": 1.0,
					},
				},
				{
					"eventType": "WagerTransactionProcessed", "aggregateType": "WagerTransaction", "aggregateId": contractOpeningID,
					"correlationId": "corr-open", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": map[string]any{
						"transactionId": contractOpeningID, "walletId": testWalletID, "playerId": testPlayerID,
						"kind": "OPENING", "money": brlJSON("100.00"), "balance": brlJSON("100.00"),
					},
				},
			},
		},
		{
			name: "processed BET consumed from a message", events: betEvents, correlationID: "corr-bet-1", causationID: "msg-bet-1",
			want: []map[string]any{
				{
					"eventType": "WalletBalanceChanged", "aggregateType": "Wallet", "aggregateId": testWalletID,
					"correlationId": "corr-bet-1", "causationId": "msg-bet-1", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": map[string]any{
						"walletId": testWalletID, "transactionId": contractBetID, "direction": "DEBIT",
						"money": brlJSON("10.00"), "balanceBefore": brlJSON("100.00"), "balanceAfter": brlJSON("90.00"),
						"walletVersion": 2.0,
					},
				},
				{
					"eventType": "WagerTransactionProcessed", "aggregateType": "WagerTransaction", "aggregateId": contractBetID,
					"correlationId": "corr-bet-1", "causationId": "msg-bet-1", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": with(fromProvider("bet-1"), map[string]any{
						"transactionId": contractBetID, "walletId": testWalletID, "playerId": testPlayerID,
						"kind": "BET", "money": brlJSON("10.00"), "balance": brlJSON("90.00"),
					}),
				},
			},
		},
		{
			name: "processed WIN of that BET", events: winEvents, correlationID: "corr-win-1",
			want: []map[string]any{
				{
					"eventType": "WalletBalanceChanged", "aggregateType": "Wallet", "aggregateId": testWalletID,
					"correlationId": "corr-win-1", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": map[string]any{
						"walletId": testWalletID, "transactionId": contractWinID, "direction": "CREDIT",
						"money": brlJSON("30.00"), "balanceBefore": brlJSON("90.00"), "balanceAfter": brlJSON("120.00"),
						"walletVersion": 3.0,
					},
				},
				{
					"eventType": "WagerTransactionProcessed", "aggregateType": "WagerTransaction", "aggregateId": contractWinID,
					"correlationId": "corr-win-1", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": with(fromProvider("win-1"), map[string]any{
						"transactionId": contractWinID, "walletId": testWalletID, "playerId": testPlayerID,
						"kind": "WIN", "money": brlJSON("30.00"), "balance": brlJSON("120.00"),
						"referenceTransactionId": contractBetID,
					}),
				},
			},
		},
		{
			name: "rejected REFUND of an already reversed BET", events: rejectedEvents, correlationID: "corr-refund-1",
			want: []map[string]any{
				{
					"eventType": "WagerTransactionRejected", "aggregateType": "WagerTransaction", "aggregateId": contractRejectedID,
					"correlationId": "corr-refund-1", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": with(fromProvider("refund-1"), map[string]any{
						"transactionId": contractRejectedID, "walletId": testWalletID, "playerId": testPlayerID,
						"kind": "REFUND", "money": brlJSON("10.00"), "balance": brlJSON("120.00"),
						"failureCode": "REFERENCE_ALREADY_REVERSED", "failureClass": "DEFINITIVE",
						"referenceExternalTransactionId": "bet-1",
					}),
				},
			},
		},
		{
			name: "REFUND waiting for its reference", events: pendingEvents, correlationID: "corr-refund-2",
			want: []map[string]any{
				{
					"eventType": "WagerTransactionPendingReference", "aggregateType": "WagerTransaction", "aggregateId": contractPendingID,
					"correlationId": "corr-refund-2", "occurredAt": contractOccurredAt, "version": 1.0,
					"data": with(fromProvider("refund-2"), map[string]any{
						"transactionId": contractPendingID, "walletId": testWalletID, "playerId": testPlayerID,
						"kind": "REFUND", "money": brlJSON("5.00"),
						"referenceExternalTransactionId": "bet-2", "nextAttemptAt": contractNextAttemptAt,
					}),
				},
			},
		},
	}

	emitted, eventIDs := 0, make(map[string]bool)
	for _, c := range cases {
		for _, e := range c.events {
			emitted++
			eventIDs[e.EventID().String()] = true
		}
	}
	if len(eventIDs) != emitted {
		t.Errorf("%d distinct event ids for %d events: consumers deduplicate by eventId", len(eventIDs), emitted)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if len(c.events) != len(c.want) {
				t.Fatalf("emitted %v, want %d events", eventTypes(c.events), len(c.want))
			}
			for i, e := range c.events {
				// The outbox stores exactly these bytes and the relay publishes them unchanged.
				payload, err := json.Marshal(NewEnvelope(e, c.correlationID, c.causationID))
				if err != nil {
					t.Fatal(err)
				}
				var got map[string]any
				if err := json.Unmarshal(payload, &got); err != nil {
					t.Fatal(err)
				}
				want := with(c.want[i], map[string]any{"eventId": e.EventID().String()})
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s envelope differs from the contract:\n got %s\nwant %s", e.EventType(), indented(t, got), indented(t, want))
				}
			}
		})
	}
}

// contractSettle settles a transaction with a fixed id at contractClock and
// requires the domain to accept the inputs.
func contractSettle(t *testing.T, w *Wallet, id string, in TransactionRequestInput, ref ReferenceState) *WagerTransaction {
	t.Helper()
	tx, err := NewExternalTransaction(mustParse(t, ParseTransactionID, id), mustRequest(t, in), "corr-"+in.ExternalTransactionID, contractClock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Settle(w, tx, ref, testPolicy, contractClock); err != nil {
		t.Fatal(err)
	}
	return tx
}

func mustParse[T any](t *testing.T, parse func(string) (T, error), value string) T {
	t.Helper()
	parsed, err := parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// brlJSON is the published form of a BRL amount: the amount is a string.
func brlJSON(amount string) map[string]any {
	return map[string]any{"amount": amount, "currency": "BRL"}
}

// with returns the union of two JSON objects.
func with(base, extra map[string]any) map[string]any {
	union := maps.Clone(base)
	maps.Copy(union, extra)
	return union
}

func indented(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
