//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
	"github.com/IkkiDK/backend-challenge-go/internal/messaging"
	"github.com/IkkiDK/backend-challenge-go/internal/outbox"
	"github.com/IkkiDK/backend-challenge-go/internal/worker"
)

func relayConfig(owner string, lease time.Duration) outbox.Config {
	return outbox.Config{
		Owner: owner, BatchSize: 10, Lease: lease, PublishTimeout: lease / 2,
		RetryBaseDelay: 100 * time.Millisecond, RetryMaxDelay: 500 * time.Millisecond,
	}
}

func connectedBroker(t *testing.T) *messaging.Broker {
	t.Helper()
	broker := messaging.NewBroker(sqsClient(t, "wallet-service"),
		config.SQS{InboundQueue: inboundQueue, DeadLetterQueue: deadLetterQueue, EventsQueue: eventsQueue})
	if err := broker.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	return broker
}

// runRelay publishes with its own pool and broker connection, like a
// separate instance, until the test ends.
func runRelay(t *testing.T, owner string) {
	t.Helper()
	p := newInProcess(t, testPolicy)
	relay, err := outbox.NewRelay(p.store.Outbox(), connectedBroker(t), relayConfig(owner, 5*time.Second), app.SystemClock, discardLogger(), p.metrics)
	if err != nil {
		t.Fatal(err)
	}
	loop := worker.NewLoop(owner, relay.PublishBatch, 50*time.Millisecond, time.Second, discardLogger())
	loop.Start()
	t.Cleanup(func() { _ = loop.Stop(context.Background()) })
}

type publishedEvent struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	Data      struct {
		WalletID      string `json:"walletId"`
		WalletVersion int64  `json:"walletVersion"`
	} `json:"data"`
}

func TestCompetingPublishersPublishEveryEventOnceInWalletOrder(t *testing.T) {
	purge(t, eventsQueue)
	p := newInProcess(t, testPolicy)
	// Committed while no relay runs: the "crash between commit and
	// publication" case. Every event must still be published.
	wallets := map[string]bool{}
	for range 5 {
		w := openWalletInProcess(t, p, "100.00")
		wallets[w.ID] = true
		for bet := range 3 {
			op := newOperation(w, "BET", fmt.Sprintf("outbox-%s-%d", w.ID, bet), "1.00")
			if _, err := p.service.Submit(t.Context(), mustRequest(t, op), httpOrigin()); err != nil {
				t.Fatal(err)
			}
		}
	}
	const eventsPerWallet = 2 + 3*2 // opening + three bets, two events each
	unpublished := func() int {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
	}

	runRelay(t, "relay-a")
	runRelay(t, "relay-b")
	eventually(t, 30*time.Second, "every outbox event to be published", func() bool { return unpublished() == 0 })

	for walletID := range wallets {
		if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND attempts = 1`, walletID); n != eventsPerWallet {
			t.Fatalf("wallet %s: %d events claimed exactly once, want %d", walletID, n, eventsPerWallet)
		}
	}
	seen := map[string]int{}
	versions := map[string][]int64{}
	for _, m := range drain(t, sqsClient(t, "events-reader"), eventsQueue, 5*time.Second) {
		var e publishedEvent
		if err := json.Unmarshal([]byte(m.body), &e); err != nil || !wallets[e.Data.WalletID] {
			continue
		}
		seen[e.EventID]++
		if e.EventType == "WalletBalanceChanged" {
			versions[e.Data.WalletID] = append(versions[e.Data.WalletID], e.Data.WalletVersion)
		}
	}
	if len(seen) != len(wallets)*eventsPerWallet {
		t.Fatalf("received %d distinct events, want %d", len(seen), len(wallets)*eventsPerWallet)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("event %s delivered %d times", id, n)
		}
	}
	for walletID, got := range versions {
		for i, version := range got {
			if version != int64(i+1) {
				t.Fatalf("wallet %s balance events out of order: %v", walletID, got)
			}
		}
	}
}

// crashAfterPublish publishes but fails to record the publication, as if
// the relay died between the broker call and the outbox update.
type crashAfterPublish struct {
	outbox.Store
}

func (crashAfterPublish) MarkPublished(context.Context, uuid.UUID, time.Time) error {
	return errors.New("simulated crash before marking the event published")
}

// recordingPublisher remembers the event ids it published.
type recordingPublisher struct {
	outbox.Publisher
	mu  sync.Mutex
	ids []uuid.UUID
}

func (r *recordingPublisher) Publish(ctx context.Context, e outbox.Event) error {
	if err := r.Publisher.Publish(ctx, e); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, e.ID)
	return nil
}

func TestEventPublishedButNotConfirmedIsRepublishedWithTheSameID(t *testing.T) {
	purge(t, eventsQueue)
	p := newInProcess(t, testPolicy)
	// Publish whatever earlier tests left behind, so only this test's events remain.
	drainOutbox(t, p)
	w := openWalletInProcess(t, p, "0.00")
	if _, err := p.service.Submit(t.Context(), mustRequest(t, newOperation(w, "WIN", "republish-"+w.ID, "5.00")), httpOrigin()); err != nil {
		t.Fatal(err)
	}

	const lease = time.Second
	broker := connectedBroker(t)
	first := &recordingPublisher{Publisher: broker}
	crashing, err := outbox.NewRelay(crashAfterPublish{p.store.Outbox()}, first, relayConfig("crashing-relay", lease), app.SystemClock, discardLogger(), p.metrics)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashing.PublishBatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(first.ids) != 1 {
		t.Fatalf("crashing relay published %d events, want the wallet's first one", len(first.ids))
	}
	republished := first.ids[0]

	// After the lease expires, another relay takes the abandoned event over.
	second := &recordingPublisher{Publisher: broker}
	recovering, err := outbox.NewRelay(p.store.Outbox(), second, relayConfig("recovering-relay", lease), app.SystemClock, discardLogger(), p.metrics)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the abandoned event to be republished", func() bool {
		if _, err := recovering.PublishBatch(t.Context()); err != nil {
			t.Fatal(err)
		}
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE id = $1 AND published_at IS NOT NULL`, republished) == 1
	})
	if second.ids[0] != republished {
		t.Fatalf("republished %s, want the same event id %s", second.ids[0], republished)
	}
	if n := countRows(t, `SELECT attempts FROM outbox_events WHERE id = $1`, republished); n != 2 {
		t.Fatalf("attempts = %d, want 2 (one per relay)", n)
	}
	// Both publications carried the same id; the FIFO queue drops the second
	// one within its deduplication window, and any consumer could drop it by id.
	copies := 0
	for _, m := range drain(t, sqsClient(t, "events-reader"), eventsQueue, 3*time.Second) {
		var e publishedEvent
		if err := json.Unmarshal([]byte(m.body), &e); err != nil || e.EventID != republished.String() {
			continue
		}
		copies++
		if attribute := m.attributes["eventId"].StringValue; attribute == nil || *attribute != e.EventID {
			t.Fatal("the eventId message attribute must match the payload")
		}
	}
	if copies < 1 {
		t.Fatal("the event never reached the queue")
	}
}

// drainOutbox publishes every pending event with a throwaway relay.
func drainOutbox(t *testing.T, p inProcess) {
	t.Helper()
	relay, err := outbox.NewRelay(p.store.Outbox(), connectedBroker(t), relayConfig("drain", 5*time.Second), app.SystemClock, discardLogger(), p.metrics)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 60*time.Second, "the outbox backlog to drain", func() bool {
		busy, err := relay.PublishBatch(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return !busy
	})
	purge(t, eventsQueue)
}
