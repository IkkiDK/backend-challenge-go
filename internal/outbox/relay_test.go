package outbox

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

const testOwner = "relay-under-test"

var testNow = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

type reschedule struct {
	id     uuid.UUID
	owner  string
	next   time.Time
	reason string
}

// fakeStore hands out one batch and records what the relay writes back.
type fakeStore struct {
	mu          sync.Mutex
	batch       []Event
	marks       [][]uuid.UUID
	reschedules []reschedule
}

func (s *fakeStore) Claim(context.Context, string, int, time.Time, time.Time) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	batch := s.batch
	s.batch = nil
	return batch, nil
}

func (s *fakeStore) MarkPublished(_ context.Context, ids []uuid.UUID, owner string, _ time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if owner != testOwner {
		return 0, errors.New("marked under another owner")
	}
	s.marks = append(s.marks, slices.Clone(ids))
	return int64(len(ids)), nil
}

func (s *fakeStore) Reschedule(_ context.Context, id uuid.UUID, owner string, next time.Time, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reschedules = append(s.reschedules, reschedule{id: id, owner: owner, next: next, reason: reason})
	return nil
}

func (s *fakeStore) Backlog(context.Context, time.Time) (int64, time.Duration, error) {
	return 0, 0, nil
}

// publisherFunc adapts a function to Publisher.
type publisherFunc func(ctx context.Context, e Event) error

func (f publisherFunc) Publish(ctx context.Context, e Event) error { return f(ctx, e) }

func batchOf(n int) []Event {
	events := make([]Event, n)
	for i := range events {
		events[i] = Event{ID: uuid.New(), OrderingKey: uuid.NewString(), EventType: "WalletBalanceChanged", Attempts: 1}
	}
	return events
}

func newTestRelay(t *testing.T, store Store, publisher Publisher, batchSize int) (*Relay, *observability.Metrics) {
	t.Helper()
	metrics := observability.NewMetrics()
	relay, err := NewRelay(store, publisher, Config{
		Owner: testOwner, BatchSize: batchSize, Lease: 2 * time.Second, PublishTimeout: time.Second,
		RetryBaseDelay: time.Second, RetryMaxDelay: time.Minute,
	}, func() time.Time { return testNow }, slog.New(slog.DiscardHandler), metrics)
	if err != nil {
		t.Fatal(err)
	}
	return relay, metrics
}

func ids(events []Event) []uuid.UUID {
	out := make([]uuid.UUID, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func sameIDs(a, b []uuid.UUID) bool {
	sorted := func(ids []uuid.UUID) []string {
		out := make([]string, len(ids))
		for i, id := range ids {
			out[i] = id.String()
		}
		slices.Sort(out)
		return out
	}
	return slices.Equal(sorted(a), sorted(b))
}

func TestABatchIsPublishedConcurrentlyAndConfirmedInOneWrite(t *testing.T) {
	events := batchOf(5)
	store := &fakeStore{batch: events}
	// Every publication waits until the whole batch is in flight at once,
	// which publishing one event after another never reaches.
	var arrived atomic.Int32
	allInFlight := make(chan struct{})
	barrier := publisherFunc(func(ctx context.Context, _ Event) error {
		if arrived.Add(1) == int32(len(events)) {
			close(allInFlight)
		}
		select {
		case <-allInFlight:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	relay, metrics := newTestRelay(t, store, barrier, len(events))

	busy, err := relay.PublishBatch(t.Context())
	if err != nil || !busy {
		t.Fatalf("PublishBatch = %v, %v", busy, err)
	}
	if len(store.marks) != 1 || !sameIDs(store.marks[0], ids(events)) {
		t.Fatalf("confirmations = %v, want the whole batch in one write", store.marks)
	}
	if len(store.reschedules) != 0 {
		t.Fatalf("rescheduled %v, want none", store.reschedules)
	}
	if published := testutil.ToFloat64(metrics.OutboxPublished); published != float64(len(events)) {
		t.Fatalf("published metric = %v, want %d", published, len(events))
	}
}

func TestAFailedPublicationIsRescheduledByItsOwnerWithBackoff(t *testing.T) {
	events := batchOf(3)
	failing := events[1]
	store := &fakeStore{batch: events}
	publisher := publisherFunc(func(_ context.Context, e Event) error {
		if e.ID == failing.ID {
			return errors.New("broker unavailable")
		}
		return nil
	})
	relay, _ := newTestRelay(t, store, publisher, len(events))

	if _, err := relay.PublishBatch(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(store.marks) != 1 || !sameIDs(store.marks[0], []uuid.UUID{events[0].ID, events[2].ID}) {
		t.Fatalf("confirmations = %v, want only the published events", store.marks)
	}
	want := reschedule{id: failing.ID, owner: testOwner, next: testNow.Add(time.Second), reason: "broker unavailable"}
	if len(store.reschedules) != 1 || store.reschedules[0] != want {
		t.Fatalf("reschedules = %+v, want %+v", store.reschedules, want)
	}
}

func TestStoppingMidBatchReleasesTheUnpublishedEvents(t *testing.T) {
	events := batchOf(2)
	store := &fakeStore{batch: events}
	ctx, cancel := context.WithCancel(t.Context())
	var started atomic.Int32
	publisher := publisherFunc(func(ctx context.Context, _ Event) error {
		if started.Add(1) == int32(len(events)) {
			cancel()
		}
		<-ctx.Done()
		return ctx.Err()
	})
	relay, _ := newTestRelay(t, store, publisher, len(events))

	if _, err := relay.PublishBatch(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishBatch error = %v, want the cancellation", err)
	}
	if len(store.marks) != 0 || len(store.reschedules) != len(events) {
		t.Fatalf("marks %v, reschedules %+v: want every event released", store.marks, store.reschedules)
	}
	for _, r := range store.reschedules {
		if r.owner != testOwner || !r.next.Equal(testNow) || r.reason != "released on shutdown" {
			t.Fatalf("release = %+v, want immediate reclaiming under the owner", r)
		}
	}
}
