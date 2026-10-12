// Package outbox publishes the events committed to the transactional outbox.
// Events reach the broker only from here, after the SQL transaction that
// recorded them has committed; delivery is at least once and keeps eventId.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

// Event is an outbox record claimed for publication.
type Event struct {
	ID           uuid.UUID
	OrderingKey  string
	EventType    string
	EventVersion int
	AggregateID  string
	Payload      []byte
	// Attempts counts claims, including the current one.
	Attempts   int
	OccurredAt time.Time
}

// Store is the outbox persistence the relay needs.
type Store interface {
	// Claim leases up to limit due events to owner until leaseUntil. For each
	// ordering key it only returns the oldest unpublished event, so events of
	// one wallet are published in commit order by any number of relays. An
	// event whose lease expired (its relay died) becomes claimable again.
	Claim(ctx context.Context, owner string, limit int, now, leaseUntil time.Time) ([]Event, error)
	// MarkPublished records the publication of an event.
	MarkPublished(ctx context.Context, id uuid.UUID, at time.Time) error
	// Reschedule makes an unpublished event claimable again at next.
	Reschedule(ctx context.Context, id uuid.UUID, next time.Time, reason string) error
	// Backlog returns how many events wait and the age of the oldest one.
	Backlog(ctx context.Context, now time.Time) (pending int64, oldestAge time.Duration, err error)
}

// Publisher delivers an event to the broker. Deliveries must keep the event id
// so consumers can discard duplicates.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Config tunes the relay.
type Config struct {
	// Owner identifies the relay instance in the claims it writes.
	Owner     string
	BatchSize int
	// Lease is how long a claim protects an event from other relays; it must
	// exceed PublishTimeout.
	Lease          time.Duration
	PublishTimeout time.Duration
	// RetryBaseDelay and RetryMaxDelay bound the exponential backoff between
	// failed publications. Events are retried forever: a committed event is
	// never dropped.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Validate reports settings under which leases could expire mid-publication
// or retries could not back off.
func (c Config) Validate() error {
	switch {
	case c.Owner == "" || c.BatchSize < 1:
		return fmt.Errorf("outbox relay needs an owner and a positive batch size")
	case c.PublishTimeout <= 0 || c.Lease <= c.PublishTimeout:
		return fmt.Errorf("outbox lease (%v) must exceed the publish timeout (%v)", c.Lease, c.PublishTimeout)
	case c.RetryBaseDelay <= 0 || c.RetryMaxDelay < c.RetryBaseDelay:
		return fmt.Errorf("outbox retry delays need 0 < base (%v) <= max (%v)", c.RetryBaseDelay, c.RetryMaxDelay)
	}
	return nil
}

// Relay moves events from the outbox to the broker.
type Relay struct {
	store     Store
	publisher Publisher
	cfg       Config
	clock     func() time.Time
	logger    *slog.Logger
	metrics   *observability.Metrics
}

// NewRelay validates cfg and returns a relay.
func NewRelay(store Store, publisher Publisher, cfg Config, clock func() time.Time, logger *slog.Logger, metrics *observability.Metrics) (*Relay, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Relay{store: store, publisher: publisher, cfg: cfg, clock: clock, logger: logger, metrics: metrics}, nil
}

// PublishBatch claims and publishes one batch and reports whether it found
// work. If ctx ends mid-batch, the events not yet published are released for
// another relay instead of waiting for their lease to expire.
func (r *Relay) PublishBatch(ctx context.Context) (bool, error) {
	now := r.clock()
	events, err := r.store.Claim(ctx, r.cfg.Owner, r.cfg.BatchSize, now, now.Add(r.cfg.Lease))
	if err != nil {
		return false, fmt.Errorf("claim outbox events: %w", err)
	}
	for i, e := range events {
		if ctx.Err() != nil {
			r.release(events[i:])
			return true, ctx.Err()
		}
		r.publish(ctx, e)
	}
	return len(events) > 0, nil
}

func (r *Relay) publish(ctx context.Context, e Event) {
	logCtx := observability.WithLogAttrs(ctx, slog.String("eventId", e.ID.String()), slog.String("eventType", e.EventType))
	publishCtx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	if err := r.publisher.Publish(publishCtx, e); err != nil {
		if ctx.Err() != nil {
			r.release([]Event{e})
			return
		}
		r.metrics.OutboxPublishFailures.Inc()
		r.metrics.Retries.WithLabelValues("outbox_publish").Inc()
		delay := r.retryDelay(e.Attempts)
		r.logger.WarnContext(logCtx, "outbox publication failed; will retry", "attempts", e.Attempts, "retryIn", delay.String(), "error", err)
		if err := r.store.Reschedule(context.WithoutCancel(ctx), e.ID, r.clock().Add(delay), err.Error()); err != nil {
			r.logger.ErrorContext(logCtx, "could not reschedule outbox event; its lease will expire instead", "error", err)
		}
		return
	}
	if err := r.store.MarkPublished(context.WithoutCancel(ctx), e.ID, r.clock()); err != nil {
		// Published but not confirmed: once the lease expires the event is
		// published again under the same eventId, which consumers deduplicate.
		r.logger.ErrorContext(logCtx, "event published but not marked; it will be republished", "error", err)
		return
	}
	r.metrics.OutboxPublished.Inc()
}

// release hands claimed but unpublished events back for immediate reclaiming.
func (r *Relay) release(events []Event) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.PublishTimeout)
	defer cancel()
	for _, e := range events {
		if err := r.store.Reschedule(ctx, e.ID, r.clock(), "released on shutdown"); err != nil {
			r.logger.WarnContext(ctx, "could not release outbox event; its lease will expire instead", "eventId", e.ID.String(), "error", err)
		}
	}
}

// retryDelay grows exponentially with the number of claims, up to the cap.
func (r *Relay) retryDelay(attempts int) time.Duration {
	delay := r.cfg.RetryBaseDelay
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= r.cfg.RetryMaxDelay {
			return r.cfg.RetryMaxDelay
		}
	}
	return delay
}

// RecordBacklog refreshes the outbox lag and size gauges.
func (r *Relay) RecordBacklog(ctx context.Context) error {
	pending, oldestAge, err := r.store.Backlog(ctx, r.clock())
	if err != nil {
		return fmt.Errorf("measure outbox backlog: %w", err)
	}
	r.metrics.OutboxPending.Set(float64(pending))
	r.metrics.OutboxLag.Set(oldestAge.Seconds())
	return nil
}
