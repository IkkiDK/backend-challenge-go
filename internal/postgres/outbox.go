package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/outbox"
)

type outboxWriter struct {
	q querier
}

// Append stores events in the current unit of work; they become claimable at
// once, but only after the transaction commits.
func (w outboxWriter) Append(ctx context.Context, records []app.OutboxRecord) error {
	batch := &pgx.Batch{}
	for _, r := range records {
		batch.Queue(`
			INSERT INTO outbox_events
				(id, aggregate_type, aggregate_id, ordering_key, event_type, event_version, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
			r.EventID, r.AggregateType, r.AggregateID, r.OrderingKey, r.EventType, r.EventVersion, r.Payload, r.OccurredAt)
	}
	return dbError(w.q.SendBatch(ctx, batch).Close())
}

// OutboxStore implements outbox.Store.
type OutboxStore struct {
	store *Store
}

// Outbox returns the outbox persistence of the relay.
func (s *Store) Outbox() OutboxStore {
	return OutboxStore{store: s}
}

// Claim leases due events with FOR UPDATE SKIP LOCKED, so concurrent relays
// never claim the same event; the NOT EXISTS keeps only the oldest
// unpublished event of each ordering key. The lease is committed at once, so
// no lock is held while publishing.
func (o OutboxStore) Claim(ctx context.Context, owner string, limit int, now, leaseUntil time.Time) ([]outbox.Event, error) {
	rows, err := o.store.pool.Query(ctx, `
		WITH due AS (
			SELECT candidate.id
			FROM outbox_events candidate
			WHERE candidate.published_at IS NULL
			  AND candidate.next_attempt_at <= $1
			  AND NOT EXISTS (
				SELECT 1 FROM outbox_events earlier
				WHERE earlier.ordering_key = candidate.ordering_key
				  AND earlier.published_at IS NULL
				  AND earlier.sequence < candidate.sequence)
			ORDER BY candidate.sequence
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events e
		SET attempts = e.attempts + 1, next_attempt_at = $3, claimed_by = $4
		FROM due
		WHERE e.id = due.id
		RETURNING e.id, e.ordering_key, e.event_type, e.event_version, e.aggregate_id, e.payload, e.attempts`,
		now, limit, leaseUntil, owner)
	if err != nil {
		return nil, dbError(err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Event, error) {
		var e outbox.Event
		err := row.Scan(&e.ID, &e.OrderingKey, &e.EventType, &e.EventVersion, &e.AggregateID, &e.Payload, &e.Attempts)
		return e, err
	})
	return events, dbError(err)
}

// MarkPublished and Reschedule only touch events the relay still holds: the
// claim's owner fences a relay whose lease expired, so it can neither shorten
// the lease of the relay that took an event over nor confirm on its behalf.
// A relay claims one batch at a time, so its owner name identifies its claim.
func (o OutboxStore) MarkPublished(ctx context.Context, ids []uuid.UUID, owner string, at time.Time) (int64, error) {
	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = id.String()
	}
	tag, err := o.store.pool.Exec(ctx, `
		UPDATE outbox_events SET published_at = $3, last_error = NULL
		WHERE id = ANY($1::uuid[]) AND claimed_by = $2 AND published_at IS NULL`, keys, owner, at)
	return tag.RowsAffected(), dbError(err)
}

func (o OutboxStore) Reschedule(ctx context.Context, id uuid.UUID, owner string, next time.Time, reason string) error {
	_, err := o.store.pool.Exec(ctx, `
		UPDATE outbox_events SET next_attempt_at = $3, last_error = $4
		WHERE id = $1 AND claimed_by = $2 AND published_at IS NULL`, id, owner, next, reason)
	return dbError(err)
}

func (o OutboxStore) Backlog(ctx context.Context, now time.Time) (int64, time.Duration, error) {
	var (
		pending  int64
		oldestAt *time.Time
	)
	err := o.store.pool.QueryRow(ctx, `
		SELECT COUNT(*), MIN(occurred_at) FROM outbox_events WHERE published_at IS NULL`).Scan(&pending, &oldestAt)
	if err != nil || oldestAt == nil {
		return pending, 0, dbError(err)
	}
	return pending, now.Sub(*oldestAt), nil
}
