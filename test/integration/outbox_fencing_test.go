//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
)

// A relay whose lease expired and passed to another relay can neither shorten
// that relay's lease nor confirm the event on its behalf.
func TestARelayThatLostItsLeaseCannotTouchTheEvent(t *testing.T) {
	p := newInProcess(t, testPolicy)
	drainOutbox(t, p)
	openWalletInProcess(t, p, "10.00")
	store, ctx, now := p.store.Outbox(), t.Context(), app.SystemClock()

	lost, err := store.Claim(ctx, "stale-relay", 1, now, now)
	if err != nil || len(lost) != 1 {
		t.Fatalf("stale claim: %v, %d events", err, len(lost))
	}
	event := lost[0].ID
	taken, err := store.Claim(ctx, "current-relay", 1, now, now.Add(time.Minute))
	if err != nil || len(taken) != 1 || taken[0].ID != event {
		t.Fatalf("the expired claim was not taken over: %v, %+v", err, taken)
	}

	if err := store.Reschedule(ctx, event, "stale-relay", now, "stale release"); err != nil {
		t.Fatal(err)
	}
	if marked, err := store.MarkPublished(ctx, []uuid.UUID{event}, "stale-relay", now); err != nil || marked != 0 {
		t.Fatalf("stale confirmation marked %d: %v", marked, err)
	}
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE id = $1 AND claimed_by = 'current-relay'
		AND next_attempt_at = $2 AND published_at IS NULL`, event, now.Add(time.Minute)); n != 1 {
		t.Fatal("the stale relay changed the current relay's claim")
	}
	if marked, err := store.MarkPublished(ctx, []uuid.UUID{event}, "current-relay", now); err != nil || marked != 1 {
		t.Fatalf("current relay confirmation marked %d: %v", marked, err)
	}
	drainOutbox(t, p)
}
