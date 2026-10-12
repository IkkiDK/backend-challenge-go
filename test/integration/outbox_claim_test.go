//go:build integration

package integration

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
)

// Every relay claims several times a second, so a claim must read pending
// events only. Pending events are the newest ones, at the end of the commit
// order: a claim that walked the outbox in that order from the start would
// read its whole published history, which nothing prunes, every time. The
// history is published by UPDATE, as the relay does, and autovacuum is
// paused so the cost of walking it does not depend on when vacuum runs.
func TestClaimsDoNotReadThePublishedHistory(t *testing.T) {
	const (
		history = 200_000
		pending = 10_000
		batch   = 50
		// claimBudget is far above a claim that reads pending events only and
		// far below one that walks the published history.
		claimBudget = 25 * time.Millisecond
		attempts    = 3
	)
	p := newInProcess(t, testPolicy)
	drainOutbox(t, p)
	ctx := t.Context()
	prefix := "claim-history-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = env.owner.Exec(context.Background(),
			`UPDATE outbox_events SET published_at = now() WHERE ordering_key LIKE $1 || '%' AND published_at IS NULL`, prefix)
		_, _ = env.owner.Exec(context.Background(), `ALTER TABLE outbox_events RESET (autovacuum_enabled)`)
	})
	for _, step := range []struct {
		sql  string
		args []any
	}{
		{`ALTER TABLE outbox_events SET (autovacuum_enabled = false)`, nil},
		{`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, ordering_key, event_type, event_version, payload, occurred_at, next_attempt_at)
			SELECT gen_random_uuid(), 'Wallet', $1 || '-published-' || (g % 1000), $1 || '-published-' || (g % 1000), 'WalletBalanceChanged', 1, '{}'::json, now(), now()
			FROM generate_series(1, $2) g`, []any{prefix, history}},
		{`UPDATE outbox_events SET published_at = now() WHERE ordering_key LIKE $1 || '-published-%'`, []any{prefix}},
		// One pending event per wallet, committed after the whole history.
		{`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, ordering_key, event_type, event_version, payload, occurred_at, next_attempt_at)
			SELECT gen_random_uuid(), 'Wallet', $1 || '-' || g, $1 || '-' || g, 'WalletBalanceChanged', 1, '{}'::json, now(), now()
			FROM generate_series(1, $2) g`, []any{prefix, pending}},
		{`ANALYZE outbox_events`, nil},
	} {
		if _, err := env.owner.Exec(ctx, step.sql, step.args...); err != nil {
			t.Fatal(err)
		}
	}
	var oldest []string
	rows, err := env.owner.Query(ctx, `SELECT id::text FROM outbox_events
		WHERE ordering_key LIKE $1 || '%' AND published_at IS NULL ORDER BY sequence LIMIT $2`, prefix, batch)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		oldest = append(oldest, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(oldest)

	store, now := p.store.Outbox(), app.SystemClock()
	fastest := time.Hour
	for range attempts {
		started := time.Now()
		// A lease that ends at once keeps the events claimable by the next attempt.
		claimed, err := store.Claim(ctx, "history-test", batch, now, now)
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, len(claimed))
		for i, e := range claimed {
			ids[i] = e.ID.String()
		}
		slices.Sort(ids)
		if !slices.Equal(ids, oldest) {
			t.Fatalf("claimed %d events, want the %d oldest pending ones", len(ids), len(oldest))
		}
		fastest = min(fastest, elapsed)
	}
	if fastest > claimBudget {
		t.Fatalf("a claim behind %d published events took %v, want under %v", history, fastest, claimBudget)
	}
}
