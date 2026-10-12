CREATE INDEX outbox_events_due ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

DROP INDEX outbox_events_pending;
