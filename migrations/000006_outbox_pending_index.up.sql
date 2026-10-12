-- The relay claims pending events in commit order. Through the unique index on
-- sequence it also walked every published event before them, so each claim,
-- idle polls included, cost as much as the outbox's whole history. This partial
-- index holds pending events only. It replaces the index on next_attempt_at,
-- which no query uses.
CREATE INDEX outbox_events_pending ON outbox_events (sequence) WHERE published_at IS NULL;

DROP INDEX outbox_events_due;
