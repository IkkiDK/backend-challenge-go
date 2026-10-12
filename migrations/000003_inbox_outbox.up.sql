-- Transactional outbox of integration events and inbox of consumed messages.
-- Both are written in the same SQL transaction as the domain changes.

CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,
    -- Commit order within a wallet: rows of one wallet are inserted under its
    -- row lock, so a higher sequence always commits later.
    sequence        bigint      GENERATED ALWAYS AS IDENTITY CONSTRAINT outbox_events_sequence_key UNIQUE,
    aggregate_type  text        NOT NULL,
    aggregate_id    text        NOT NULL,
    -- Events sharing an ordering key (the wallet) are published in sequence order.
    ordering_key    text        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL CHECK (event_version >= 1),
    -- Immutable snapshot of the published envelope. json (not jsonb) keeps
    -- the exact serialized text, so every publication is byte-identical.
    payload         json        NOT NULL,
    occurred_at     timestamptz NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    -- Earliest time a publisher may claim the event: the backoff after a
    -- failure, or the lease expiry while a publisher holds it.
    next_attempt_at timestamptz NOT NULL,
    published_at    timestamptz,
    claimed_by      text,
    last_error      text
);

CREATE INDEX outbox_events_due ON outbox_events (next_attempt_at) WHERE published_at IS NULL;
CREATE INDEX outbox_events_unpublished_by_key ON outbox_events (ordering_key, sequence) WHERE published_at IS NULL;

-- The event itself never changes, a published event stays published, and an
-- unpublished event can never be deleted (published ones may be pruned).
CREATE FUNCTION outbox_events_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NULL THEN
            RAISE EXCEPTION 'outbox event % is not published yet and cannot be deleted', OLD.id
                USING ERRCODE = 'WL003';
        END IF;
        RETURN OLD;
    END IF;
    -- json has no equality operator; its text is what must not change.
    IF (NEW.id, NEW.sequence, NEW.aggregate_type, NEW.aggregate_id, NEW.ordering_key,
        NEW.event_type, NEW.event_version, NEW.payload::text, NEW.occurred_at)
        IS DISTINCT FROM
       (OLD.id, OLD.sequence, OLD.aggregate_type, OLD.aggregate_id, OLD.ordering_key,
        OLD.event_type, OLD.event_version, OLD.payload::text, OLD.occurred_at) THEN
        RAISE EXCEPTION 'outbox event % is an immutable snapshot', OLD.id
            USING ERRCODE = 'WL002';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % was already published at %', OLD.id, OLD.published_at
            USING ERRCODE = 'WL002';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER outbox_events_guard BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();

CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    -- Hash of the message content, compared on every redelivery.
    payload_hash   text        NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    -- The transaction the message created or replayed.
    transaction_id uuid        NOT NULL REFERENCES wagering_transactions (id),
    received_at    timestamptz NOT NULL,
    completed_at   timestamptz NOT NULL,
    CONSTRAINT inbox_messages_pkey PRIMARY KEY (consumer_name, message_id),
    CONSTRAINT inbox_messages_completed_after_received CHECK (completed_at >= received_at)
);

CREATE TRIGGER inbox_messages_immutable BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION forbid_change();
