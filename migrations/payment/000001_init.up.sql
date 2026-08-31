BEGIN;

CREATE TABLE payments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id  UUID NOT NULL,
    user_id         TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    currency        CHAR(3) NOT NULL DEFAULT 'BRL',
    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    idempotency_key TEXT NOT NULL,
    gateway_ref     TEXT,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT payments_idempotency_unique UNIQUE (idempotency_key)
);

CREATE INDEX payments_reservation ON payments (reservation_id);
CREATE INDEX payments_status ON payments (status);

CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY,
    subject        TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  INTEGER NOT NULL DEFAULT 1,
    source         TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id   TEXT,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ
);

CREATE INDEX outbox_events_unprocessed
    ON outbox_events (created_at)
    WHERE processed_at IS NULL;

CREATE TABLE processed_messages (
    message_id   TEXT PRIMARY KEY,
    consumer     TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
