BEGIN;

CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    venue           TEXT NOT NULL,
    starts_at       TIMESTAMPTZ NOT NULL,
    sale_opens_at   TIMESTAMPTZ NOT NULL,
    capacity        INTEGER NOT NULL CHECK (capacity > 0),
    reserved_count  INTEGER NOT NULL DEFAULT 0,
    sold_count      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_counts_valid CHECK (
        reserved_count >= 0
        AND sold_count >= 0
        AND reserved_count + sold_count <= capacity
    )
);

CREATE TABLE reservations (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id      UUID NOT NULL REFERENCES events(id),
    user_id       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'CONFIRMED', 'EXPIRED', 'FAILED', 'CANCELLED')),
    quantity      INTEGER NOT NULL CHECK (quantity > 0),
    amount_cents  BIGINT NOT NULL DEFAULT 0 CHECK (amount_cents >= 0),
    currency      CHAR(3) NOT NULL DEFAULT 'BRL',
    expires_at    TIMESTAMPTZ NOT NULL,
    confirmed_at  TIMESTAMPTZ,
    version       INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX reservations_one_active_per_user
    ON reservations (user_id, event_id)
    WHERE status IN ('PENDING', 'CONFIRMED');

CREATE INDEX reservations_expires_at_pending
    ON reservations (expires_at)
    WHERE status = 'PENDING';

CREATE TABLE tickets (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    reservation_id  UUID REFERENCES reservations(id),
    seat_label      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'AVAILABLE'
                    CHECK (status IN ('AVAILABLE', 'RESERVED', 'SOLD')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tickets_seat_unique UNIQUE (event_id, seat_label)
);

CREATE INDEX tickets_event_status ON tickets (event_id, status);
CREATE INDEX tickets_reservation
    ON tickets (reservation_id)
    WHERE reservation_id IS NOT NULL;

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
