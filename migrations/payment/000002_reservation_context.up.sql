BEGIN;

CREATE TABLE reservation_context (
    reservation_id  UUID PRIMARY KEY,
    user_id         TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL CHECK (amount_cents >= 0),
    currency        CHAR(3) NOT NULL DEFAULT 'BRL',
    expires_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
