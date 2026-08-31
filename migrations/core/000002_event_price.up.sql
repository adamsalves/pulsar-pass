BEGIN;

ALTER TABLE events
    ADD COLUMN price_cents BIGINT NOT NULL DEFAULT 0 CHECK (price_cents >= 0);

COMMIT;
