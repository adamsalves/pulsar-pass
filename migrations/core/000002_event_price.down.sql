BEGIN;

ALTER TABLE events
    DROP COLUMN price_cents;

COMMIT;
