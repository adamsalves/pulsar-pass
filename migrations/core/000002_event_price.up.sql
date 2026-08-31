BEGIN;

-- No DEFAULT: pricing is explicit, and CHECK > 0 keeps reservations
-- amounts positive so the payments CHECK (amount_cents > 0) can never
-- be violated by a zero-priced event.
ALTER TABLE events
    ADD COLUMN price_cents BIGINT NOT NULL CHECK (price_cents > 0);

COMMIT;
