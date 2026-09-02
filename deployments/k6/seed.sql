-- Seeds one load-test event with the given capacity and returns its id.
-- Run via `make load-seed CAPACITY=1000`, which passes :capacity to psql.
INSERT INTO events (name, venue, starts_at, sale_opens_at, price_cents, capacity)
VALUES ('Load Test', 'Arena', now() + interval '24 hours', now() - interval '1 hour', 1000, :capacity)
RETURNING id;
