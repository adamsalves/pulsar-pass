-- Inventory invariants after a load run. Returns a single number: the
-- count of violated invariants (0 = sound). Run via `make load-verify`
-- a few seconds after the run ends, once the outboxes drained and the
-- TTL sweeper caught up.
--
-- 1. Zero overbooking: reserved + sold never exceeds capacity.
-- 2. sold_count matches exactly the confirmed reservations' tickets.
-- 3. reserved_count matches exactly the pending reservations' tickets.
-- LEFT JOINs make absent-side reservations (e.g. sold tickets with no
-- confirmed rows) count as violations instead of vanishing.
SELECT
    (SELECT count(*) FROM events WHERE reserved_count + sold_count > capacity)
  + (SELECT count(*) FROM events e
       LEFT JOIN (SELECT event_id, sum(quantity) AS q
                    FROM reservations WHERE status = 'CONFIRMED' GROUP BY event_id) c
         ON c.event_id = e.id
      WHERE COALESCE(c.q, 0) <> e.sold_count)
  + (SELECT count(*) FROM events e
       LEFT JOIN (SELECT event_id, sum(quantity) AS q
                    FROM reservations WHERE status = 'PENDING' GROUP BY event_id) p
         ON p.event_id = e.id
      WHERE COALESCE(p.q, 0) <> e.reserved_count)
  AS violations
FROM (SELECT 1) one;
