BEGIN;

DROP TABLE processed_messages;
DROP TABLE outbox_events;
DROP TABLE tickets;
DROP TABLE reservations;
DROP TABLE events;

COMMIT;
