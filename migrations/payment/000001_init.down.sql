BEGIN;

DROP TABLE processed_messages;
DROP TABLE outbox_events;
DROP TABLE payments;

COMMIT;
