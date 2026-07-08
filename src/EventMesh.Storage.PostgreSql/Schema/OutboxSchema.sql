CREATE TABLE IF NOT EXISTS eventmesh_outbox
(
    id             TEXT        NOT NULL PRIMARY KEY,
    message_id     TEXT        NOT NULL,
    envelope_json  JSONB       NOT NULL,
    destination    TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at   TIMESTAMPTZ NULL,
    status         SMALLINT    NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_eventmesh_outbox_status_created_at
    ON eventmesh_outbox (status, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_eventmesh_outbox_message_id
    ON eventmesh_outbox (message_id);

CREATE TABLE IF NOT EXISTS eventmesh_inbox
(
    id             TEXT        NOT NULL PRIMARY KEY,
    message_id     TEXT        NOT NULL,
    consumer_group TEXT        NOT NULL DEFAULT 'default',
    processed_at   TIMESTAMPTZ NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_eventmesh_inbox_message_consumer
    ON eventmesh_inbox (message_id, consumer_group);

CREATE INDEX IF NOT EXISTS ix_eventmesh_inbox_processed_at
    ON eventmesh_inbox (processed_at);

CREATE TABLE IF NOT EXISTS eventmesh_scheduled_messages
(
    id             TEXT        NOT NULL PRIMARY KEY,
    envelope_json  JSONB       NOT NULL,
    destination    TEXT        NOT NULL,
    scheduled_at   TIMESTAMPTZ NOT NULL,
    status         SMALLINT    NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_eventmesh_scheduled_messages_status_scheduled_at
    ON eventmesh_scheduled_messages (status, scheduled_at);
