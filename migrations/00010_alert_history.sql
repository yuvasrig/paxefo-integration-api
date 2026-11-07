-- +goose Up
-- A history of operational alerts for the console's Notifications page: one row per alert occurrence, opened when the
-- alert starts and closed when it clears. Operator data written through the owner connection; the application role has
-- no access. The partial unique index lets several API instances record the same alert without duplicating it.
CREATE TABLE alert_history (
    id          bigserial PRIMARY KEY,
    key         text NOT NULL,
    severity    text NOT NULL,
    tenant_id   text NOT NULL DEFAULT '',
    title       text NOT NULL,
    detail      text NOT NULL DEFAULT '',
    started_at  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now(),
    cleared_at  timestamptz
);
CREATE UNIQUE INDEX alert_history_open ON alert_history (key) WHERE cleared_at IS NULL;
CREATE INDEX alert_history_started ON alert_history (started_at DESC);

-- +goose Down
DROP TABLE IF EXISTS alert_history;
