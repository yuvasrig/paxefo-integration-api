-- +goose Up

-- Previous outcomes of events that were reprocessed (`api replay --apply`). Reprocessing overwrites an event's
-- derived outcome, so the old one is archived here first: nothing is lost, and it is clear what changed and why.
CREATE TABLE event_outcome_history (
    id                bigserial PRIMARY KEY,
    raw_event_id      text NOT NULL REFERENCES raw_events (id),
    tenant_id         text NOT NULL REFERENCES tenants (id),
    replay_run_id     text NOT NULL,
    status            text NOT NULL,
    reasons           jsonb NOT NULL DEFAULT '[]',
    flags             jsonb NOT NULL DEFAULT '[]',
    expected_outputs  jsonb NOT NULL DEFAULT '{}',
    transform_version text NOT NULL DEFAULT '',
    rules_version     text NOT NULL DEFAULT '',
    processed_at      timestamptz,
    attempts          integer NOT NULL DEFAULT 1,
    archived_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX event_outcome_history_event ON event_outcome_history (raw_event_id, archived_at);
CREATE INDEX event_outcome_history_tenant ON event_outcome_history (tenant_id, archived_at);

ALTER TABLE event_outcome_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_outcome_history FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON event_outcome_history
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

-- One row per tenant per applied replay run: who replayed what, and what it changed (audit).
-- Operator data written through the owner connection; the application role has no access.
CREATE TABLE replay_runs (
    run_id       text NOT NULL,
    tenant_id    text NOT NULL REFERENCES tenants (id),
    requested_by text NOT NULL DEFAULT '',
    params       jsonb NOT NULL DEFAULT '{}',
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz NOT NULL DEFAULT now(),
    selected     integer NOT NULL DEFAULT 0,
    reprocessed  integer NOT NULL DEFAULT 0,
    changed      integer NOT NULL DEFAULT 0,
    errors       integer NOT NULL DEFAULT 0,
    transitions  jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, tenant_id)
);

-- Reprocessing removes an event's own readings and conflict rows before recomputing them.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        GRANT SELECT, INSERT ON event_outcome_history TO paxafe_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO paxafe_app;
        GRANT DELETE ON reading_conflicts TO paxafe_app;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'could not grant privileges to paxafe_app for replay';
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS replay_runs;
DROP TABLE IF EXISTS event_outcome_history;
