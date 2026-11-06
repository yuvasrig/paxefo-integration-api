-- +goose Up

-- Async ingest: the work queue. A job is added in the same transaction as the raw payload (no dual write), and
-- deleted in the transaction that records the outcome. This is operational plumbing (ids, counters, timestamps; no
-- payload), so it is not row-level secured: workers must see every tenant's jobs to serve them fairly.
CREATE TABLE ingest_jobs (
    raw_event_id    text PRIMARY KEY REFERENCES raw_events (id),
    tenant_id       text NOT NULL REFERENCES tenants (id),
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing')),
    attempts        integer NOT NULL DEFAULT 0,
    enqueued_at     timestamptz NOT NULL DEFAULT now(),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_at       timestamptz,
    locked_by       text,
    last_error      text NOT NULL DEFAULT ''
);
CREATE INDEX ingest_jobs_ready ON ingest_jobs (tenant_id, next_attempt_at) WHERE status = 'pending';
CREATE INDEX ingest_jobs_stuck ON ingest_jobs (locked_at) WHERE status = 'processing';

-- A job that keeps failing ends as outcome 'dead' (its raw payload is kept; an operator can replay it).
ALTER TABLE event_outcomes DROP CONSTRAINT event_outcomes_status_check;
ALTER TABLE event_outcomes ADD CONSTRAINT event_outcomes_status_check
    CHECK (status IN ('accepted','flagged','rejected','duplicate','conflict','failed','dead'));
CREATE INDEX event_outcomes_processed ON event_outcomes (processed_at);

-- One row per worker process: how many workers it runs and when it last reported. "Active workers" is the sum over
-- processes that reported recently, so it is right however many instances run.
CREATE TABLE worker_heartbeats (
    instance        text PRIMARY KEY,
    role            text NOT NULL,
    workers         integer NOT NULL DEFAULT 0,
    in_flight       integer NOT NULL DEFAULT 0,
    processed_total bigint NOT NULL DEFAULT 0,
    started_at      timestamptz NOT NULL DEFAULT now(),
    last_seen       timestamptz NOT NULL DEFAULT now()
);

-- Runtime settings changed from the admin console (or `api settings set`); instances pick them up within a refresh
-- interval. Environment variables are the defaults.
CREATE TABLE settings (
    key        text PRIMARY KEY,
    value      text NOT NULL,
    updated_by text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON ingest_jobs, worker_heartbeats TO paxafe_app;
        GRANT SELECT ON settings TO paxafe_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO paxafe_app;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'could not grant privileges to paxafe_app for async ingest';
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS settings;
DROP TABLE IF EXISTS worker_heartbeats;
DROP TABLE IF EXISTS ingest_jobs;
ALTER TABLE event_outcomes DROP CONSTRAINT event_outcomes_status_check;
ALTER TABLE event_outcomes ADD CONSTRAINT event_outcomes_status_check
    CHECK (status IN ('accepted','flagged','rejected','duplicate','conflict','failed'));
DROP INDEX IF EXISTS event_outcomes_processed;
