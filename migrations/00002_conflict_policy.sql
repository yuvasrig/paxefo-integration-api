-- +goose Up

-- How each conflict was resolved under the tenant's conflict policy (tenants.config -> conflict.policy).
ALTER TABLE reading_conflicts
    ADD COLUMN resolution text NOT NULL DEFAULT 'kept_first'
    CHECK (resolution IN ('kept_first', 'replaced', 'rejected'));

-- The keep_last policy replaces a reading, so the application role needs DELETE on the two reading tables.
-- (raw_events stays append-only; the replaced event remains archived and is named in reading_conflicts.)
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        GRANT DELETE ON sensor_readings, location_readings TO paxafe_app;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'could not grant DELETE to paxafe_app; keep_last needs it';
END
$$;
-- +goose StatementEnd

-- +goose Down
ALTER TABLE reading_conflicts DROP COLUMN resolution;
