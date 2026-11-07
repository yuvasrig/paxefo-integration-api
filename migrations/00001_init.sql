-- +goose Up

-- Registry and auth tables. Not row-level secured: the API key is looked up before the tenant is known.
-- The tenant comes from the API key only; the payload's AccountId is stored as metadata and never checked.
-- The application role may only SELECT from them.
CREATE TABLE tenants (
    id             text PRIMARY KEY,
    name           text NOT NULL,
    status         text NOT NULL DEFAULT 'active',
    plan           text NOT NULL DEFAULT 'free',
    retention_days integer NOT NULL DEFAULT 1095,
    config         jsonb NOT NULL DEFAULT '{}',
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id         bigserial PRIMARY KEY,
    key_hash   text NOT NULL UNIQUE,
    prefix     text NOT NULL,
    tenant_id  text NOT NULL REFERENCES tenants (id),
    name       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    revoked_at timestamptz
);

-- Immutable archive of every payload exactly as received (constitution #2). tenant_id is the API key's tenant.
CREATE TABLE raw_events (
    id           text PRIMARY KEY,
    tenant_id    text NOT NULL REFERENCES tenants (id),
    provider     text NOT NULL,
    payload_raw  bytea NOT NULL,
    payload      jsonb,
    payload_hash text NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    request_id   text NOT NULL DEFAULT ''
);
CREATE INDEX raw_events_tenant_received ON raw_events (tenant_id, received_at DESC);

-- +goose StatementBegin
CREATE FUNCTION raw_events_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'raw_events is append-only';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER raw_events_no_change
    BEFORE UPDATE OR DELETE ON raw_events
    FOR EACH ROW EXECUTE FUNCTION raw_events_immutable();

CREATE TABLE event_outcomes (
    raw_event_id      text PRIMARY KEY REFERENCES raw_events (id),
    tenant_id         text NOT NULL REFERENCES tenants (id),
    status            text NOT NULL CHECK (status IN ('accepted','flagged','rejected','duplicate','conflict','failed')),
    reasons           jsonb NOT NULL DEFAULT '[]',
    flags             jsonb NOT NULL DEFAULT '[]',
    expected_outputs  jsonb NOT NULL DEFAULT '{}',
    transform_version text NOT NULL DEFAULT '',
    rules_version     text NOT NULL DEFAULT '',
    processed_at      timestamptz NOT NULL DEFAULT now(),
    attempts          integer NOT NULL DEFAULT 1,
    error             text NOT NULL DEFAULT ''
);
CREATE INDEX event_outcomes_status ON event_outcomes (status, processed_at);

-- Independent acknowledgement record (insert-only, no hot counter row).
CREATE TABLE ingest_ledger (
    id           bigserial PRIMARY KEY,
    raw_event_id text NOT NULL REFERENCES raw_events (id),
    tenant_id    text NOT NULL REFERENCES tenants (id),
    acked_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ingest_ledger_tenant_acked ON ingest_ledger (tenant_id, acked_at);

CREATE TABLE sensor_readings (
    tenant_id          text NOT NULL REFERENCES tenants (id),
    device_imei        text NOT NULL,
    ts                 bigint NOT NULL,
    device_id          text NOT NULL,
    temperature        double precision,
    temperature_source text,
    probe_temperature  double precision,
    humidity           double precision,
    light_lux          double precision,
    accel_x            double precision,
    accel_y            double precision,
    accel_z            double precision,
    accel_g            double precision,
    quality_flags      jsonb NOT NULL DEFAULT '[]',
    extras             jsonb NOT NULL DEFAULT '{}',
    raw_event_id       text NOT NULL REFERENCES raw_events (id),
    transform_version  text NOT NULL,
    PRIMARY KEY (tenant_id, device_imei, ts)
);
CREATE INDEX sensor_readings_device_ts ON sensor_readings (tenant_id, device_id, ts DESC);

CREATE TABLE location_readings (
    tenant_id         text NOT NULL REFERENCES tenants (id),
    device_imei       text NOT NULL,
    ts                bigint NOT NULL,
    device_id         text NOT NULL,
    lat               double precision NOT NULL,
    lon               double precision NOT NULL,
    accuracy_m        integer,
    accuracy_category text,
    source            text,
    street            text,
    locality          text,
    state             text,
    country           text,
    postal_code       text,
    full_address      text,
    battery_pct       integer,
    cellular_dbm      double precision,
    wifi_aps          integer,
    quality_flags     jsonb NOT NULL DEFAULT '[]',
    extras            jsonb NOT NULL DEFAULT '{}',
    raw_event_id      text NOT NULL REFERENCES raw_events (id),
    transform_version text NOT NULL,
    PRIMARY KEY (tenant_id, device_imei, ts)
);
CREATE INDEX location_readings_device_ts ON location_readings (tenant_id, device_id, ts DESC);

-- Second, different versions of an existing key; the first reading is kept.
CREATE TABLE reading_conflicts (
    id                bigserial PRIMARY KEY,
    tenant_id         text NOT NULL REFERENCES tenants (id),
    device_imei       text NOT NULL,
    ts                bigint NOT NULL,
    raw_event_id      text NOT NULL UNIQUE REFERENCES raw_events (id),
    first_raw_event_id text NOT NULL REFERENCES raw_events (id),
    diff              jsonb NOT NULL DEFAULT '{}',
    created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reading_conflicts_key ON reading_conflicts (tenant_id, device_imei, ts);

-- Identity registry per tenant with validity windows (a reused tracker keeps each tenant's history).
CREATE TABLE devices (
    tenant_id   text NOT NULL REFERENCES tenants (id),
    imei        text NOT NULL,
    device_name text NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz,
    first_seen  timestamptz NOT NULL,
    last_seen   timestamptz NOT NULL,
    last_lat    double precision,
    last_lon    double precision,
    last_ts     bigint,
    PRIMARY KEY (tenant_id, imei, valid_from)
);
-- At most one open assignment per IMEI across all tenants.
CREATE UNIQUE INDEX devices_one_open_per_imei ON devices (imei) WHERE valid_to IS NULL;
CREATE INDEX devices_open_name ON devices (tenant_id, device_name) WHERE valid_to IS NULL;

CREATE TABLE shipments (
    tenant_id    text NOT NULL REFERENCES tenants (id),
    shipment_id  text NOT NULL,
    public_id    text,
    carrier      text,
    description  text,
    from_lat     double precision,
    from_lon     double precision,
    from_address text,
    to_lat       double precision,
    to_lon       double precision,
    to_address   text,
    temp_min_c   double precision,
    temp_max_c   double precision,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, shipment_id)
);

-- Row-level security: every tenant table, forced so even the table owner is subject to it.
-- A policy matches on the transaction-local setting app.tenant_id (set by Store.WithTenantTx).
-- An unset setting matches nothing.
-- +goose StatementBegin
DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['raw_events','event_outcomes','ingest_ledger','sensor_readings','location_readings','reading_conflicts','devices','shipments']
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
        EXECUTE format($p$CREATE POLICY tenant_isolation ON %I
            USING (tenant_id = current_setting('app.tenant_id', true))
            WITH CHECK (tenant_id = current_setting('app.tenant_id', true))$p$, t);
    END LOOP;
END
$$;
-- +goose StatementEnd

-- Application role: no BYPASSRLS, least privilege (no UPDATE/DELETE on raw_events or readings).
-- The store runs SET LOCAL ROLE paxafe_app inside each tenant transaction. If the migrating user
-- may not create roles (some managed databases), FORCE ROW LEVEL SECURITY above still applies
-- to a non-superuser owner.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        BEGIN
            CREATE ROLE paxafe_app NOLOGIN NOBYPASSRLS;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL; -- another migration (another instance, or another database in the cluster) created it at the same moment
        END;
    END IF;
    EXECUTE format('GRANT paxafe_app TO %I', current_user);
    GRANT USAGE ON SCHEMA public TO paxafe_app;
    GRANT SELECT ON tenants, api_keys TO paxafe_app;
    GRANT SELECT, INSERT ON raw_events, ingest_ledger, sensor_readings, location_readings, reading_conflicts TO paxafe_app;
    GRANT SELECT, INSERT, UPDATE ON event_outcomes, devices, shipments TO paxafe_app;
    GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO paxafe_app;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'could not set up role paxafe_app; relying on FORCE ROW LEVEL SECURITY';
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS shipments, devices, reading_conflicts, location_readings, sensor_readings,
    ingest_ledger, event_outcomes, raw_events, api_keys, tenants CASCADE;
DROP FUNCTION IF EXISTS raw_events_immutable();
