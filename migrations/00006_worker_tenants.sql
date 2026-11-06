-- +goose Up
-- Each tenant has its own worker pool, so a process reports one heartbeat row per tenant pool it runs.
ALTER TABLE worker_heartbeats ADD COLUMN tenant_id text NOT NULL DEFAULT '';
ALTER TABLE worker_heartbeats DROP CONSTRAINT worker_heartbeats_pkey;
ALTER TABLE worker_heartbeats ADD PRIMARY KEY (instance, tenant_id);

-- +goose Down
DELETE FROM worker_heartbeats WHERE tenant_id <> '';
ALTER TABLE worker_heartbeats DROP CONSTRAINT worker_heartbeats_pkey;
ALTER TABLE worker_heartbeats ADD PRIMARY KEY (instance);
ALTER TABLE worker_heartbeats DROP COLUMN tenant_id;
