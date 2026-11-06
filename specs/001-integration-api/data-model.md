# Data model (M2 scope)

Source: DESIGN.md §7, §7.2. Postgres 16. Every tenant table carries `tenant_id`, every unique key and index leads with it, and every tenant table has a row-level-security policy. The app role has no `BYPASSRLS`; migrations run under a separate role. Timestamps are `timestamptz` in UTC; reading event time `ts` is `bigint` epoch ms (matches PAXAFE).

## Tables

### tenants
`id` text PK · `name` · `status` · `plan` · `retention_days` · `config` jsonb · `created_at`. `config.conflict = {policy: keep_first|keep_last|reject, http_status: 200|409}` sets the tenant's conflict handling (default keep_first / 200); set it with `api tenants set-conflict`. `config.ingest_mode` (sync|async) and `config.workers = {min, max}` (the tenant's worker pool limits; `api tenants set-workers`) are the other optional keys.
Seeded by `api seed`: `reviewer`, `acme-pharma`, `bioco`. The tenant of a request is the API key's; there is no account-to-tenant mapping.

### api_keys
`id` · `key_hash` (SHA-256, unique) · `prefix` · `tenant_id` FK · `name` · `created_at` · `expires_at` · `revoked_at`. Plaintext never stored.

### event_outcome_history
Outcomes replaced by a reprocess (`api replay --apply`): `raw_event_id` · `tenant_id` · `replay_run_id` · `status` · `reasons` · `flags` · `expected_outputs` · `transform_version` · `rules_version` · `processed_at` · `attempts` · `archived_at`. Row-level secured; the application role can only read and insert.

### replay_runs
One row per tenant per applied replay: `run_id` · `tenant_id` · `requested_by` · `params` (window, filters) · `started_at` · `finished_at` · `selected` · `reprocessed` · `changed` · `errors` · `transitions` (e.g. `conflict -> accepted`: 3). Operator data written through the owner connection.

### rulesets
`id` · `version` (unique; recorded on every event, so a changed ruleset needs a new version) · `yaml` · `created_by` · `created_at` · `active`. At most one row is active (partial unique index). Written by `api rules set`; the running service only reads it. No active row means the service uses `RULES_PATH` or the embedded default. Global configuration: not row-level secured.

### raw_events (append-only)
`id` text PK (`evt_…`) · `tenant_id` (the API key's tenant) · `provider` · `payload_raw` bytea (the exact bytes received; always stored, even if not JSON) · `payload` jsonb (convenience copy when the bytes are valid JSON and jsonb can hold them) · `payload_hash` (SHA-256 of canonical JSON) · `received_at` · `request_id`.
Index `(tenant_id, received_at DESC)`. No UPDATE/DELETE grants.

### event_outcomes
`raw_event_id` PK/FK · `tenant_id` · `status` (accepted, flagged, rejected, duplicate, conflict, failed) · `reasons` jsonb · `flags` jsonb · `expected_outputs` jsonb `{sensor, location}` · `transform_version` · `rules_version` · `processed_at` · `attempts` · `error` (message of the last failure).
Index `(status, processed_at)`.

### ingest_ledger (insert-only)
`id` · `raw_event_id` FK · `tenant_id` · `acked_at`. Index `(tenant_id, acked_at)`.

### sensor_readings
`tenant_id` · `device_imei` · `ts` · `device_id` · `temperature` · `temperature_source` (device|probe) · `probe_temperature` · `humidity` · `light_lux` · `accel_x/y/z/g` · `quality_flags` jsonb · `extras` jsonb · `raw_event_id` FK · `transform_version`.
**Unique `(tenant_id, device_imei, ts)`**; index `(tenant_id, device_id, ts DESC)`. Full precision stored; rounding only on output.

### location_readings
`tenant_id` · `device_imei` · `ts` · `device_id` · `lat` · `lon` · `accuracy_m` · `accuracy_category` · `source` · `street`, `locality`, `state`, `country`, `postal_code`, `full_address` · `battery_pct` · `cellular_dbm` · `wifi_aps` · `quality_flags` jsonb · `extras` jsonb · `raw_event_id` FK · `transform_version`.
**Unique `(tenant_id, device_imei, ts)`**; index `(tenant_id, device_id, ts DESC)`.

### reading_conflicts
`tenant_id` · `device_imei` · `ts` · `raw_event_id` · `first_raw_event_id` · `diff` jsonb · `created_at`. `resolution` (`kept_first`, `replaced`, `rejected`) records the tenant's policy outcome. `raw_event_id` is unique, so replays do not duplicate conflicts. Index `(tenant_id, device_imei, ts)`.

### devices
`tenant_id` · `imei` · `device_name` · `valid_from` · `valid_to` (null = open) · `first_seen` · `last_seen` · `last_lat` · `last_lon` · `last_ts`.
Unique `(tenant_id, imei, valid_from)`; at most one open assignment per IMEI across tenants (partial unique index on `imei WHERE valid_to IS NULL`). `last_*` update only when the new `ts` is newer. A payload never takes over a device that is open in another tenant (its readings are still stored); moving a tracker is the operator action `Store.ReassignDevice` (no CLI command yet).

### shipments
`tenant_id` · `shipment_id` · `public_id` · `carrier` · ship-from/to coordinates and addresses · `temp_min_c` · `temp_max_c`. Unique `(tenant_id, shipment_id)`.

## Row-level security
Registry and auth tables (`tenants`, `api_keys`) are not row-level secured because they are read before a tenant is known; the application role may only SELECT from them. All other tables are secured and `FORCE`d.

The store runs `SET LOCAL ROLE paxafe_app` (no `BYPASSRLS`, no UPDATE/DELETE on raw events or readings) inside every tenant transaction. `raw_events` also has a trigger that rejects UPDATE and DELETE for every role. If the migrating database user cannot create roles, `FORCE ROW LEVEL SECURITY` still applies to a non-superuser owner.

Policy per table: `USING (tenant_id = current_setting('app.tenant_id', true))`. `Store.WithTenantTx(ctx, tenantID, fn)` runs `SET LOCAL app.tenant_id` inside the transaction. Unset setting → no rows.

## Async ingest (migration 00005)
- **ingest_jobs**: `raw_event_id` PK · `tenant_id` · `status` (pending|processing) · `attempts` · `enqueued_at` · `next_attempt_at` · `locked_at` · `locked_by` · `last_error`. One row per event waiting for a worker; inserted with the raw event, deleted with the outcome. Workers claim with `FOR UPDATE SKIP LOCKED`. Global (not row-level secured): workers serve every tenant.
- **worker_heartbeats**: (`instance`, `tenant_id`) PK, one row per tenant pool a process runs (`tenant_id` empty for a process with no pools yet) · `role` · `workers` · `in_flight` · `processed_total` · `started_at` · `last_seen`. A process reports every few seconds; rows older than a minute are ignored.
- **settings**: `key` PK · `value` · `updated_by` · `updated_at`. Runtime overrides of the environment defaults.
- `event_outcomes.status` also allows `dead` (gave up after `MAX_ATTEMPTS`).

## Not in M0–M7
`alerts`, `reconciliation_runs`, Timescale hypertables.

## Invariants
1. Every normalized row has a `raw_event_id` that exists.
2. `raw_events` rows are never updated or deleted.
3. At most one reading per `(tenant_id, device_imei, ts)`.
4. A conflict row exists iff a second, different payload arrived for an existing key.
