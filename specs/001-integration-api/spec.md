# Spec 001 — Integration API (M0–M7 scope)

Source of truth: DESIGN.md. This spec states testable requirements in EARS form. Every requirement has an ID; tests and code comments reference it. Sample numbers (`#0`–`#12`) are the index in `assignment/sample-tive-payloads.json`; expected results are in `expected/`.

EARS patterns: `WHEN <event>, THE SYSTEM SHALL <response>` · `IF <condition>, THEN THE SYSTEM SHALL …` · `THE SYSTEM SHALL …` (always).

Test precondition (unless stated): fresh `reviewer` tenant, Tive `AccountId` 478 mapped to it, 999 unmapped, samples sent in order 0 → 12.

Out of scope for this spec: async ingest, queue, workers, alerts, reconciler, Timescale, domain checks (`TEMP_EXCURSION`, `IMPOSSIBLE_TRAVEL`).

---

## F1 — Ingest, happy path

- **F1-R1** WHEN a valid POST arrives at `/api/v1/webhook/tive` or `/api/webhook/tive` with a valid API key, THE SYSTEM SHALL process it identically on both paths.
- **F1-R2** WHEN a payload is processed, THE SYSTEM SHALL commit the raw payload and one ledger row before returning any 2xx.
- **F1-R3** WHEN a payload has a usable temperature or other sensor value, THE SYSTEM SHALL emit a PAXAFE sensor record per `contracts/mapping.md`.
- **F1-R4** WHEN a payload has valid coordinates, THE SYSTEM SHALL emit a PAXAFE location record per `contracts/mapping.md`.
- **F1-R5** THE SYSTEM SHALL round only when emitting PAXAFE JSON (half away from zero) and store full precision.
- **F1-R6** THE SYSTEM SHALL validate every emitted record against the PAXAFE sensor/location JSON Schemas.
- **F1-R7** WHEN processing completes without flags of severity warn or error, THE SYSTEM SHALL respond 200 with `status: accepted`, the PAXAFE output, empty `errors`, and an `event_id` and `request_id`.
- **F1-R8** THE SYSTEM SHALL accept unknown extra fields and keep them in the raw payload.

**Acceptance (Given/When/Then)**
- *#2:* GIVEN a clean GPS payload, WHEN posted, THEN 200 `accepted`, one sensor and one location record, `location_source` = `GPS`, no flags.
- *#10:* GIVEN a payload with null probe values, null accuracy and null `LocationMethod`, WHEN posted, THEN 200 `accepted` (only an info flag), `location_accuracy`, category and source are `null`.
- *F1-R1:* GIVEN #2, WHEN posted to both paths on two fresh tenants, THEN the responses are identical apart from ids.

## F2 — Ingest, partial or flagged

- **F2-R1** WHEN `Location.Latitude` ∉ [-90, 90] or `Location.Longitude` ∉ [-180, 180], THE SYSTEM SHALL skip the location record AND add flag `INVALID_COORDINATES` AND still emit the sensor record.
- **F2-R2** WHEN `Temperature.Celsius` or `ProbeTemperature.Celsius` is -32768, 327.67 or 32767, THE SYSTEM SHALL set it to null AND add flag `SENSOR_FAULT`.
- **F2-R3** WHEN `Humidity.Percentage` ∉ [0, 100], `Light.Lux` < 0 or `Battery.Percentage` ∉ [0, 100], THE SYSTEM SHALL set it to null AND add flag `OUT_OF_RANGE`.
- **F2-R4** WHEN a numeric field arrives as a numeric string, THE SYSTEM SHALL parse it AND add flag `COERCED_TYPE` (one per field) AND keep the parsed value.
- **F2-R5** WHEN `EntryTimeEpoch` < 1e12, THE SYSTEM SHALL multiply it by 1000 AND add flag `TIMESTAMP_SECONDS`.
- **F2-R6** WHEN `EntryTimeEpoch` and `EntryTimeUtc` differ by more than 1 s (after F2-R5), THE SYSTEM SHALL keep the epoch AND add flag `TIMESTAMP_MISMATCH`.
- **F2-R7** WHEN `EntityName` ≠ `DeviceName`, or `Shipment.DeviceId` ≠ `DeviceId`, THE SYSTEM SHALL keep the values as sent AND add flag `IDENTITY_MISMATCH`.
- **F2-R8** WHEN `Temperature.Celsius` is null or nulled by F2-R2 and `ProbeTemperature.Celsius` is usable, THE SYSTEM SHALL use the probe value AND set `temperature_source = probe` AND add flag `TEMPERATURE_FALLBACK`.
- **F2-R9** WHEN `Temperature.Celsius` is null and `Temperature.Fahrenheit` is present, THE SYSTEM SHALL compute Celsius = (F − 32) × 5/9 AND add flag `TEMPERATURE_CONVERTED`.
- **F2-R10** WHEN `Accelerometer.G` is missing and X, Y, Z are present, THE SYSTEM SHALL set magnitude = √(x²+y²+z²).
- **F2-R11** THE SYSTEM SHALL record, for every fix or nullification, the rule ID, the original value and the new value.
- **F2-R12** WHEN any flag of severity warn or error is present and the event is not rejected, duplicate or conflict, THE SYSTEM SHALL respond 200 with `status: flagged`.
- **F2-R13** WHEN only info flags are present, THE SYSTEM SHALL respond `accepted`.
- **F2-R15** WHEN a field that should be a number is a non-numeric string, an object or an array, THE SYSTEM SHALL set it to null AND add flag `INVALID_TYPE`.
- **F2-R14** WHEN `Location.Accuracy.Meters` is present, THE SYSTEM SHALL set the accuracy category High if < 50, Medium if < 500, else Low (thresholds configurable).

**Acceptance**
- *#0:* GIVEN a clean payload whose `Shipment.DeviceId` differs from `DeviceId`, THEN 200 `flagged` with `IDENTITY_MISMATCH`; sensor temperature 10.08, location category High, source `WiFi`.
- *#3:* GIVEN probe 327.67, THEN `SENSOR_FAULT`, probe nulled, device temperature 4.63 (4.625 rounds half away from zero) used with no fallback flag.
- *#5:* GIVEN epoch 1739561246, THEN `TIMESTAMP_SECONDS`, `timestamp` = 1739561246000, no `TIMESTAMP_MISMATCH`.
- *#6:* GIVEN probe -32768 and a `cell` location of 500 m, THEN `SENSOR_FAULT`, `location_source` = `Cellular`, category `Low`, `Shipment` null accepted.
- *#11:* GIVEN `EntryTimeUtc` 14:27:26Z and epoch for 19:27:26Z, THEN `TIMESTAMP_MISMATCH`, epoch kept as `timestamp`, 200 `flagged`.
- *#7:* GIVEN `Dbm` and `Temperature.*` as strings and a shipment device mismatch, THEN `COERCED_TYPE` for each coerced field and `IDENTITY_MISMATCH`; temperature 10.5, `cellular_dbm` -100.00.

## F3 — Ingest, rejected

- **F3-R1** IF the body is not valid JSON, THEN THE SYSTEM SHALL store the raw bytes, respond 400 with error `INVALID_JSON` and status `rejected`.
- **F3-R2** IF `DeviceId` is missing or not 15 digits AND `DeviceName` is not found in the tenant's `devices`, THEN THE SYSTEM SHALL reject with 422 `NO_DEVICE_IDENTITY`. THE SYSTEM SHALL NOT resolve identity from `EntityName`.
- **F3-R3** IF `EntryTimeEpoch` is missing or invalid, THEN THE SYSTEM SHALL reject with 422 `NO_TIMESTAMP`.
- **F3-R4** IF the timestamp is more than 5 minutes ahead of server time, THEN THE SYSTEM SHALL reject with 422 `TIMESTAMP_FUTURE`.
- **F3-R5** IF there is no usable temperature AND no valid coordinates, THEN THE SYSTEM SHALL reject with 422 `NO_USABLE_DATA`.
- **F3-R6** IF `AccountId` maps to a known tenant other than the API key's, THEN THE SYSTEM SHALL reject with 422 `TENANT_MISMATCH` AND write nothing to the other tenant.
- **F3-R7** WHEN an event is rejected, THE SYSTEM SHALL keep its raw payload, record outcome `rejected` with reasons, and return flags raised before the rejection.
- **F3-R8** IF `AccountId` is absent, THEN THE SYSTEM SHALL use the API key's tenant AND add flag `ACCOUNT_ID_MISSING`.
- **F3-R9** IF `AccountId` is not mapped to any tenant, THEN THE SYSTEM SHALL store the event under tenant `common-pool` AND add flag `UNKNOWN_ACCOUNT` AND process it normally, so no data is dropped. The submitting tenant SHALL be able to read the event by id but not the resulting readings.

**Acceptance**
- *#12:* GIVEN `DeviceName` Ship33CABOL (unknown) and no `DeviceId`, WHEN posted, THEN 422 `rejected`, error `NO_DEVICE_IDENTITY`, flag `IDENTITY_MISMATCH` (EntityName A571992 ≠ DeviceName), and the raw event exists.
- *#9:* GIVEN `AccountId` 999 (unmapped), THEN 200 `flagged` with `UNKNOWN_ACCOUNT`; event readable with the reviewer key; `GET /readings` for the device with the reviewer key returns none.
- *F3-R6:* GIVEN `AccountId` 1001 (mapped to `acme-pharma`) posted with the reviewer key, THEN 422 `TENANT_MISMATCH` and zero rows in `acme-pharma`.
- *F3-R1:* GIVEN body `{not json`, THEN 400 and one raw event.
- *F3-R5:* GIVEN no `Temperature.Celsius`, no probe and latitude 95, THEN 422 `NO_USABLE_DATA`.

## F4 — Duplicate and conflict

- **F4-R1** THE SYSTEM SHALL enforce uniqueness of readings on `(tenant_id, device_imei, ts)` in the database.
- **F4-R2** WHEN a payload has the same key and the same canonical-JSON hash as a stored event, THE SYSTEM SHALL respond 200 `duplicate` AND write no new readings.
- **F4-R3** WHEN a payload has the same key but a different hash, THE SYSTEM SHALL keep the first readings, store the second version in `reading_conflicts`, add flag `DUPLICATE_CONFLICT`, and respond 200 `conflict`.
- **F4-R4** WHEN the same key arrives concurrently, THE SYSTEM SHALL end with exactly one reading row.
- **F4-R5** THE SYSTEM SHALL compute the payload hash over canonical JSON (sorted keys, no insignificant whitespace), so key order and whitespace never create a conflict.
- **F4-R6** WHEN the status of one event satisfies several of rejected / conflict / duplicate / flagged / accepted, THE SYSTEM SHALL report the first in that precedence.

**Acceptance**
- *#1 after #0:* same `(imei, ts)`, different content → 200 `conflict`, flags `INVALID_COORDINATES`, `DUPLICATE_CONFLICT`, `ACCOUNT_ID_MISSING`; stored sensor reading is still #0's (temperature 10.08).
- *#4 after #0:* resolved by name to the same key → 200 `conflict`.
- *#8 after #0:* → 200 `conflict`, `INVALID_COORDINATES`.
- *Re-send of #2:* → 200 `duplicate`, row count unchanged.
- *F4-R5:* #2 re-sent with reordered keys and different whitespace → `duplicate`, not `conflict`.

## F5 — Device resolution

- **F5-R1** WHEN `DeviceId` is a valid 15-digit IMEI, THE SYSTEM SHALL use it as `device_imei` AND upsert the device (`first_seen`; `last_*` only when the new timestamp is newer).
- **F5-R2** WHEN `DeviceId` is missing or invalid AND `DeviceName` matches an open device assignment in the tenant, THE SYSTEM SHALL fill the IMEI AND add flag `DEVICE_RESOLVED_BY_NAME`.
- **F5-R3** WHEN `DeviceName` maps to more than one IMEI in the tenant, THE SYSTEM SHALL reject with `NO_DEVICE_IDENTITY` rather than guess.
- **F5-R4** THE SYSTEM SHALL scope device identity per tenant with validity windows, so a reused tracker's history stays with the right tenant.

**Acceptance**
- *#4 after #0:* IMEI `863257063350583` resolved from `A571992`, flag `DEVICE_RESOLVED_BY_NAME`.
- *#4 on a fresh tenant with no prior A571992:* 422 `NO_DEVICE_IDENTITY`.
- *#12:* see F3.

## F6 — Edge rejection

- **F6-R1** IF `X-API-Key` is missing, malformed, unknown, expired or revoked, THEN THE SYSTEM SHALL respond 401 AND store nothing.
- **F6-R2** THE SYSTEM SHALL store only the SHA-256 hash of API keys and never log a key.
- **F6-R3** IF the body exceeds 256 KB, THEN THE SYSTEM SHALL respond 413 AND store nothing.
- **F6-R4** IF a tenant exceeds its rate limit, THEN THE SYSTEM SHALL respond 429 with `Retry-After` AND store nothing.
- **F6-R5** THE SYSTEM SHALL resolve the tenant from the API key and apply row-level security so that no query returns another tenant's rows.
- **F6-R6** THE SYSTEM SHALL accept or generate `X-Request-ID` and echo it in the response and logs.

**Acceptance**
- GIVEN no key / bad key / expired key / revoked key, THEN 401 and zero new `raw_events`.
- GIVEN a 300 KB body, THEN 413.
- GIVEN tenant A's key, WHEN requesting tenant B's event or readings, THEN 404 / empty (also at the SQL level via RLS).

## F7 — Failure and replay

- **F7-R1** IF an error occurs after the raw payload is committed, THEN THE SYSTEM SHALL record outcome `failed` AND respond 500 with `status: failed` AND keep the raw payload.
- **F7-R2** WHEN `POST /api/v1/events/{id}/replay` is called for a `failed` or `rejected` event, THE SYSTEM SHALL re-run the stored raw payload through the pipeline and return the new outcome.
- **F7-R3** THE SYSTEM SHALL make replay idempotent: no duplicate readings, raw payload unchanged.
- **F7-R4** WHEN replay is called for an `accepted` or `flagged` event, THE SYSTEM SHALL respond 409.
- **F7-R5** THE SYSTEM SHALL store `transform_version` and `rules_version` with every outcome.
- **F7-R6** WHEN `GET /api/v1/events/{id}` is called by the owning or submitting tenant, THE SYSTEM SHALL return the raw payload, outcome, flags and PAXAFE output.

**Acceptance**
- GIVEN an injected failure in the second transaction, THEN raw event exists, outcome `failed`, HTTP 500; after the fault is removed, replay returns 200 and exactly one reading row exists.
- *#12 replay after A571992-style registration of Ship33CABOL:* outcome changes from `rejected` to `accepted`/`flagged`.

## F13 — Lifecycle

- **F13-R1** WHEN the process starts, THE SYSTEM SHALL load configuration, connect to the database, run migrations, then serve.
- **F13-R2** IF configuration is invalid (including the ruleset), THEN THE SYSTEM SHALL exit non-zero before serving.
- **F13-R3** THE SYSTEM SHALL expose `/healthz` (process up) without authentication.
- **F13-R4** THE SYSTEM SHALL expose `/readyz` reporting database reachability, migration version, storage backend and ingest mode.
- **F13-R5** WHEN SIGTERM is received, THE SYSTEM SHALL stop accepting connections, finish in-flight requests within 10 s, and close the database pool.
- **F13-R6** THE SYSTEM SHALL log one structured JSON line per request with request id, tenant id, event id, status and latency, and SHALL NOT log API keys or full payloads.
- **F13-R7** THE SYSTEM SHALL expose Prometheus metrics at `/metrics`.

*M0 delivers F13-R3 only (hello-world `/healthz`); the rest arrive in M2–M4.*

---

## Traceability

| Flow | Requirement IDs | Samples | Milestone |
|---|---|---|---|
| F1 | F1-R1…R8 | #2, #10 | M1 (transform), M3 (HTTP) |
| F2 | F2-R1…R15 | #0, #3, #5, #6, #7, #11 | M1 |
| F3 | F3-R1…R9 | #9, #12 | M1 (rules), M3 (HTTP, tenants) |
| F4 | F4-R1…R6 | #1, #4, #8 | M2 (DB), M3 |
| F5 | F5-R1…R4 | #4, #12 | M2 |
| F6 | F6-R1…R6 | — | M3, M4 |
| F7 | F7-R1…R6 | — | M3 |
| F13 | F13-R1…R7 | — | M0 (R3), M2–M4 |
