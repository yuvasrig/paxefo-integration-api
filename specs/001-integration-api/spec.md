# Spec 001 — Integration API (M0–M7 scope)

Source of truth: DESIGN.md. This spec states testable requirements in EARS form. Every requirement has an ID; tests and code comments reference it. Sample numbers (`#0`–`#12`) are the index in `assignment/sample-tive-payloads.json`; expected results are in `expected/`.

EARS patterns: `WHEN <event>, THE SYSTEM SHALL <response>` · `IF <condition>, THEN THE SYSTEM SHALL …` · `THE SYSTEM SHALL …` (always).

Test precondition (unless stated): an empty database, one tenant, samples sent in order 0 → 12 with that tenant's API key. The files in `expected/` are the golden outputs.

Out of scope for this spec: async ingest, queue, workers, alerts, reconciler, Timescale, the `IMPOSSIBLE_TRAVEL` check, and any temperature-band (excursion) check: a temperature is stored as sent.

---

## F1 — Ingest, happy path

- **F1-R1** WHEN a valid POST arrives at `/api/webhook/tive` (the path the exercise requires) or its alias `/api/v1/webhook/tive` with a valid API key, THE SYSTEM SHALL process it identically on both paths.
- **F1-R2** WHEN a payload is processed, THE SYSTEM SHALL commit the raw payload and one ledger row before returning any 2xx.
- **F1-R3** WHEN a payload has a usable temperature or other sensor value, THE SYSTEM SHALL emit a PAXAFE sensor record per `contracts/mapping.md`.
- **F1-R4** WHEN a payload has valid coordinates, THE SYSTEM SHALL emit a PAXAFE location record per `contracts/mapping.md`.
- **F1-R5** THE SYSTEM SHALL round only when emitting PAXAFE JSON (half away from zero) and store full precision.
- **F1-R6** THE SYSTEM SHALL validate every emitted record against the PAXAFE sensor/location JSON Schemas.
- **F1-R7** WHEN processing completes with no flags at all, THE SYSTEM SHALL respond 200 with `status: accepted`, the PAXAFE output, empty `errors`, and an `event_id` and `request_id`.
- **F1-R8** THE SYSTEM SHALL accept unknown extra fields and keep them in the raw payload.

**Acceptance (Given/When/Then)**
- *#2:* GIVEN a clean GPS payload, WHEN posted, THEN 200 `accepted`, one sensor and one location record, `location_source` = `GPS`, no flags.
- *#10:* GIVEN a payload with null probe values, null accuracy and null `LocationMethod`, WHEN posted, THEN 200 `accepted` with no flags, `location_accuracy`, category and source are `null`.
- *F1-R1:* GIVEN #2, WHEN posted to both paths on two fresh tenants, THEN the responses are identical apart from ids.

## F2 — Ingest, partial or flagged

- **F2-R1** WHEN `Location.Latitude` ∉ [-90, 90] or `Location.Longitude` ∉ [-180, 180], THE SYSTEM SHALL skip the location record AND add flag `INVALID_COORDINATES` AND still emit the sensor record.
- **F2-R2** WHEN `Temperature.Celsius` or `ProbeTemperature.Celsius` is -32768, 327.67 or 32767, THE SYSTEM SHALL set it to null AND add flag `SENSOR_FAULT`.
- **F2-R3** WHEN `Humidity.Percentage` ∉ [0, 100], `Light.Lux` < 0 or `Battery.Percentage` ∉ [0, 100], THE SYSTEM SHALL set it to null AND add flag `OUT_OF_RANGE`.
- **F2-R4** WHEN a numeric field arrives as a numeric string, THE SYSTEM SHALL parse it AND add flag `COERCED_TYPE` (one per field) AND keep the parsed value.
- **F2-R5** WHEN `EntryTimeEpoch` < 1e12, THE SYSTEM SHALL multiply it by 1000 AND add flag `TIMESTAMP_SECONDS`.
- **F2-R6** THE SYSTEM SHALL ignore `EntryTimeUtc`: `EntryTimeEpoch` is the event time, and a disagreement between the two SHALL NOT raise a flag.
- **F2-R7** WHEN `EntityName` ≠ `DeviceName`, or `Shipment.DeviceId` ≠ `DeviceId`, THE SYSTEM SHALL keep the values as sent AND add flag `IDENTITY_MISMATCH`.
- **F2-R8** WHEN `Temperature.Celsius` is null or nulled by F2-R2 and `ProbeTemperature.Celsius` is usable, THE SYSTEM SHALL use the probe value AND set `temperature_source = probe` AND add flag `TEMPERATURE_FALLBACK`.
- **F2-R9** WHEN `Temperature.Celsius` is null and `Temperature.Fahrenheit` is present, THE SYSTEM SHALL compute Celsius = (F − 32) × 5/9 AND add flag `TEMPERATURE_CONVERTED`.
- **F2-R10** WHEN `Accelerometer.G` is missing and X, Y, Z are present, THE SYSTEM SHALL set magnitude = √(x²+y²+z²).
- **F2-R11** THE SYSTEM SHALL record, for every fix or nullification, the rule ID, the original value and the new value.
- **F2-R12** WHEN any flag is present, whatever its severity, and the event is not rejected, duplicate or conflict, THE SYSTEM SHALL respond 200 with `status: flagged`.
- **F2-R13** WHEN no flag is present, THE SYSTEM SHALL respond `accepted`.
- **F2-R16** THE SYSTEM SHALL NOT judge a temperature against a cold-chain band: a reading outside 2-8 C, with or without a shipment, SHALL be stored and emitted as sent with no flag.
- **F2-R15** WHEN a field that should be a number is a non-numeric string, an object or an array, THE SYSTEM SHALL set it to null AND add flag `INVALID_TYPE`.
- **F2-R14** WHEN `Location.Accuracy.Meters` is present, THE SYSTEM SHALL set the accuracy category High if < 50, Medium if < 500, else Low (thresholds configurable).

**Acceptance**
- *#0:* GIVEN a payload whose `Shipment.DeviceId` differs from `DeviceId`, THEN 200 `flagged` with `IDENTITY_MISMATCH` only (10.08 C raises nothing); sensor temperature 10.08, location category High, source `WiFi`.
- *#3:* GIVEN probe 327.67, THEN `SENSOR_FAULT`, probe nulled, device temperature 4.63 (4.625 rounds half away from zero) used with no fallback flag.
- *#5:* GIVEN epoch 1739561246, THEN `TIMESTAMP_SECONDS`, `timestamp` = 1739561246000.
- *#6:* GIVEN probe -32768 and a `cell` location of 500 m, THEN `SENSOR_FAULT`, `location_source` = `Cellular`, category `Low`, `Shipment` null; the device temperature -18.5 C is stored as sent.
- *#11:* GIVEN `EntryTimeUtc` 14:27:26Z but an epoch for 19:27:26Z, THEN 200 `accepted` with no flags; `timestamp` is the epoch.
- *#7:* GIVEN `Dbm` and `Temperature.*` as strings and a shipment device mismatch, THEN `COERCED_TYPE` for each coerced field, and `IDENTITY_MISMATCH`; temperature 10.5, `cellular_dbm` -100.00.

## F3 — Ingest, rejected

- **F3-R1** IF the body is not valid JSON, THEN THE SYSTEM SHALL store the raw bytes, respond 400 with error `INVALID_JSON` and status `rejected`.
- **F3-R2** IF `DeviceId` is missing or not 15 digits AND `DeviceName` is not found in the tenant's `devices`, THEN THE SYSTEM SHALL reject with 422 `NO_DEVICE_IDENTITY`. THE SYSTEM SHALL NOT resolve identity from `EntityName`.
- **F3-R3** IF `EntryTimeEpoch` is missing or invalid, THEN THE SYSTEM SHALL reject with 422 `NO_TIMESTAMP`.
- **F3-R4** IF the timestamp is more than 5 minutes ahead of server time, THEN THE SYSTEM SHALL reject with 422 `TIMESTAMP_FUTURE`.
- **F3-R5** IF there is no usable temperature AND no valid coordinates, THEN THE SYSTEM SHALL reject with 422 `NO_USABLE_DATA`.
- **F3-R6** THE SYSTEM SHALL take the tenant from the API key only. The payload's `AccountId` SHALL be stored as metadata and never checked: no value, and no missing value, changes the outcome.
- **F3-R7** WHEN an event is rejected, THE SYSTEM SHALL keep its raw payload, record outcome `rejected` with reasons, and return flags raised before the rejection.

**Acceptance**
- *#12:* GIVEN `DeviceName` Ship33CABOL (unknown) and no `DeviceId`, WHEN posted, THEN 422 `rejected`, error `NO_DEVICE_IDENTITY`, flag `IDENTITY_MISMATCH` (EntityName A571992 ≠ DeviceName), and the raw event exists.
- *#9:* GIVEN `AccountId` 999, THEN 200 `accepted` with no flags; the event and its readings belong to the key's tenant like any other.
- *F3-R6:* GIVEN sample 0 with `AccountId` 478, 1001, 999, `"abc"` or none, posted with the reviewer key, THEN the same outcome each time (never a rejection, no flag about accounts) and nothing is written to any other tenant; `AccountId` is kept in the reading's metadata.
- *F3-R1:* GIVEN body `{not json`, THEN 400 and one raw event.
- *F3-R5:* GIVEN no `Temperature.Celsius`, no probe and latitude 95, THEN 422 `NO_USABLE_DATA`.

## F4 — Duplicate and conflict

- **F4-R1** THE SYSTEM SHALL enforce uniqueness of readings on `(tenant_id, device_imei, ts)` in the database.
- **F4-R2** WHEN a payload has the same key and the same canonical-JSON hash as a stored event, THE SYSTEM SHALL respond 200 `duplicate` AND write no new readings.
- **F4-R3** WHEN a payload has the same key but a different hash, THE SYSTEM SHALL apply the tenant's conflict policy (F4-R7), record the version in `reading_conflicts` with its resolution, add flag `DUPLICATE_CONFLICT`, and respond with `status: conflict`. The default policy is `keep_first`: the first readings are kept.
- **F4-R4** WHEN the same key arrives concurrently, THE SYSTEM SHALL end with exactly one reading row.
- **F4-R5** THE SYSTEM SHALL compute the payload hash over canonical JSON (sorted keys, no insignificant whitespace), so key order and whitespace never create a conflict.
- **F4-R7** ("last" means last received) THE SYSTEM SHALL read the conflict policy from the tenant's config (`conflict.policy`), one of: `keep_first` (default: keep the stored readings), `keep_last` (replace the stored readings with the new version in one transaction; the replaced event stays archived and is named in `reading_conflicts.first_raw_event_id`), `reject` (write nothing to the readings). `DUPLICATE_CONFLICT` carries the action `kept`, `replaced` or `rejected` accordingly.
- **F4-R8** THE SYSTEM SHALL read the HTTP status of a conflict response from the tenant's config (`conflict.http_status`, 200 or 409), defaulting to 409 for `reject` and 200 otherwise. The status code is independent of the policy. A duplicate (same hash) is always 200 `duplicate`, whatever the policy.
- **F4-R9** IF a tenant's stored conflict config is missing or invalid, THEN THE SYSTEM SHALL use `keep_first` / 200 and SHALL NOT stop ingesting.
- **F4-R6** WHEN the status of one event satisfies several of rejected / conflict / duplicate / flagged / accepted, THE SYSTEM SHALL report the first in that precedence.

**Acceptance**
- *#1 after #0:* same `(imei, ts)`, different content → 200 `conflict`, flags `INVALID_COORDINATES`, `DUPLICATE_CONFLICT`; stored sensor reading is still #0's (temperature 10.08).
- *Policies:* GIVEN sample 2 stored and the same device and time sent again with a different humidity, THEN `keep_first` leaves humidity 55.3 and answers 200; `keep_last` stores the new humidity (and a new version without valid coordinates removes the old location row); `reject` leaves it unchanged and answers 409 by default; a tenant's `http_status` overrides the code; another tenant is unaffected; a malformed config behaves as `keep_first` / 200.
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
- **F7-R0** (Note) F7 covers replaying one event over the API. Reprocessing many events after a fix is the operator command in F15.
- **F7-R4** WHEN replay is called for an event that is not `failed`, `rejected` or unfinished (that is `accepted`, `flagged`, `duplicate` or `conflict`), THE SYSTEM SHALL respond 409, because replaying it could put an old version over a newer reading under `keep_last`.
- **F7-R5** THE SYSTEM SHALL store `transform_version` and `rules_version` with every outcome.
- **F7-R6** WHEN `GET /api/v1/events/{id}` is called by the owning tenant, THE SYSTEM SHALL return the raw payload, outcome, flags and PAXAFE output.

**Acceptance**
- GIVEN an injected failure in the second transaction, THEN raw event exists, outcome `failed`, HTTP 500; after the fault is removed, replay returns 200 and exactly one reading row exists.
- *#12 replay after A571992-style registration of Ship33CABOL:* outcome changes from `rejected` to `accepted`/`flagged`.

## F13 — Lifecycle

- **F13-R1** WHEN the process starts, THE SYSTEM SHALL load configuration, connect to the database, run migrations, then serve.
- **F13-R2** IF configuration is invalid (including the ruleset in force at startup: `RULES_PATH`, the embedded default, or the one published to the database), THEN THE SYSTEM SHALL exit non-zero before serving.
- **F13-R3** THE SYSTEM SHALL expose `/healthz` (process up) without authentication.
- **F13-R4** THE SYSTEM SHALL expose `/readyz` reporting database reachability, migration version, storage backend and ingest mode.
- **F13-R5** WHEN SIGTERM is received, THE SYSTEM SHALL stop accepting connections, finish in-flight requests within 10 s, and close the database pool.
- **F13-R6** THE SYSTEM SHALL log one structured JSON line per request with request id, tenant id, event id, status and latency, and SHALL NOT log API keys or full payloads.
- **F13-R7** THE SYSTEM SHALL expose Prometheus metrics at `/metrics`.

*M0 delivers F13-R3 only (hello-world `/healthz`); the rest arrive in M2–M4.*

## F14 — Rules framework, configurable on the fly

The rules engine is a framework for configurable rules; a default set ships with the service.

- **F14-R1** THE SYSTEM SHALL express its payload checks and fixes as rules (for a field, when a condition holds, take an action) loaded from a ruleset, and SHALL ship a default ruleset that implements the standard checks. Adding, changing or disabling a rule SHALL need only a ruleset change, not a code change.
- **F14-R2** WHEN an operator publishes a ruleset (`api rules set`), THE SYSTEM SHALL validate it, store it as the active ruleset, and every running instance SHALL adopt it within the refresh interval (`RULES_REFRESH_INTERVAL`, default 10 s) without a restart.
- **F14-R3** THE SYSTEM SHALL process each payload with exactly one ruleset (read once), so a change in the middle of a request never mixes two.
- **F14-R4** IF a ruleset is invalid, THEN THE SYSTEM SHALL refuse to publish it; IF a published ruleset fails to load at runtime or the database cannot be read, THEN THE SYSTEM SHALL keep the working ruleset and continue serving, and SHALL log the problem.
- **F14-R5** WHEN `api rules reset` is run, THE SYSTEM SHALL return running instances to the base ruleset (`RULES_PATH` or the embedded default) within the refresh interval, keeping the history.
- **F14-R6** THE SYSTEM SHALL record the ruleset version on every event outcome (`rules_version`) and SHALL refuse to publish a version that was published before.
- **F14-R7** WHEN a ruleset carries per-tenant `overrides`, THE SYSTEM SHALL apply the override only to that tenant's payloads.
- **F14-R8** THE SYSTEM SHALL report the ruleset in force (`GET /api/v1/rules`: version, source `default`/`file`/`database`, load time) and SHALL NOT let a tenant API key publish rules.

**Acceptance**
- GIVEN the default rules and a payload with humidity 54.8, THEN it carries no humidity flag; WHEN an operator publishes the defaults plus a `HUMIDITY-HIGH` rule, THEN within the refresh interval the next payload is `flagged` with `HUMIDITY_HIGH` and its outcome records the new `rules_version`, while the earlier event keeps its own.
- GIVEN a corrupt published ruleset, THEN the service keeps serving with the working ruleset; and a service started with a corrupt published ruleset exits non-zero.
- GIVEN a ruleset whose override enables a rule for tenant A only, THEN tenant A's payload carries the flag and tenant B's does not.
- GIVEN `api rules reset`, THEN `GET /api/v1/rules` reports the default ruleset again.

## F15 — Reprocess stored payloads after a fix (`api replay`)

An operator can fix the rules or a tenant's conflict policy and then apply the fix to payloads that were already received. This is separate from F7 (replay of one failed or rejected event over the API).

- **F15-R1** WHEN an operator runs `api replay` for one tenant (or every tenant) and a time window, THE SYSTEM SHALL reprocess each matching stored event: re-run its stored raw payload through the ruleset and conflict policy in force now, and write the result.
- **F15-R2** THE SYSTEM SHALL measure the window on the time the payload was received (default) or on the reading's own time (`EntryTimeEpoch`, with seconds understood); `--from` is inclusive and `--to` is exclusive. An event with no readable epoch is not selected by event time.
- **F15-R3** THE SYSTEM SHALL optionally narrow the selection by device (the payload's IMEI or DeviceName), by account (the payload's `AccountId`), and by the event's current status (including `pending`), and SHALL allow a limit on the number of events.
- **F15-R4** THE SYSTEM SHALL run as a dry run unless `--apply` is given: it SHALL report what would change (counts, status transitions such as `conflict -> accepted`, examples) and SHALL write nothing, and the report SHALL be exact, with each event seeing the effect of the events before it.
- **F15-R5** WHEN applying, THE SYSTEM SHALL archive each reprocessed event's previous outcome (`event_outcome_history`), record the run per tenant (`replay_runs`: who, parameters, counts), and SHALL NEVER modify or delete a raw payload.
- **F15-R6** THE SYSTEM SHALL process events oldest received first, and under a `keep_last` conflict policy SHALL NOT let an event overwrite a reading produced by an event that was received later.
- **F15-R7** IF one event cannot be reprocessed, THEN THE SYSTEM SHALL report the error, leave that event's previous outcome and readings unchanged, and continue with the others.
- **F15-R8** THE SYSTEM SHALL keep tenants isolated: a replay of one tenant SHALL read and write only that tenant's rows.
- **F15-R9** THE SYSTEM SHALL be idempotent: applying the same replay again with nothing changed in between SHALL change nothing.
- **F15-R10** THE SYSTEM SHALL offer replay only as an operator command, not to tenant API keys.

**Acceptance**
- GIVEN four stored events and a published rule that flags humidity above 50, THEN a dry run reports 3 changed (2 `accepted -> flagged`) and leaves every outcome, history and audit table as it was; applying it updates those outcomes to the new `rules_version`, archives the old ones, records the run, and a second apply changes nothing.
- GIVEN two conflicting versions stored under `keep_first` and the tenant switched to `keep_last`, THEN replaying the window stores the later version; replaying only the earlier event afterwards does not bring the old version back.
- GIVEN a payload rejected because its device was unknown, and the device registered since, THEN replaying with `--status rejected` turns it into `flagged` with `DEVICE_RESOLVED_BY_NAME`.
- GIVEN events for two devices and two accounts, THEN `--device`, `--account`, `--status` and the two kinds of time window each select exactly the expected events (the window's end is exclusive).
- GIVEN an event whose reprocessing fails, THEN it keeps its previous outcome, the failure is listed, and the other events are reprocessed.

---

## F16 — Async ingest and workers

A tenant can be switched to async ingest. The webhook then only stores the payload and queues it; a pool of workers does the processing. This keeps the acknowledgement fast and lets a burst be absorbed and drained.

- **F16-R1** WHEN a tenant is in async mode (`tenants.config.ingest_mode = "async"`, else `INGEST_MODE`) and a payload is received, THE SYSTEM SHALL commit the raw payload, the ledger row and a queue job in one transaction and answer `202` with status `queued`; nothing else is processed before the answer.
- **F16-R2** THE SYSTEM SHALL process each queued job exactly as sync ingest would, and SHALL remove the job in the same transaction that writes the outcome.
- **F16-R3** WHEN processing a job fails, THE SYSTEM SHALL retry it with growing delay, and after `MAX_ATTEMPTS` SHALL mark the event `dead` (raw payload kept, error recorded, replayable) so it never blocks other jobs.
- **F16-R4** WHEN a tenant already has `QUEUE_MAX_DEPTH` jobs waiting, THE SYSTEM SHALL answer `503` with `Retry-After` and store nothing for that request.
- **F16-R5** THE SYSTEM SHALL give every tenant its own worker pool: a tenant's workers claim only that tenant's jobs, so one tenant's backlog cannot starve or slow another.
- **F16-R6** THE SYSTEM SHALL never hand one job to two workers (`FOR UPDATE SKIP LOCKED`), and SHALL return jobs held by a worker that died to the queue.
- **F16-R7** THE SYSTEM SHALL size each tenant's pool in each process from that tenant's backlog and its oldest job's age, between the tenant's own minimum and maximum (`tenants.config.workers`, else `WORKERS_MIN` and `WORKERS_MAX`), growing at once and shrinking slowly; a sync tenant SHALL have no workers; and workers SHALL finish already claimed jobs on shutdown.
- **F16-R8** THE SYSTEM SHALL report each process's workers through heartbeats, and a replay SHALL leave events that still have a queued job to their worker.
- **F16-R9** THE SYSTEM SHALL allow the queue limit, worker bounds, jobs per worker, lag target, attempts, rate limit and reject-rate alert to be changed at runtime, globally (table `settings`) and per tenant (`tenants.config.settings`), taking effect on every instance within `SETTINGS_REFRESH_INTERVAL`; a tenant's own value SHALL override the global one for that tenant only, a tenant without its own value SHALL follow the global one, and an invalid per-tenant value SHALL be ignored rather than applied.

**Acceptance**
- GIVEN an async tenant, THEN a webhook answers 202 and the event is `accepted` or `flagged` once a worker has run; a sync tenant is unchanged.
- GIVEN a payload that always fails, THEN it ends `dead` after `MAX_ATTEMPTS` and the other jobs finish.
- GIVEN a full queue, THEN the next webhook gets 503 with `Retry-After` and no raw event is stored.
- GIVEN many workers claiming at once, THEN no job is claimed twice; GIVEN a big backlog for one tenant, THEN another tenant's jobs are still processed at once by its own workers and its pool does not grow.

---

## Traceability

| Flow | Requirement IDs | Samples | Milestone |
|---|---|---|---|
| F1 | F1-R1…R8 | #2, #9, #10, #11 | M1 (transform), M3 (HTTP) |
| F2 | F2-R1…R16 | #0, #3, #5, #6, #7 | M1 |
| F3 | F3-R1…R7 | #12 | M1 (rules), M3 (HTTP, tenants) |
| F4 | F4-R1…R9 | #1, #4, #8 | M2 (DB), M3 |
| F5 | F5-R1…R4 | #4, #12 | M2 |
| F6 | F6-R1…R6 | — | M3, M4 |
| F7 | F7-R1…R6 | — | M3 |
| F15 | F15-R1…R10 | — | operator CLI (added later) |
| F14 | F14-R1…R8 | — | M4 (added later) |
| F16 | F16-R1…R9 | — | async mode (queue + workers) |
| F13 | F13-R1…R7 | — | M0 (R3), M2–M4 |
