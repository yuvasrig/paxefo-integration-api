# PAXAFE Tive Integration API

Receives Tive webhook payloads, transforms them to PAXAFE sensor and location records, and stores them in Postgres. Raw payloads are stored first and never changed; everything else (validation, transform, quality flags) is replayable.

**Live:** Integration API admin console https://paxafe-api.onrender.com/admin/ (the live app is the operator's admin view; sign in with the admin token) · API documentation https://paxafe-api.onrender.com/docs · mock sender https://paxafe-sender.onrender.com · see [SUBMISSION.md](SUBMISSION.md) for how to verify it.
The companion repository is the [mock Tive sender](https://github.com/yuvasrig/paxafe-mock-sender).

## Quick start (local)

```bash
make up                      # Postgres + API on http://localhost:8080 (docker compose)
docker compose exec api /api seed      # creates tenants and prints their API keys once
scripts/smoke.sh http://localhost:8080 <reviewer-key>   # posts the 13 provided samples, checks each result
```
Admin console: http://localhost:8080/admin (token `local-admin-token` in `docker-compose.yml`). OpenAPI docs: `/docs`. Tests: `go test ./...` (needs Docker for the database tests), lint: `golangci-lint run`.

## What it does

`POST /api/webhook/tive` with an `x-api-key` header (the key identifies the tenant):

1. **Authenticate and store.** The raw JSON is stored untouched in `raw_events` (immutable).
2. **Process.** Rules clean the payload (sentinel temperatures, numeric strings, seconds timestamps, ranges, device lookup by name), then the transform maps it to PAXAFE `sensor` and `location` records, validated against the PAXAFE JSON schemas. A bad field is nulled and flagged; the event is only rejected when there is no timestamp or no usable data.
3. **Store and answer.** Records are stored with an outcome (`accepted`, `flagged`, `rejected`, `duplicate`, `conflict`). Idempotency key: `(tenant, device IMEI, timestamp)`.
4. **Sync or async.** A sync tenant is processed before the answer (200). An async tenant gets `202` and its event is processed by that tenant's worker pool from a Postgres queue.

Read side: `GET /api/v1/events/{id}`, `GET /api/v1/rules`, `/healthz`, `/readyz`, `/metrics`, `/docs`.

## Architecture

```
Tive ──▶ webhook ──▶ raw_events (immutable) ──▶ ingest_jobs ──▶ worker pool (per tenant)
                │                                                    │
                └── sync tenants: process inline ───────────────────┤
                                                                     ▼
                                         event_outcomes · sensor/location records · reading_conflicts
```
Pooled multi-tenancy with Postgres row-level security; one binary with `ROLE=all|api|worker`; stdlib `net/http`, `pgx/v5`, `goose`, `slog`. Pure functions for parse, rules, transform and validation (no I/O, enforced by a purity test). Details: [DESIGN.md](DESIGN.md), [specs/001-integration-api/](specs/001-integration-api), decisions in [docs/adr/](docs/adr/README.md).

## Compare what was received with what was produced

The console's **Compare** page (`/admin/compare`) sets the Tive payload a device sent beside the PAXAFE payload the API made, per tenant and device and for a time window: temperature and location charts (Tive payload as a line, PAXAFE payload as dots, out-of-range Tive values as red rings), counts of events changed by rules, and a list of what changed. Each event page has a field-by-field table of Tive payload versus PAXAFE payload values with the flag and rule behind each change. The mock sender's report does the same from the sender's side.

## Built versus designed

Built: everything above, the rules framework, per-tenant settings and overrides, async workers with backpressure, conflict handling and replay, the admin console with alerts, the Render deployment, performance tests. Designed but not built (see ADRs and DESIGN.md): the in-service reconciler, OpenTelemetry tracing, Prometheus alert rules, the local k3d/KEDA scaling demo, Timescale, retention, webhook signature verification and source-IP allow-listing.

Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (components, flows, data model, Tive to PAXAFE mapping table).

How to use the apps and what each tab does (with screenshots): [docs/USER_GUIDE.md](docs/USER_GUIDE.md).

Operations: [docs/RUNBOOK.md](docs/RUNBOOK.md) · deployment: [docs/DEPLOY.md](docs/DEPLOY.md) · load results: [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## Rules framework (configurable on the fly)

The checks and fixes applied to every payload are **configurable rules**, not hard-coded logic. The rules engine is a small framework (`internal/rules`); the rules themselves are data, and a set of **default rules ships with the service** (`internal/rules/default_rules.yaml`, also in [specs/001-integration-api/contracts/rules.yaml](specs/001-integration-api/contracts/rules.yaml)). A rule says: *for this field, when this condition holds, take this action*.

```yaml
- { id: HUMIDITY-RANGE, code: OUT_OF_RANGE, field: Humidity.Percentage, when: { outside: [0, 100] }, action: nullify, severity: warn }
```

- **Defaults:** coerce numeric strings, resolve a device by name, scale seconds timestamps, reject timestamps in the future, null sentinel temperatures, range-check humidity, light, battery and coordinates, flag identity mismatches (15 rules).
- **Add your own:** a rule's `code` is free text, so a new rule can raise a new flag. Conditions and strategies are listed in `internal/rules/ruleset.go` and `strategies.go`; `action: ignore` switches a rule off; `overrides:` make a rule behave differently for one tenant.
- **On the fly:** no restart and no deploy.

```bash
api rules default > my.yaml                 # start from the shipped rules
# edit my.yaml, and change `version:` (it is recorded on every event)
api rules validate --file my.yaml           # check it; nothing is published
api rules set --file my.yaml                # publish; every running instance adopts it within ~10 s
api rules show                              # what is published
api rules history                           # earlier versions
api rules reset                             # back to the shipped defaults
```
Running instances poll every `RULES_REFRESH_INTERVAL` (default 10 s; `0` turns polling off). `GET /api/v1/rules` shows the ruleset in force, with its `version`, `source` (`default`, `file` or `database`) and `loaded_at`.
- **Safe by design:** a ruleset is validated before it is published and again when an instance loads it. A bad one is refused, and one that somehow fails to load never replaces the working one: the instance logs it and keeps serving. Each event records the `rules_version` that processed it. Tenant API keys cannot publish rules: it is an operator action.
- **Not rules:** auth, size limits, duplicate/conflict handling, and a few checks that need context a rule cannot express (the probe fallback, the "no usable data" and "no timestamp" rejections, the accuracy category). Those are code, with their own settings.

## Fixing rules or conflicts and reprocessing what was already received

Because the raw payload is stored untouched, a fix can be applied to data that is already in. After changing the rules (`api rules set`) or a tenant's conflict policy (`api tenants set-conflict`), an operator reprocesses a window:

```bash
api replay --tenant reviewer --from 2026-11-01 --to 2026-11-08                    # dry run (the default): shows what would change
api replay --tenant reviewer --from 2026-11-01 --to 2026-11-08 --device A571992    # only one device (IMEI or name)
api replay --all-tenants --from 2026-11-01 --to 2026-11-08 --status conflict --apply
api replay runs                                                                    # audit of applied replays
```
- **Scope:** one tenant or `--all-tenants` (each isolated); a window on received time (default) or the reading's own time (`--time event`); optional `--device`, `--account` (the payload's `AccountId`), `--status` and `--limit`.
- **Safe:** it is a dry run unless you add `--apply`, and the dry run is exact (it runs inside a rolled-back transaction). Applying archives each event's previous outcome in `event_outcome_history` and records the run in `replay_runs`. Raw payloads are never changed. An event that fails keeps its old result and the run carries on.
- **Order and conflicts:** events are processed oldest received first, so under `keep_last` the last received wins, and an older event never overwrites a later one. Replaying the same window twice changes nothing the second time.
- **Operator only:** tenant API keys cannot start a replay. The API's `POST /api/v1/events/{id}/replay` still replays one failed or rejected event.

## Performance

Measured on the local stack with the mock sender (`docs/PERFORMANCE.md`, reproducible with `scripts/perf/`): about 1,200 events/s sustained for an async tenant and 1,450/s for a sync one on a laptop, p95 under 10 ms to the acknowledgement at realistic rates, nothing acknowledged ever lost, including with workers killed, the API restarted and a flooding neighbour. The report lists what broke along the way and what to size (notably `QUEUE_MAX_DEPTH`).

## Async mode and workers

By default a webhook is processed before it is answered (sync). A tenant can instead be **async**: the webhook stores the raw payload and queues a job, answers `202 {"status":"queued"}`, and a worker pool processes it. Choose it per tenant (`tenants.config.ingest_mode`) or for everyone with `INGEST_MODE=async`.

- **Workers are per tenant.** Every async tenant has its own worker pool in each process, sized from that tenant's own backlog between its own minimum and maximum (a per-tenant setting: `api tenants set-workers`, or the Config page; otherwise the global `WORKERS_MIN` and `WORKERS_MAX`). A tenant's workers only take that tenant's jobs, so one tenant's burst cannot slow another. Pools grow at once and shrink slowly; sync tenants run none. `WORKERS_TOTAL_MAX` (editable live in the console) is a hard cap on the workers one process runs across all tenants, enforced at once, so many tenants cannot exhaust the database connections. Idle async tenants each keep `WORKERS_MIN` workers polling, so set `WORKERS_MIN=0` with many tenants. Workers live inside the API process (`ROLE=all`, the default) or run in separate containers (`ROLE=worker`; `ROLE=api` only accepts webhooks): try `make up-split` for one API and two worker containers (see `docs/adr/0001-deployment-topology.md`).
- **Safe by construction:** the job is created in the same transaction as the raw payload, claimed with `FOR UPDATE SKIP LOCKED` (never twice), removed in the transaction that writes the outcome, retried with backoff, and marked `dead` (replayable) after `MAX_ATTEMPTS`. A worker that dies mid-job loses nothing: its jobs return to the queue.
- **Backpressure:** a tenant with `QUEUE_MAX_DEPTH` jobs waiting gets `503` + `Retry-After` and nothing is stored, so a sender knows to slow down.
- **Runtime settings:** queue limit, worker bounds, lag target and attempts can be changed without a restart (see the admin console); the rest of the configuration needs a restart.

## Admin console

`/admin` is a small operator console. Set `ADMIN_TOKEN` to enable it (without the token the console does not exist); sign in with that token. It is server-rendered, uses no external scripts, and every action is also available as an `api` command.

- **Dashboard:** per tenant, events arriving per minute, devices active in the last minute, completed per minute, queue and oldest wait, latency, plus the workers running and whether they are keeping up.
- **Tenants:** add a tenant (this also issues its API key and shows the webhook URL and a ready-to-paste Tive setup, once), issue or revoke keys, set a tenant's ingest mode and conflict policy.
- **Payloads:** the PAXAFE sensor and location records the API produced, per tenant. Filter by **time** (when the API received it, or the reading's own time; presets from 15 minutes to 7 days, or a custom UTC window), device (name or IMEI), status, and "only events that produced a PAXAFE record"; page through newest first. Open an event to see its PAXAFE payload beside the Tive payload exactly as received, the quality flags and why anything was rejected. The selection can be downloaded as JSON (`/admin/payloads.json?tenant=...`).
- **Conflicts:** per tenant, each conflicting pair with the fields that really differ and what the tenant's policy did; change the policy or rules, then **preview** (dry run) and **apply** a replay of the conflicting events from the last N hours (same safe replay as `api replay`).
- **Rules:** every rule is shown as a plain-English sentence ("When Humidity (%) is outside 0 to 100, clear the value and flag it OUT_OF_RANGE (warning)"). Switch a rule on or off, edit its thresholds, action, flag code and severity in a form, reorder, delete, or **add a rule from dropdowns** (field, check, action) with no YAML. Pick a tenant to **override a rule for that tenant only**. Each change is checked (safety guardrails included), given a new version automatically and goes live on every instance within seconds. The raw YAML editor, validate/publish, history and reset remain under *Advanced*.
- **Config:** settings are **global, and each tenant can override them** (pick the scope at the top): queue limit, worker min/max, jobs per worker, lag target, retry attempts and the reject-rate alert. A tenant's own value wins over the global one; a tenant that sets nothing follows the global value, including when it changes. Only the process-wide worker cap is global only. Restart-required settings are shown read-only; secrets are never shown. The same from the CLI: `api tenants set-setting --tenant T --key K --value V` (or `--clear`), `api tenants show`.
- **Alerts:** rules over the live figures show as banners on the Dashboard and Monitoring pages: dead events, jobs waiting with no worker, workers behind the lag target, a queue near its limit (80%), and a high reject rate (live setting `alert_reject_percent`). The **Notifications** tab keeps the history of every alert (when it started, when it cleared, what it said). Set `ALERT_WEBHOOK_URL` (a Slack incoming webhook works) and each alert is also posted once when it starts and once when it clears.
- **Monitoring:** live rates and latency, recent rejects and events, workers, and a prediction of whether the workers are up to date or how long the current backlog needs to clear (from the last minute's arrival and completion rates), with links to `/metrics`, your logs (`LOGS_URL`) and dashboard (`DASHBOARD_URL`).

Safety: the session cookie is HttpOnly and SameSite=Strict, state-changing requests must come from the same origin, ten wrong tokens lock sign-in for a minute, and pages are served with a strict content-security policy. Use it over HTTPS (Render provides it). For scripts, send the token in an `X-Admin-Token` header.

## Assumptions

- **Rate limiting was deliberately skipped, because Tive cannot be throttled.** From Tive's developer documentation: a webhook should be answered with `2xx` quickly; Tive retries a `5xx` once and then gives up, and it does not honour `429` or `Retry-After`. A request refused for rate would be lost on Tive's side (recoverable only through Tive's pull API, which is out of scope), so refusing traffic to protect the service trades an availability problem for data loss. Instead the webhook is protected by raw-first storage, async mode (`202`, bursts wait in the queue), a per-tenant queue limit (a `503` only as a last resort: size `QUEUE_MAX_DEPTH` so it is not reached), per-tenant worker pools, a hard cap on total workers, and alerts. The read and replay endpoints are not limited either; put a gateway in front if the service is exposed to untrusted callers. See `docs/adr/0002-no-rate-limiting.md`.
- **Not built, documented by Tive:** verifying the `x-tive-signature` header (HMAC-SHA256 over `timestamp.body` with the webhook's secret) and allow-listing Tive's source address `34.195.102.163`. Use HTTPS (Render provides it).

- **Addresses are split structurally, not validated against USPS rules.** From Tive's `FormattedAddress` we take everything before the city as `street`, and parse only the `<City>, <ST> <ZIP>, <Country>` tail (two capital letters and a 5-digit or ZIP+4 code); we do not check state codes, street suffixes (USPS Pub 28), secondary units or whether a ZIP exists, and non-US addresses only parse as `<city>, <country>`. `full_address` is always kept verbatim.
- **`EntryTimeEpoch` is assumed to be the correct event time; `EntryTimeUtc` is ignored.** The two fields should name the same instant, but they can disagree (sample 11 is 5 hours apart, which looks like an upstream timezone error). We trust the epoch because it is an unambiguous integer, it becomes PAXAFE's `timestamp`, and it forms the duplicate key. A disagreement is not flagged, so a wrong epoch would be stored without warning.
- **Every Tive record is `provider: "Tive"` and `type: "Active"`.** Both are constants: the payload carries no field for either. `Active` follows the PAXAFE schema's "real-time trackers" and its example for Tive, and Tive trackers report over cellular. We do not infer the type from the payload, the report delay or the IMEI's model code (TAC). If a Tive logger that uploads later should be `Passive`, that needs a per-device setting or a TAC list from PAXAFE; neither is built.
- **Conflicting duplicates are handled per tenant; the default keeps the first reading.** A reading is identified by `(tenant, device, EntryTimeEpoch)`. The same payload again is a `duplicate` (HTTP 200, nothing written). Different content for the same key is a `conflict`, resolved by the tenant's setting in `tenants.config.conflict`: `keep_first` (default; the first reading stays, the new version is saved in `reading_conflicts`), `keep_last` (the new version replaces the stored reading; the old event stays archived) or `reject` (nothing is written); `http_status` (200 or 409) is set separately. Set it with `api tenants set-conflict`. Whether Tive ever legitimately corrects a reading is unknown; if it does, a tenant should choose `keep_last`.
- **Temperatures are stored as sent and never judged.** There is no excursion (out-of-range) flag: PAXAFE's sensor and location schemas have no field for one, and the right band depends on the product, which the payload does not tell us (the shipment object carries no temperature range). Downstream systems decide what a temperature means.

## Out of scope

- **Tive's pull API (`api.tive.com/public/v3`).** This service ingests the webhook push stream only. A backfill job that polls Tive after an outage would feed the same ingest endpoint, since the raw-first pipeline and `(tenant, device, timestamp)` idempotency already make replays safe. Not implemented.
