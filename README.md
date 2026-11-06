# PAXAFE Tive Integration API

Receives Tive webhook payloads, transforms them to PAXAFE sensor and location records, and stores them in Postgres.
This README is a stub; the full version (quick start, architecture, decisions, built vs designed) is written in M7.
For now see [DESIGN.md](DESIGN.md), [specs/001-integration-api/](specs/001-integration-api), and [docs/DEPLOY.md](docs/DEPLOY.md).

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
- **Not rules:** auth, size and rate limits, duplicate/conflict handling, and a few checks that need context a rule cannot express (the probe fallback, the "no usable data" and "no timestamp" rejections, the accuracy category). Those are code, with their own settings.

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

## Async mode and workers

By default a webhook is processed before it is answered (sync). A tenant can instead be **async**: the webhook stores the raw payload and queues a job, answers `202 {"status":"queued"}`, and a worker pool processes it. Choose it per tenant (`tenants.config.ingest_mode`) or for everyone with `INGEST_MODE=async`.

- **Workers are per tenant.** Every async tenant has its own worker pool in each process, sized from that tenant's own backlog between its own minimum and maximum (`api tenants set-workers`, or the Tenants page; otherwise `WORKERS_MIN` and `WORKERS_MAX`). A tenant's workers only take that tenant's jobs, so one tenant's burst cannot slow another. Pools grow at once and shrink slowly; sync tenants run none. `WORKERS_TOTAL_MAX` (editable live in the console) is a hard cap on the workers one process runs across all tenants, enforced at once, so many tenants cannot exhaust the database connections. Idle async tenants each keep `WORKERS_MIN` workers polling, so set `WORKERS_MIN=0` with many tenants. Workers live inside the API process (`ROLE=all`, the default) or run in separate containers (`ROLE=worker`; `ROLE=api` only accepts webhooks): try `make up-split` for one API and two worker containers (see `docs/adr/0001-deployment-topology.md`).
- **Safe by construction:** the job is created in the same transaction as the raw payload, claimed with `FOR UPDATE SKIP LOCKED` (never twice), removed in the transaction that writes the outcome, retried with backoff, and marked `dead` (replayable) after `MAX_ATTEMPTS`. A worker that dies mid-job loses nothing: its jobs return to the queue.
- **Backpressure:** a tenant with `QUEUE_MAX_DEPTH` jobs waiting gets `503` + `Retry-After` and nothing is stored, so a sender knows to slow down.
- **Runtime settings:** rate limit, queue limit, worker bounds, lag target and attempts can be changed without a restart (see the admin console); the rest of the configuration needs a restart.

## Admin console

`/admin` is a small operator console. Set `ADMIN_TOKEN` to enable it (without the token the console does not exist); sign in with that token. It is server-rendered, uses no external scripts, and every action is also available as an `api` command.

- **Dashboard:** per tenant, events arriving per minute, devices active in the last minute, completed per minute, queue and oldest wait, latency, plus the workers running and whether they are keeping up.
- **Tenants:** add a tenant (this also issues its API key and shows the webhook URL and a ready-to-paste Tive setup, once), issue or revoke keys, set a tenant's ingest mode and conflict policy.
- **Conflicts:** per tenant, each conflicting pair with the fields that really differ and what the tenant's policy did; change the policy or rules, then **preview** (dry run) and **apply** a replay of the conflicting events from the last N hours (same safe replay as `api replay`).
- **Rules:** view the rules in force, edit, validate, publish (live on every instance within seconds) and reset; history lets you load an older version back into the editor.
- **Config:** every setting, split into live ones you can edit here (rate limit, queue limit, worker bounds, lag target, attempts) and restart-required ones shown read-only. Secrets are never shown.
- **Monitoring:** live rates and latency, recent rejects and events, workers, and a prediction of whether the workers are up to date or how long the current backlog needs to clear (from the last minute's arrival and completion rates), with links to `/metrics`, your logs (`LOGS_URL`) and dashboard (`DASHBOARD_URL`).

Safety: the session cookie is HttpOnly and SameSite=Strict, state-changing requests must come from the same origin, ten wrong tokens lock sign-in for a minute, and pages are served with a strict content-security policy. Use it over HTTPS (Render provides it). For scripts, send the token in an `X-Admin-Token` header.

## Assumptions

- **Addresses are split structurally, not validated against USPS rules.** From Tive's `FormattedAddress` we take everything before the city as `street`, and parse only the `<City>, <ST> <ZIP>, <Country>` tail (two capital letters and a 5-digit or ZIP+4 code); we do not check state codes, street suffixes (USPS Pub 28), secondary units or whether a ZIP exists, and non-US addresses only parse as `<city>, <country>`. `full_address` is always kept verbatim.
- **`EntryTimeEpoch` is assumed to be the correct event time; `EntryTimeUtc` is ignored.** The two fields should name the same instant, but they can disagree (sample 11 is 5 hours apart, which looks like an upstream timezone error). We trust the epoch because it is an unambiguous integer, it becomes PAXAFE's `timestamp`, and it forms the duplicate key. A disagreement is not flagged, so a wrong epoch would be stored without warning.
- **Every Tive record is `provider: "Tive"` and `type: "Active"`.** Both are constants: the payload carries no field for either. `Active` follows the PAXAFE schema's "real-time trackers" and its example for Tive, and Tive trackers report over cellular. We do not infer the type from the payload, the report delay or the IMEI's model code (TAC). If a Tive logger that uploads later should be `Passive`, that needs a per-device setting or a TAC list from PAXAFE; neither is built.
- **Conflicting duplicates are handled per tenant; the default keeps the first reading.** A reading is identified by `(tenant, device, EntryTimeEpoch)`. The same payload again is a `duplicate` (HTTP 200, nothing written). Different content for the same key is a `conflict`, resolved by the tenant's setting in `tenants.config.conflict`: `keep_first` (default; the first reading stays, the new version is saved in `reading_conflicts`), `keep_last` (the new version replaces the stored reading; the old event stays archived) or `reject` (nothing is written); `http_status` (200 or 409) is set separately. Set it with `api tenants set-conflict`. Whether Tive ever legitimately corrects a reading is unknown; if it does, a tenant should choose `keep_last`.
- **Temperatures are stored as sent and never judged.** There is no excursion (out-of-range) flag: PAXAFE's sensor and location schemas have no field for one, and the right band depends on the product, which the payload does not tell us (the shipment object carries no temperature range). Downstream systems decide what a temperature means.

## Out of scope

- **Tive's pull API (`api.tive.com/public/v3`).** This service ingests the webhook push stream only. A backfill job that polls Tive after an outage would feed the same ingest endpoint, since the raw-first pipeline and `(tenant, device, timestamp)` idempotency already make replays safe. Not implemented.
