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

## Assumptions

- **Addresses are split structurally, not validated against USPS rules.** From Tive's `FormattedAddress` we take everything before the city as `street`, and parse only the `<City>, <ST> <ZIP>, <Country>` tail (two capital letters and a 5-digit or ZIP+4 code); we do not check state codes, street suffixes (USPS Pub 28), secondary units or whether a ZIP exists, and non-US addresses only parse as `<city>, <country>`. `full_address` is always kept verbatim.
- **`EntryTimeEpoch` is assumed to be the correct event time; `EntryTimeUtc` is ignored.** The two fields should name the same instant, but they can disagree (sample 11 is 5 hours apart, which looks like an upstream timezone error). We trust the epoch because it is an unambiguous integer, it becomes PAXAFE's `timestamp`, and it forms the duplicate key. A disagreement is not flagged, so a wrong epoch would be stored without warning.
- **Every Tive record is `provider: "Tive"` and `type: "Active"`.** Both are constants: the payload carries no field for either. `Active` follows the PAXAFE schema's "real-time trackers" and its example for Tive, and Tive trackers report over cellular. We do not infer the type from the payload, the report delay or the IMEI's model code (TAC). If a Tive logger that uploads later should be `Passive`, that needs a per-device setting or a TAC list from PAXAFE; neither is built.
- **Conflicting duplicates are handled per tenant; the default keeps the first reading.** A reading is identified by `(tenant, device, EntryTimeEpoch)`. The same payload again is a `duplicate` (HTTP 200, nothing written). Different content for the same key is a `conflict`, resolved by the tenant's setting in `tenants.config.conflict`: `keep_first` (default; the first reading stays, the new version is saved in `reading_conflicts`), `keep_last` (the new version replaces the stored reading; the old event stays archived) or `reject` (nothing is written); `http_status` (200 or 409) is set separately. Set it with `api tenants set-conflict`. Whether Tive ever legitimately corrects a reading is unknown; if it does, a tenant should choose `keep_last`.
- **Temperatures are stored as sent and never judged.** There is no excursion (out-of-range) flag: PAXAFE's sensor and location schemas have no field for one, and the right band depends on the product, which the payload does not tell us (the shipment object carries no temperature range). Downstream systems decide what a temperature means.

## Out of scope

- **Tive's pull API (`api.tive.com/public/v3`).** This service ingests the webhook push stream only. A backfill job that polls Tive after an outage would feed the same ingest endpoint, since the raw-first pipeline and `(tenant, device, timestamp)` idempotency already make replays safe. Not implemented.
