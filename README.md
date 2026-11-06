# PAXAFE Tive Integration API

Receives Tive webhook payloads, transforms them to PAXAFE sensor and location records, and stores them in Postgres.
This README is a stub; the full version (quick start, architecture, decisions, built vs designed) is written in M7.
For now see [DESIGN.md](DESIGN.md), [specs/001-integration-api/](specs/001-integration-api), and [docs/DEPLOY.md](docs/DEPLOY.md).

## Assumptions

- **Addresses are split structurally, not validated against USPS rules.** From Tive's `FormattedAddress` we take everything before the city as `street`, and parse only the `<City>, <ST> <ZIP>, <Country>` tail (two capital letters and a 5-digit or ZIP+4 code); we do not check state codes, street suffixes (USPS Pub 28), secondary units or whether a ZIP exists, and non-US addresses only parse as `<city>, <country>`. `full_address` is always kept verbatim.
- **`EntryTimeEpoch` is assumed to be the correct event time; `EntryTimeUtc` is ignored.** The two fields should name the same instant, but they can disagree (sample 11 is 5 hours apart, which looks like an upstream timezone error). We trust the epoch because it is an unambiguous integer, it becomes PAXAFE's `timestamp`, and it forms the duplicate key. A disagreement is not flagged, so a wrong epoch would be stored without warning.
- **Every Tive record is `provider: "Tive"` and `type: "Active"`.** Both are constants: the payload carries no field for either. `Active` follows the PAXAFE schema's "real-time trackers" and its example for Tive, and Tive trackers report over cellular. We do not infer the type from the payload, the report delay or the IMEI's model code (TAC). If a Tive logger that uploads later should be `Passive`, that needs a per-device setting or a TAC list from PAXAFE; neither is built.

## Out of scope

- **Tive's pull API (`api.tive.com/public/v3`).** This service ingests the webhook push stream only. A backfill job that polls Tive after an outage would feed the same ingest endpoint, since the raw-first pipeline and `(tenant, device, timestamp)` idempotency already make replays safe. Not implemented.
