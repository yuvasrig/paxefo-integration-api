# PAXAFE Tive Integration API

Receives Tive webhook payloads, transforms them to PAXAFE sensor and location records, and stores them in Postgres.
This README is a stub; the full version (quick start, architecture, decisions, built vs designed) is written in M7.
For now see [DESIGN.md](DESIGN.md), [specs/001-integration-api/](specs/001-integration-api), and [docs/DEPLOY.md](docs/DEPLOY.md).

## Assumptions

- **Addresses are split structurally, not validated against USPS rules.** From Tive's `FormattedAddress` we take everything before the city as `street`, and parse only the `<City>, <ST> <ZIP>, <Country>` tail (two capital letters and a 5-digit or ZIP+4 code); we do not check state codes, street suffixes (USPS Pub 28), secondary units or whether a ZIP exists, and non-US addresses only parse as `<city>, <country>`. `full_address` is always kept verbatim.
- **`EntryTimeEpoch` is assumed to be the correct event time; `EntryTimeUtc` is ignored.** The two fields should name the same instant, but they can disagree (sample 11 is 5 hours apart, which looks like an upstream timezone error). We trust the epoch because it is an unambiguous integer, it becomes PAXAFE's `timestamp`, and it forms the duplicate key. A disagreement is not flagged, so a wrong epoch would be stored without warning.
