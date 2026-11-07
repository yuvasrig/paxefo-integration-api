# ADR 0008: Postgres (Timescale as a later toggle)

**Status:** accepted (built (Postgres); Timescale not built) · **Source:** DESIGN.md §20, decision 6

## Decision
Postgres (Timescale as a later toggle).

## Rejected
InfluxDB, DynamoDB, MongoDB.

## Why
Database-enforced idempotency, transactions and JSONB; an upgrade path that needs no rewrite.

## Consequences
The idempotency key `(tenant_id, device_imei, ts)` is a unique constraint. Retention is not built; storage is about 2.6 to 4.6 KB per event.
