# ADR 0019: Insert-only ingest ledger

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 17

## Decision
Insert-only ingest ledger.

## Rejected
A per-minute counter upsert.

## Why
Avoids a hot row lock that would serialise ingest.

## Consequences
Table created in the initial migration; used for completeness figures.
