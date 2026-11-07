# ADR 0006: Separate immutable `raw_events`, `event_outcomes` and `ingest_jobs`

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 4

## Decision
Separate immutable `raw_events`, `event_outcomes` and `ingest_jobs`.

## Rejected
A status column on the raw table.

## Why
The raw archive is never updated (ALCOA+) and does not bloat from status churn.

## Consequences
Outcomes and their history are separate tables; replay archives the previous outcome in `event_outcome_history`.
