# ADR 0005: Postgres queue (`ingest_jobs`, `FOR UPDATE SKIP LOCKED`)

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 3

## Decision
Postgres queue (`ingest_jobs`, `FOR UPDATE SKIP LOCKED`).

## Rejected
Kafka/SQS/Redis now.

## Why
The job is created in the same transaction as the raw payload, so nothing acknowledged is lost; it is durable and free.

## Consequences
Kafka stays possible behind a queue interface later. Measured about 1,200 events/s async on a laptop (`docs/PERFORMANCE.md`).
