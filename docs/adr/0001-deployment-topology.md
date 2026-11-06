# ADR 0001: One image, three roles, one container on the free tier

**Status:** accepted · **Date:** 2026-11-06

## Context
The service takes webhooks, and for async tenants processes them later with per-tenant worker pools. The exercise
asks for a free-tier deployment (Render) that works end to end, and expects us to show how it runs in production.
Render's free plan gives one web service and a small Postgres; background worker services and extra instances are paid.

## Decision
Build **one image** that runs in one of three roles (`ROLE`):

| Role | Does | Serves |
|---|---|---|
| `all` (default) | webhooks, console, workers | everything |
| `api` | webhooks, console | API, `/admin`, `/metrics`, probes |
| `worker` | per-tenant worker pools | `/healthz`, `/readyz`, `/metrics` only |

**On Render's free tier we deploy `ROLE=all`: one container.** Workers are goroutines inside it. In production the
same image runs as an `api` service plus a separately scaled `worker` service. `docker-compose.split.yml` runs that
layout locally (`make up-split`: one `api` and two `worker` containers on one database).

## Why this is safe with many containers
- The queue is Postgres (`ingest_jobs`), written in the same transaction as the raw payload, so nothing acknowledged is lost when any process dies.
- Workers claim with `FOR UPDATE SKIP LOCKED`: no job reaches two workers. A job's outcome and its removal from the queue commit together.
- A worker that dies mid-job leaves a claimed job; the rescue loop in every process returns it to the queue after `StuckAfter`.
- Each process reports a heartbeat per tenant pool, so the console shows every container and its workers.
- Rules and live settings are read from Postgres by every process, so a console change reaches all of them.

## Verified
Locally with `make up-split`: 800 payloads sent to the `api` container were all acknowledged and processed, split between the two
worker containers (413 and 387). Stopping one worker container mid-run lost nothing: the other drained the queue (0 missing, 0 left).

## Consequences
- On the free tier one container restart or idle sleep stops the API and the workers together. Queued jobs wait in Postgres and resume on wake.
- Per-tenant worker totals are `max` times the number of worker containers; database connections are budgeted per container (`DB_MAX_CONNS`).
- The rate limiter is in memory per `api` container, so with several API instances the effective limit per tenant is multiplied. A shared limiter (for example Postgres or Redis) is the next step if that matters.
- Not built: a worker container dedicated to chosen tenants (it would need a `WORKER_TENANTS` filter).
