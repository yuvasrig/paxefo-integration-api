# Performance test

Run on 2026-11-06/07 on localhost (the local Docker stack on a laptop), with the mock sender's load plans. The numbers show
what the design and code can do on that machine; they were not measured on Render. Scripts: `scripts/perf/` (they reproduce every figure here).

## Summary

- **Nothing acknowledged was ever lost.** Across about 1.3 million requests in every stage (steady, burst, flood, restarts, killed
  workers), the number of acknowledged events that were missing or unprocessed was **0**, and the database reconciled exactly
  (stored events = outcomes, queue empty).
- **Capacity on this machine, default settings:** async about **1,200 events/s** sustained (p95 7.4 ms, queue never above 300);
  sync about **1,450 events/s** (it slows down instead of refusing). Realistic Tive traffic is far below this: 1,000 trackers
  reporting every 5 minutes is about 3 events/s, so the headroom is roughly **400×**.
- **Past capacity, async refuses with `503`** once a tenant's queue reaches its limit (5,000 by default). Tive retries a `5xx`
  once and then gives up, so a refused payload is effectively lost. **The queue limit is the setting to size**: raising it to
  100,000 let the same 2,000/s burst be accepted in full (see *Burst*).
- **The test found four real defects**, all fixed (see *Defects found*): the console failed under load, the sender's verification
  could wait for hours when the workers were down, the split layout could run a stale worker image, and the local database had
  no persistent volume.
- Tenant isolation is **good for processing, weaker for shared resources**: a flooding tenant raised another tenant's p95 about 3×
  (12 → 37 ms) without any failure.

## Environment and settings

| | |
|---|---|
| Machine | Docker Desktop VM, 12 CPUs, 8 GB; API and Postgres 16 in containers; the sender on the same host |
| API | one container, `ROLE=all` (webhooks and workers together), no CPU or memory limit |
| Defaults in force | `DB_MAX_CONNS=10`, workers 2–8 per tenant, `JOBS_PER_WORKER=50`, lag target 10 s, queue limit 5,000 per tenant, 5 attempts |
| Tenants | `perf-a`, `perf-b` (async), `perf-s` (sync), each with its own key; payloads from the route and fault scenarios (valid readings, some sentinel values) |
| Measured by | the sender (latency to the acknowledgement, completeness, sent-vs-stored comparison) and a sampler (container CPU/memory, DB connections, queue, oldest wait, workers) |

Latency is the time to the HTTP answer: for an async tenant that is the `202` (payload stored, job queued); the processing
itself is the queue's job and is reported as oldest wait and drain time.

## A. Baseline

10 requests/s for 60 s, async: p50 6.8 ms, p95 9.2 ms, p99 12.5 ms, API CPU 8%, queue never above 2, 0 lost.

## B and K. Step ramp (30 s per step)

**Async tenant** (`202` after the payload and its job are stored):

| Target/s | Delivered/s | p50 ms | p95 ms | p99 ms | Peak queue | Refused | Lost (acknowledged) | API CPU (of 1,200%) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 10 | 6.7 | 8.9 | 10.0 | 2 | 0 | 0 | 13% |
| 25 | 25 | 5.4 | 7.8 | 10.4 | 4 | 0 | 0 | 13% |
| 50 | 50 | 3.0 | 6.2 | 7.7 | 7 | 0 | 0 | 21% |
| 100 | 100 | 2.0 | 4.1 | 5.3 | 19 | 0 | 0 | 23% |
| 200 | 200 | 1.4 | 2.8 | 5.3 | 30 | 0 | 0 | 44% |
| 400 | 400 | 1.1 | 2.5 | 10.6 | 72 | 0 | 0 | 62% |
| 800 | 800 | 1.2 | 2.9 | 6.2 | 103 | 0 | 0 | 113% |
| 1,000 | 1,000 | 2.0 | 4.5 | 11.7 | 38 | 0 | 0 | 219% |
| 1,200 | 1,200 | 3.0 | 7.4 | 15.2 | 288 | 0 | 0 | 318% |
| 1,400 | 1,400 | 7.7 | 21.9 | 44.7 | 4,998 | 27% | 0 | 280% |
| 1,600 | 1,599 | 8.9 | 20.5 | 85.6 | 5,003 | 40% | 0 | 263% |
| 2,000 | 1,983 | 11.6 | 26.3 | 92.3 | 5,012 | 65% | 0 | 227% |

**Sync tenant** (processed before the answer):

| Target/s | Delivered/s | p50 ms | p95 ms | p99 ms | Peak queue | Refused | Lost (acknowledged) | API CPU (of 1,200%) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 10 | 11.7 | 17.0 | 24.1 | 0 | 0 | 0 | 12% |
| 25 | 25 | 9.6 | 13.0 | 15.9 | 0 | 0 | 0 | 13% |
| 50 | 50 | 5.2 | 7.9 | 9.9 | 0 | 0 | 0 | 17% |
| 100 | 100 | 3.6 | 5.6 | 11.6 | 0 | 0 | 0 | 19% |
| 200 | 200 | 2.5 | 3.9 | 6.0 | 0 | 0 | 0 | 26% |
| 400 | 400 | 2.0 | 3.4 | 6.2 | 0 | 0 | 0 | 42% |
| 800 | 800 | 2.1 | 5.9 | 30.5 | 0 | 0 | 0 | 93% |
| 1,200 | 1,200 | 21.4 | 33.4 | 75.6 | 0 | 0 | 0 | 278% |
| 1,600 | 1,371 | 21.9 | 30.6 | 37.7 | 0 | 0 | 0 | 278% |
| 2,400 | 1,458 | 21.0 | 27.8 | 36.3 | 0 | 0 | 0 | 281% |
| 3,200 | 1,449 | 20.8 | 28.6 | 41.1 | 0 | 0 | 0 | 286% |

How to read it: async is clean to 1,200/s; at 1,400/s the queue reaches its 5,000 limit and 27% are refused with `503`, rising to
65% at 2,000/s. Sync never refuses: its delivered rate flattens near 1,450/s (the sender cannot push more through, p95 stays
about 28 ms), so it degrades by slowing down. Latency *falls* with rate at first because connections, caches and the workers' batches warm up.

## C. Steady load

400/s (half of the 800/s the first ramp reached) for 3 minutes: **72,000 events, p95 3.0 ms, p99 7.2 ms, 0 lost**,
queue peak 76, oldest wait 0.2 s, API CPU 83%, memory 27 MiB.

## D and D2. Burst and drain

A 30 s burst at 2,000/s (60,000 events) on one async tenant:

| Queue limit | Accepted | Refused | Peak queue | Oldest wait | Drained after the load | Lost |
|---|---:|---:|---:|---:|---:|---:|
| 5,000 (default) | 29,380 (49%) | 30,620 (51%) | 5,011 | 6.2 s | 0.0 s | 0 |
| 100,000 | 60,000 (100%) | 0 | 41,657 | 35.4 s | 35.1 s | 0 |

With the default limit the queue fills in seconds and the rest of the burst is refused. With a larger limit the whole burst is
accepted and drains at the workers' pace (about 1,200/s), the oldest job waiting 35 s. Rule of thumb: **queue limit ≈ the longest
burst you want to absorb × workers' rate**, and the wait you accept is limit ÷ 1,200 per second. The console's prediction
matched: it said "falling behind" while the queue grew and "up to date" once it drained.

## E. Noisy neighbour

`perf-a` floods at 1,600/s for 60 s while `perf-b` sends 5/s:

| | `perf-b` p95 | `perf-b` failures |
|---|---:|---:|
| alone | 12.1 ms | 0 |
| during the flood (default settings) | 37.2 ms (**3.07×**) | 0 |
| during the flood (tuned: 40 DB connections, 16 workers per tenant) | 946.2 ms | 0 |

Each tenant has its own worker pool and queue, so the victim was never refused and never starved of workers. But the flood still
costs it latency, because both share one API process, one 10-connection database pool and the database's CPU. **Making the
shared pool bigger made the victim worse (946 ms)**, because the flooding tenant could take more connections.

## T. Tuning experiment

`DB_MAX_CONNS=40`, `WORKERS_MAX=16`:

| Target/s | Delivered/s | p50 ms | p95 ms | p99 ms | Peak queue | Refused | Lost (acknowledged) | API CPU (of 1,200%) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 800 | 800 | 1.2 | 3.2 | 8.2 | 84 | 0 | 0 | 123% |
| 1,200 | 1,200 | 2.5 | 6.8 | 22.2 | 348 | 0 | 0 | 293% |
| 1,600 | 1,598 | 10.3 | 31.9 | 59.0 | 5,025 | 28% | 0 | 361% |
| 2,000 | 1,996 | 11.8 | 27.2 | 65.6 | 5,019 | 42% | 0 | 346% |

It accepts more under overload (at 1,600/s: 34,392 accepted of 48,000, against 28,566 with defaults) but does not move the
sustainable ceiling (1,200/s is clean either way, 1,600/s refuses in both), and it worsens the noisy-neighbour case. The limit is the
database's CPU, not the connection pool. Keep the defaults, and use `workers_total_max` and per-tenant limits to keep one tenant
from taking the pool.

## F. Mixed realistic traffic

Two tenants at 30/s each for 60 s with valid readings, sensor faults, malformed payloads and duplicates: 3,600 requests,
p95 8.1 ms, 0 lost, 0 failed checks. Of 450 events compared with what was sent, 430 matched, 20 were
dropped by a rule (a sentinel value nulled and flagged), and **0 differed without an explanation**.

## G. Idempotency under concurrency

| Case | Responses | Readings stored | Raw payloads stored |
|---|---|---:|---:|
| 200 identical payloads sent by 50 threads at once | 1 accepted/flagged, 199 duplicate (all `200`) | 1 sensor, 1 location | 200 |
| 5 different versions of one reading × 40 copies at once | 1 flagged, 160 conflict, 39 duplicate (all `200`) | 1 sensor | 200 (160 conflict rows) |

Exactly one reading per (tenant, device, time) however many arrive at once, and every payload is kept.

## H. Failure and recovery

(API in one container, workers in two others.)

| Failure | Result |
|---|---|
| One worker container stopped mid-load (100/s) | 4,000 sent, 0 refused, 0 lost; queue peak 13; the other worker kept up |
| Both workers stopped for 38 s (60/s) | 0 lost; backlog grew to 1,800, oldest wait 37.2 s; the console raised **"No workers"** (critical) and **"Workers are behind"**; after the restart the backlog drained within seconds |
| API container restarted mid-load (100/s) | 3,000 sent, 2,978 acknowledged, 22 got a connection error during the restart; **stored = acknowledged** (nothing stored without an acknowledgement, nothing acknowledged but missing). Tive would resend the 22 as duplicates, which the idempotency key absorbs |
| `workers_total_max` set to 2 live while a 2,250-job backlog waited | no process ever ran more than 2 workers (4 across both); the backlog still drained |

## I. Soak

200/s for 5 minutes: 60,000 events, p95 3.0 ms, p99 5.0 ms, 0 lost, queue flat (14.3 → 16.2),
mean CPU 30%, memory 18.1 → 21.9 MiB. The small rise over 5 minutes is
worth watching in a longer run, but there is no sign of a leak at this length.

## Defects found by the test (all fixed)

1. **The console failed under load** (`tenant stats: ... context deadline exceeded`). At 800+ events/s its per-tenant queries
   (sorting every outcome of 5 minutes, a count over every event) took seconds and shared the ingest connection pool. Fixed:
   bounded queries, a tenant-led index (migration 00009), figures shared between pages for 2 s, partial results with a notice,
   and a separate 4-connection pool. Measured afterwards at 800 events/s with eight pages refreshing every second: 0 failed
   refreshes, worst 1.6 s (was 5.5 s), and **ingest p95 improved from 14 to 4 ms**.
2. **The sender waited per event for workers that were down**, which could take hours. Fixed: one shared deadline.
3. **`docker-compose.split.yml` built a separate worker image** that went stale and ignored newer settings (the hard cap
   appeared not to work). Fixed: the worker uses the API's image.
4. **The local database had no volume**, so `docker compose down` erased it (it happened during this test). Fixed: a named volume.

## Findings that are not defects

- **Storage grows about 2.6–4.6 KB per event** (raw payload kept as bytes and as JSON, outcome, readings, ledger, indexes). At
  1,000 trackers every 5 minutes that is 288,000 events a day, roughly 0.75–1.3 GB a day, so **the free Render database (1 GB)
  fills within about a day at that fleet size**. There is no retention job yet.
- **Autovacuum matters under heavy writes**: right after the bulk runs a count that should take 19 ms took 650 ms until the
  tables were vacuumed.
- **Refused requests are not free**: they still authenticate and check the queue, using database connections.

## Recommendations

1. Set `QUEUE_MAX_DEPTH` for the burst you must absorb (limit ÷ 1,200/s = the wait you accept). The default of 5,000 is only
   about 4 seconds at saturation.
2. Keep `DB_MAX_CONNS=10` and the default worker maximum; use per-tenant overrides and `workers_total_max` for fairness.
3. Add a retention policy (or drop the duplicate JSON copy of the payload) before holding more than a day or two of a large fleet.
4. For stronger tenant isolation, run the API as `ROLE=api` plus separate `ROLE=worker` containers (see ADR 0001) and consider a
   connection pool per tenant.

## What this does not tell you

- The sender and the stack share one machine, so very high rates include the sender's own cost; latency is to localhost.
- Tive's real traffic is bursty and arrives from one address; the numbers say nothing about its retry behaviour beyond the
  documented one retry on `5xx`.
- One 5-minute soak does not exercise vacuum, backups or table growth over days.
