# Architecture decision records

One per row of the decisions table in DESIGN.md §20, plus two written from deployment and review. The status says whether the decision is built.

- [ADR 0001: One image, three roles, one container on the free tier](0001-deployment-topology.md) — accepted
- [ADR 0002: No rate limiting on the webhook (it was built, then removed)](0002-no-rate-limiting.md) — accepted
- [ADR 0003: Go, stdlib routing, pgx](0003-go-stdlib-routing-pgx.md) — accepted (built)
- [ADR 0004: Raw-first ingest, sync/async toggle](0004-raw-first-ingest-sync-async-toggle.md) — accepted (built)
- [ADR 0005: Postgres queue (`ingest_jobs`, `FOR UPDATE SKIP LOCKED`)](0005-postgres-queue-ingest-jobs-for-update-sk.md) — accepted (built)
- [ADR 0006: Separate immutable `raw_events`, `event_outcomes` and `ingest_jobs`](0006-separate-immutable-raw-events-event-outc.md) — accepted (built)
- [ADR 0007: Order-independent processing](0007-order-independent-processing.md) — accepted (built)
- [ADR 0008: Postgres (Timescale as a later toggle)](0008-postgres-timescale-as-a-later-toggle.md) — accepted (built (Postgres); Timescale not built)
- [ADR 0009: Two-tier validation](0009-two-tier-validation.md) — accepted (built)
- [ADR 0010: Device temperature primary, probe as fallback](0010-device-temperature-primary-probe-as-fall.md) — accepted (built)
- [ADR 0011: Conflict handling is a per-tenant setting: `keep_first` (default), `keep_last`, `reject`, answering 200 or 409](0011-conflict-handling-is-a-per-tenant-settin.md) — accepted (built)
- [ADR 0012: Rounding only at output](0012-rounding-only-at-output.md) — accepted (built)
- [ADR 0013: Reconciler with a ledger](0013-reconciler-with-a-ledger.md) — accepted (designed, not built)
- [ADR 0014: Prometheus rules plus an in-app evaluator](0014-prometheus-rules-plus-an-in-app-evaluato.md) — accepted (partly built)
- [ADR 0015: OpenTelemetry, Prometheus and slog](0015-opentelemetry-prometheus-and-slog.md) — accepted (partly built)
- [ADR 0016: Elastic worker pool plus a KEDA demo](0016-elastic-worker-pool-plus-a-keda-demo.md) — accepted (partly built)
- [ADR 0017: Render with Render Postgres](0017-render-with-render-postgres.md) — accepted (built)
- [ADR 0018: A rules framework with default rules, configurable at runtime](0018-a-rules-framework-with-default-rules-con.md) — accepted (built)
- [ADR 0019: Insert-only ingest ledger](0019-insert-only-ingest-ledger.md) — accepted (built)
- [ADR 0020: Pooled multi-tenancy with row-level security](0020-pooled-multi-tenancy-with-row-level-secu.md) — accepted (built)
- [ADR 0021: Per-tenant worker allocation](0021-per-tenant-worker-allocation.md) — accepted (built (dedicated pools))
