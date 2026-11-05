# Plan 001 — Integration API (M0–M7)

Source: DESIGN.md. This plan is a pointer map, not a copy; if they disagree, DESIGN.md wins and is updated first.

## Technical context
Go 1.23 · stdlib `net/http` · `pgx/v5` + `pgxpool` · `goose` migrations (run at startup) · `slog` JSON · Postgres 16 · `santhosh-tekuri/jsonschema` for output validation · `testcontainers-go` for integration tests · Render (Docker) deploy.

## Modules built in this scope (DESIGN.md §3.1)

| Package | Responsibility | Pure? | Milestone |
|---|---|---|---|
| `internal/config` | Env parsing, thresholds with defaults | yes | M0 stub, M1+ |
| `internal/provider` | `Provider` interface + registry | yes | M1 |
| `internal/provider/tive` | Input structs tolerant of strings/nulls, parse, transform | yes | M1 |
| `internal/rules` | Load/validate ruleset, `Apply`, strategies | yes | M1 |
| `internal/paxafe` | Output records, rounding at marshal, schema validation | yes | M1 |
| `internal/store` | `Store` interface, `PGStore`, `WithTenantTx` | no | M2 |
| `internal/auth` | Key format, hash lookup, tenant context | no | M3 |
| `internal/ingest` | Pipeline orchestration, tenant routing, idempotency, replay | no | M3 |
| `internal/httpapi` (+ `middleware`) | Routes, handlers, error envelope, request ID, logging, limits | no | M3–M4 |
| `internal/observability` | Metrics, log helpers | no | M4 |
| `cmd/api` | Wiring, graceful shutdown, `keys` / `tenants` CLI | no | M0 stub → M4 |

Not built in M0–M7 (empty packages with doc comments only): `queue`, `worker`, `domain`, `reconcile`, `alerting`.

Dependency direction: `httpapi → ingest → provider | rules | store`. Pure packages import no I/O packages and take time as a parameter.

## Pipeline (DESIGN.md §4.1, sync only)
auth → tenant routing → **tx1** raw event + ledger (commit) → parse → rules → required-field check → transform → output schema validation → **tx2** readings / conflicts / device / shipment / outcome → respond.

## Non-functional requirements (built scope)

| NFR | Target | How | Verified by |
|---|---|---|---|
| Durability | No acked payload lost | Raw + ledger committed before 2xx | F7 integration test (fail tx2, raw exists) |
| Correctness | Output always schema-valid | Schema validation; golden tests | Golden + schema tests (M1) |
| Idempotency | One reading per key | Unique `(tenant_id, device_imei, ts)` + `ON CONFLICT` | Concurrent-insert test (M2) |
| Tenant isolation | No cross-tenant reads | Key → tenant; RLS on every table | RLS + API tests (M2, M3) |
| Auditability | Row traces to raw event + versions | Immutable raw, `transform_version`, `rules_version` | Outcome tests |
| Latency | p95 < 200 ms sync | Pure transform, indexed writes | Sender load run (M6/M8c) |
| Security | No unauthenticated writes | Hashed keys, 256 KB cap, rate limit, no secrets in logs | F6 tests, log test |
| Testability | Unit suite < 10 s | Pure functions, table tests | CI |

Designed, not built: async queue, elastic workers, reconciler, alerting, traces, Timescale, KEDA demo, Kafka path (DESIGN.md §8–§11, §16, §22).

## Decisions
ADRs live in `docs/adr/`, one per DESIGN.md §20 row that applies to what is built (written in M7): 1, 2 (sync part), 4, 6 (Postgres part), 7, 8, 9, 10, 15, 16, 17, 18; rows 3, 5, 11–14, 19 are documented as designed-not-built.

## Open questions (DESIGN.md §21)
200 vs 409 for conflicts (200 chosen) · `type` always `Active` for Tive · authoritative temperature source · source of shipment temperature bands.
