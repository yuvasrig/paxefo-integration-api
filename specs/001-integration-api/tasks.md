# Tasks — M1 to M5 (test first)

Rule for every task: write the failing test (table-driven, requirement ID in the test name), then the code, then `go vet ./... && go test ./...`. Done = tests pass, IDs covered, DESIGN.md/specs still accurate, short commit. Milestone ends with its exit criteria checked one by one and a tag.

## M1 — Transform core (`m1-transform`)
- [ ] T1.1 `provider/tive` tolerant types: numbers as strings, nulls, absent objects record original type. Tests: F2-R4, sample #7, fuzz seed corpus.
- [ ] T1.2 `Provider` interface (`Name`, `Parse`, `Transform`) + registry. Test: unknown provider name.
- [ ] T1.3 `rules` loader: embedded default, `RULES_PATH` override, startup validation (unknown strategy, unsafe action on protected field, duplicate ID). Tests: F13-R2.
- [ ] T1.4 `rules.Apply` per rule ID, one table case each: NUMERIC-STRING (F2-R4), NO-IDENTITY / NAME-RESOLVED (F3-R2, F5-R2), TS-SECONDS (F2-R5), TS-FUTURE (F3-R4), TS-UTC-MISMATCH (F2-R6, #11), TEMP-F-ONLY (F2-R9), TEMP-SENTINEL / PROBE-SENTINEL (F2-R2), HUMIDITY-RANGE / LUX-NEGATIVE / BATTERY-RANGE (F2-R3), LAT-RANGE / LON-RANGE (F2-R1), ENTITY-NAME-MISMATCH / SHIPMENT-DEVICE-MISMATCH (F2-R7). Every fix records rule ID, original, new (F2-R11).
- [ ] T1.5 Strategies: `scale`, `coerce_number`, `fahrenheit_to_celsius`, `normalize_enum`, `derive_magnitude` (F2-R10).
- [ ] T1.6 `paxafe` records + rounding at marshal (half away from zero) (F1-R5). Table: -0.5625→-0.563, 0.6875→0.688, 4.625→4.63, 10.078125→10.08.
- [ ] T1.7 Transform mapping per `contracts/mapping.md`: temperature fallback (F2-R8), accuracy category incl. 500 m→Low (F2-R14), location source, address parsing cases, null handling, no-sensor-record case.
- [ ] T1.8 Output schema validation against embedded PAXAFE schemas (F1-R6).
- [ ] T1.9 Required-field/reject checks: NO_TIMESTAMP, NO_USABLE_DATA (F3-R3, F3-R5) with a `DeviceResolver` fake.
- [ ] T1.10 Golden test: 13 samples → `expected/*.json` (status, flags, records), `-update` flag; fuzz `FuzzParseTransform` 30 s.
- **Exit:** unit + golden + schema pass; fuzz clean; no I/O in `provider/tive`, `rules`, `paxafe`.

## M2 — Database and store (`m2-store`)
- [ ] T2.1 goose migrations per `data-model.md`; roles (migration vs app, no BYPASSRLS); RLS policies. Test: RLS isolation (F6-R5).
- [ ] T2.2 `Store` interface + `PGStore`, `WithTenantTx`. Test: unset tenant → no rows.
- [ ] T2.3 Concurrent insert of the same key → exactly one reading (F4-R1, F4-R4).
- [ ] T2.4 Conflict recording and device upsert "only if newer" (F4-R3, F5-R1).
- [ ] T2.5 Real `DeviceResolver`; validity windows; IMEI reuse across tenants (F5-R2, F5-R3, F5-R4).
- [ ] T2.6 `docker-compose.yml` Postgres 16 + API; `make seed` (tenants incl. `common-pool`, sources, one key each).
- **Exit:** `docker compose up` + `make seed` from a clean clone; integration tests green locally and in CI; migrations run on Render Postgres.

## M3 — Webhook endpoint (`m3-webhook`)
- [ ] T3.1 `auth`: key format, hash lookup, expiry/revocation (F6-R1, F6-R2).
- [ ] T3.2 Tenant routing: absent / own / unmapped→`common-pool` / other-tenant→reject (F3-R6, F3-R8, F3-R9).
- [ ] T3.3 `ingest` tx1 (raw + ledger) then pipeline then tx2; canonical-JSON hash (F1-R2, F4-R5).
- [ ] T3.4 Duplicate/conflict handling and status precedence (F4-R2, F4-R3, F4-R6).
- [ ] T3.5 Failure after tx1 → `failed`, 500; replay endpoint, idempotent, 409 when already ok (F7-R1…R4).
- [ ] T3.6 Handlers: webhook (+ alias), `GET /events/{id}` (owner or submitter), `GET /readings` (cursor), envelope and codes (F1-R1, F1-R7, F7-R6).
- [ ] T3.7 Body limit 256 KB, per-tenant token bucket (F6-R3, F6-R4); invalid JSON → 400 (F3-R1).
- [ ] T3.8 HTTP integration: 13 samples in order → match `expected/`; 401/413/400/422/429; cross-tenant read blocked; raw-first fault injection.
- [ ] T3.9 `scripts/smoke.sh <url> <key>` posts 13 samples, checks statuses (accepts first-run or re-run column).
- **Exit:** all tests green; smoke passes; every sample response matches `expected/`.

## M4 — Ops baseline (`m4-ops`)
- [ ] T4.1 Middleware: request ID echo, recover, access log fields, 5 s server timeouts (F6-R6, F13-R6). Test: no keys/payloads in logs.
- [ ] T4.2 `/healthz`, `/readyz` (DB, migration version, backend, mode), `/metrics` (RED, per-stage, flags, rejects, pool, runtime) (F13-R3, R4, R7).
- [ ] T4.3 Graceful shutdown, 10 s drain (F13-R5).
- [ ] T4.4 CLI: `keys create|list|revoke`, `tenants create|map-source`.
- [ ] T4.5 `/docs` (Redoc) + `/openapi.yaml` embedded; `GET /api/v1/rules`.
- **Exit:** log lines carry request_id, tenant_id, event_id, status, latency_ms; `/metrics` shows histograms after smoke; `/docs` renders.

## M5 — Deployed (`m5-deployed`)
- [ ] T5.1 Render env (`DATABASE_URL` internal, `ROLE=all`, `INGEST_MODE=sync`, `LOG_LEVEL=info`), health check `/healthz`.
- [ ] T5.2 Seed Render DB; reviewer key with `--expires 2026-12-15`.
- [ ] T5.3 CI → Render auto-deploy on `main`.
- **Exit:** `scripts/smoke.sh https://<api>.onrender.com <reviewer-key>` passes 13/13; `/readyz` green; `/docs` reachable; cold start measured.
