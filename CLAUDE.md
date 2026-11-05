# PAXAFE Take-Home — Instructions for Claude Code

## Read first
- **`DESIGN.md`** — the approved design and single source of truth. **§0 Scope** and **§23 Milestones** govern what to build and when; the rest is design intent. Build only the current milestone.
- `./assignment/` — `EXERCISE.md`, Tive + PAXAFE JSON schemas, `sample-tive-payloads.json`.

## Context
Senior Integration Engineer take-home. Due **Fri Nov 6, 2026 EOB**. Two deployed Go apps (Integration API + Mock Tive Sender) on Render.
**Next round: live walkthrough + live changes.** I must understand and own every line. Prefer simple, readable code over clever abstractions.

## How we work
- **Spec-driven** (GitHub Spec Kit layout):
  ```
  .specify/memory/constitution.md      # from DESIGN.md §1.3
  specs/001-integration-api/{spec.md, plan.md, data-model.md, contracts/, tasks.md}
  specs/002-mock-sender/...
  docs/adr/                            # one ADR per DESIGN.md §20 row
  ```
- Requirements in **EARS** with IDs (e.g. `F2-R3: WHEN ProbeTemperature.Celsius is -32768, 327.67 or 32767, THE SYSTEM SHALL set it to null AND add flag SENSOR_FAULT`). Tests and code comments reference IDs.
- **Test-first**: write table-driven tests from acceptance criteria, then implement.
- **Iterative**: one step at a time; show me each spec/module and wait for approval. Keep responses concise.
- If code needs to deviate from DESIGN.md, stop and propose the change to DESIGN.md first.
- Commit after each approved step with a clear message.

## Coding conventions
- Go 1.23, stdlib `net/http`, `pgx/v5`, `goose`, `slog`. No web framework, no ORM.
- Pure functions for transform/validation/quality/`desired()`; no I/O in them.
- Interfaces only at real boundaries (Provider, Store, Queue, Notifier).
- All thresholds from config with defaults; no magic numbers.
- Errors wrapped with context (`fmt.Errorf("...: %w", err)`); no panics in request paths.
- Context propagation and timeouts on every DB call.
- Never log API keys or full payloads.

## Definition of done (per task)
- Tests pass (`go test ./...`), `go vet` and `golangci-lint` clean.
- Requirement IDs covered by tests.
- DESIGN.md / specs still accurate.
- Short commit.
