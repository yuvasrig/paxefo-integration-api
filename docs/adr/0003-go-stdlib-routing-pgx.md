# ADR 0003: Go, stdlib routing, pgx

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 1

## Decision
Go, stdlib routing, pgx.

## Rejected
gin/echo.

## Why
Strongest language; less to explain live.

## Consequences
Plain `net/http` handlers, `pgx/v5`, `goose` migrations, `slog`. No web framework or ORM.
