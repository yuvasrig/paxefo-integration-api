# ADR 0017: Render with Render Postgres

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 15

## Decision
Render with Render Postgres.

## Rejected
Neon, Cloud Run.

## Why
One platform; the database is swappable through the environment.

## Consequences
`render.yaml` blueprints for both apps; see `docs/DEPLOY.md` and ADR 0001.
