# ADR 0004: Raw-first ingest, sync/async toggle

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 2

## Decision
Raw-first ingest, sync/async toggle.

## Rejected
Sync only; external broker.

## Why
Instant demo results plus burst absorption, no new infrastructure.

## Consequences
Every payload is stored untouched before anything else happens. A tenant is `sync` (processed before the answer) or `async` (`202`, processed by workers). See DESIGN §8.
