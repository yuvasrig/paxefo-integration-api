# ADR 0012: Rounding only at output

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 10

## Decision
Rounding only at output.

## Rejected
Round on store.

## Why
Full precision is kept for audit.

## Consequences
Records round half away from zero when serialised; the raw payload is never rounded.
