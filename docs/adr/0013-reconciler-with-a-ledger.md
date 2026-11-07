# ADR 0013: Reconciler with a ledger

**Status:** accepted (designed, not built) · **Source:** DESIGN.md §20, decision 11

## Decision
Reconciler with a ledger.

## Rejected
Trust counters.

## Why
Independent proof of completeness.

## Consequences
The insert-only ledger exists (decision 17) and the sender compares what it sent with what is stored, which gives the same proof from outside. The in-service reconciler (`internal/reconcile`) is a stub.
