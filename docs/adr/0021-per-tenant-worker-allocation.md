# ADR 0021: Per-tenant worker allocation

**Status:** accepted (built (dedicated pools)) · **Source:** DESIGN.md §20, decision 19

## Decision
Per-tenant worker allocation.

## Rejected
One global pool.

## Why
Fairness and capacity derived per customer.

## Consequences
Every async tenant has its own pool with its own min and max; `workers_total_max` caps the process. A shared pool for small tenants is not built.
