# ADR 0014: Prometheus rules plus an in-app evaluator

**Status:** accepted (partly built) · **Source:** DESIGN.md §20, decision 12

## Decision
Prometheus rules plus an in-app evaluator.

## Rejected
One only.

## Why
The production standard and something visible in a demo.

## Consequences
`/metrics` and the in-app alert evaluator (`internal/alerts`, shown in the console, optional webhook) are built; a Prometheus rules file is not.
