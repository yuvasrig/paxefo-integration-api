# ADR 0016: Elastic worker pool plus a KEDA demo

**Status:** accepted (partly built) · **Source:** DESIGN.md §20, decision 14

## Decision
Elastic worker pool plus a KEDA demo.

## Rejected
Fixed workers.

## Why
Shows scaling at both levels.

## Consequences
Pools scale per tenant inside a process and workers can run as separate containers (`make up-split`, ADR 0001). The local k3d and KEDA demo is not built.
