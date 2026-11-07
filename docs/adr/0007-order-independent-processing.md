# ADR 0007: Order-independent processing

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 5

## Decision
Order-independent processing.

## Rejected
Per-device sharding.

## Why
Free horizontal scale; any worker can take any job.

## Consequences
Under `keep_last` the last *received* event wins, so an older event never overwrites a newer one. Strict ordering would need Kafka partitions later.
