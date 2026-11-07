# ADR 0009: Two-tier validation

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 7

## Decision
Two-tier validation.

## Rejected
Reject on any error.

## Why
One bad field must not drop a valid temperature.

## Consequences
Rejects are limited to `NO_TIMESTAMP` and `NO_USABLE_DATA`-style cases; other problems null the field and add a flag.
