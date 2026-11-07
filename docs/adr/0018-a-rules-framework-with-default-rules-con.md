# ADR 0018: A rules framework with default rules, configurable at runtime

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 16

## Decision
A rules framework with default rules, configurable at runtime.

## Rejected
Hard-coded checks; rules only at startup; a separate quality package.

## Why
Behaviour changes through configuration without a deploy; fixes are always flagged and reversible; every event records its rules version.

## Consequences
Plain-English rule editor, per-tenant overrides, guardrails on protected fields; see the README.
