# ADR 0011: Conflict handling is a per-tenant setting: `keep_first` (default), `keep_last`, `reject`, answering 200 or 409

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 9

## Decision
Conflict handling is a per-tenant setting: `keep_first` (default), `keep_last`, `reject`, answering 200 or 409.

## Rejected
One fixed behaviour.

## Why
Tenants differ: audit-first, latest-wins or strict; it avoids hard-coding the 200-versus-409 question.

## Consequences
Set with `api tenants set-conflict` or the console; the Conflicts page shows the differing fields and can replay.
