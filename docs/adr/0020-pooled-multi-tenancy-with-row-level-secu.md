# ADR 0020: Pooled multi-tenancy with row-level security

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 18

## Decision
Pooled multi-tenancy with row-level security.

## Rejected
Schema or database per tenant.

## Why
Cheap to operate; isolation is enforced by the database.

## Consequences
`FORCE ROW LEVEL SECURITY`; requests run as `paxafe_app` with `app.tenant_id` set per transaction. Physical separation remains an option for a tenant that needs it.
