# PAXAFE Tive Integration — Constitution

Source: DESIGN.md §1.3. These principles outrank convenience. If code or a spec conflicts with one, change DESIGN.md first (CLAUDE.md), then this file if needed.

## Principles

### 1. No acknowledged data is ever lost
The raw payload is committed (with its ledger row) before any 2xx is returned.
*In code:* the ingest pipeline's first transaction is raw save; nothing is validated, rejected or transformed before it. The tenant is the API key's, so there is nothing to look up before the raw save.

### 2. Raw payloads are immutable
`raw_events` is append-only. Processing results live in `event_outcomes`.
*In code:* no `UPDATE` or `DELETE` on `raw_events`; the app role has no such grant.

### 3. Flag, don't fix silently
An invalid optional field becomes null plus a quality flag. Every fix records rule ID, original value and new value. No interpolation, clamping or guessing.
*In code:* `rules.Apply` returns flags for every change it makes; protected fields accept only allow-listed strategies.

### 4. Reject only unusable records
Reject (422) only when there is no device identity, no timestamp, no usable temperature and no valid coordinates. One bad field must not discard a valid reading.
*In code:* the reject list in DESIGN.md §5 is exhaustive; anything else is accepted with flags.

### 5. Idempotent by construction
Uniqueness is enforced by the database, `(tenant_id, device_imei, ts)`, and writes use `ON CONFLICT`. At-least-once processing is therefore safe; by default the first write wins and a differing second version is stored as a conflict; a tenant may instead choose `keep_last` or `reject` (the version is always recorded, never silently dropped).
*In code:* the key is checked and written inside one transaction under a per-key lock, with the unique index as the backstop.

### 6. Transforms are pure; providers and storage are pluggable
Parsing, rules, transform, quality and output rounding do no I/O (no DB, HTTP or clock). Interfaces exist only at real boundaries: `Provider`, `Store`, `Queue`, `Notifier`.
*In code:* `provider/tive`, `rules`, `paxafe` import nothing that performs I/O; time is passed in.

### 7. UTC everywhere; event time ≠ received time
Timestamps are epoch milliseconds UTC. `received_at` is stored separately from the device's event time. Seconds-precision epochs are detected, scaled and flagged.

### 8. Config over magic numbers
Every threshold (accuracy bands, future-timestamp limit, body limit, queue limit) is configuration with a default. The payload checks themselves are configurable rules in a ruleset (a framework plus a shipped default set), and an operator can change them at runtime without a restart.
*In code:* no numeric literals with domain meaning outside config defaults.

### 9. Every requirement has a test
Requirements carry IDs (EARS); tests and code comments reference them. Tests are table-driven, written before the code, and the unit suite runs in seconds.
*In code:* test names include the requirement ID; a requirement without a test is not done.

### 10. Boring, readable Go over cleverness
Standard library routing, plain structs, small functions, errors wrapped with context, no panics in request paths. The author must be able to explain and change every line live.

## Governance
- Order of work: spec → test → code.
- DESIGN.md is the source of truth; specs, ADRs and code must agree with it.
- A milestone is done only when its exit criteria are verified one by one.
