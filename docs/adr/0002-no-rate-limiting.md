# ADR 0002: No rate limiting on the webhook (it was built, then removed)

**Status:** accepted · **Date:** 2026-11-06 · **Supersedes:** the per-tenant rate limit in DESIGN.md §12 and spec F6-R4

## Context
The first design had a per-tenant token-bucket rate limit that answered `429` with `Retry-After`. It is a standard
protection against a runaway sender, and it was built and tested (and later made per-tenant configurable).

Tive's webhook sender cannot be throttled. Tive's developer documentation says it retries a `5xx` response once and then
gives up (missed data can be recovered only through Tive's own API, which this service does not call), and it
documents no handling of `429` or `Retry-After`. The operator confirmed that Tive does not support throttling.

## Decision
**Rate limiting is skipped.** There is no rate limiter, no `429`, and no rate-limit settings.

Reason: the only client of the webhook is Tive, and a refused payload is lost for good on Tive's side. A limit that
protects the service by discarding real cold-chain readings trades an availability problem for a data-loss problem,
which contradicts the first principle of this service (no acknowledged data is lost) in spirit, and gives the
operator a setting that can only hurt. A hard-coded limit would also be a magic number.

## What protects the service instead
- **Raw-first storage and a fast acknowledgement.** The webhook only stores the payload, so the work per request is small.
- **Async mode.** A tenant in async mode gets `202` and its burst waits in the Postgres queue; processing speed is the workers' concern, not the endpoint's.
- **Per-tenant queue limit** (`queue_max_depth`, default 5000). Beyond it ingest answers `503` with `Retry-After`. Tive retries a `5xx` once, so this is a last resort, not flow control: size the limit so it is not reached in normal operation.
- **Per-tenant worker pools and a hard cap on total workers**, so one tenant's backlog cannot take the workers or the database connections of the others.
- **Alerts** when a queue approaches its limit, when workers fall behind, or when events die.
- **Body size limit** (256 KB) and API-key authentication at the edge.

## Consequences
- A sender that floods the webhook is accepted until the queue limit; it is visible (dashboard, alerts) rather than silently throttled.
- The read and replay endpoints are not limited either. They are authenticated and cheap; a limiter can be added in front of them (an API gateway, or a shared limiter) if the service is exposed to untrusted callers.
- If a second provider that honours `429` is added later, a per-provider limit is a small change to the middleware; the git history (`cd13307` and before) has the token-bucket implementation.
- Documented by Tive but **not built**: verifying the `x-tive-signature` header (HMAC-SHA256 over `timestamp.body`) and allow-listing Tive's source address.
