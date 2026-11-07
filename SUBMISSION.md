# Submission: PAXAFE Tive integration

## Repositories
- Integration API: https://github.com/yuvasrig/paxefo-integration-api
- Mock Tive Sender: https://github.com/yuvasrig/paxafe-mock-sender

## Live (Render, free tier)
- Integration API: https://paxafe-api.onrender.com/admin/ . This is the admin view: the app is a webhook service with no end-user pages, so what you open in a browser is the operator console (dashboard, tenants, payloads, compare, conflicts, rules, config, monitoring, notifications). Sign in with the admin token from the submission email. The service itself is at https://paxafe-api.onrender.com (`/readyz`, `/docs`, and the webhook below).
- Mock Tive Sender: https://paxafe-sender.onrender.com
- Free services sleep when idle; the first request can take about a minute.
- API documentation: https://paxafe-api.onrender.com/docs (the API contract: quick start, statuses, flags, examples; the bare address redirects here). The raw spec is at `/openapi.yaml`.
- Webhook endpoint: `POST https://paxafe-api.onrender.com/api/webhook/tive` with header `x-api-key`.

## Test API key and admin token
Both are sent in the submission email (not committed to this public repository). The API key belongs to the `reviewer` tenant, which runs in async ingest mode. Tenants `acme-pharma` and `bioco` are also seeded. The admin token signs you in to the admin view; it is the `ADMIN_TOKEN` set on the service.

## How to see it working
1. `scripts/smoke.sh https://paxafe-api.onrender.com <key>` posts the 13 provided samples and checks each result against `specs/001-integration-api/expected/` (13 of 13 pass on the live deployment).
2. Open the sender, pick scenarios (route, excursion, fault, duplicates, malformed, auth, load), run them, and read the report: counts sent, acknowledged and processed, plus sent-vs-stored charts for temperature and location.
3. In the API console (`/admin`, operator token): **Payloads** shows each tenant's PAXAFE records; **Compare** sets what a device sent beside what the API produced, per device, with charts and a list of what the rules changed.

## Assumptions
- Idempotency key is `(tenant, device IMEI, timestamp)`; the same content is a duplicate and different content is a conflict handled by a per-tenant policy.
- The raw payload is stored untouched before anything else happens; transforms can be replayed after a rule fix.
- Tive cannot be throttled and retries a 5xx only once, so there is no rate limiting; overload shows as backpressure (503 with Retry-After) at a queue limit. See `docs/adr/0002-no-rate-limiting.md`.
- Webhook signature verification and the Tive source-IP allow-list are documented but not built.
- No retention policy is built; storage is roughly 2.6 to 4.6 KB per event (`docs/PERFORMANCE.md`).
- The free Render database expires after about 30 days.

## Documents
`DESIGN.md` (design), `specs/` (requirements and tasks), `docs/DEPLOY.md`, `docs/PERFORMANCE.md`, `docs/adr/`.
