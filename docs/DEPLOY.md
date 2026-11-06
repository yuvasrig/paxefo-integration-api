# Deploying the Integration API to Render (M5)

Everything here is done in the Render dashboard or on your laptop. Nothing below has been run yet: the free
Render tier needs your account, and a free web service has no shell, so seeding runs from your laptop.

## 1. Push the repo
Create a GitHub repository and push `main`. CI (`.github/workflows/ci.yml`) runs vet, lint and tests with a Postgres service container.

## 2. Create the services from the blueprint
Render dashboard → **New → Blueprint** → pick the repo. `render.yaml` creates:
- Postgres `paxafe-db` (free, region `oregon`)
- Web service `paxafe-api` (Docker, free, same region, health check `/healthz`, deploys `main` after CI passes)

`DATABASE_URL` is wired to the database's **internal** connection string. `ROLE=all`, `INGEST_MODE=async`, `WORKERS_MIN=0`, `WORKERS_MAX=3`, `WORKERS_TOTAL_MAX=6`, `DB_MAX_CONNS=15`, `LOG_LEVEL=info` are set by the blueprint. Migrations run when the service starts.

**Async mode:** the web service also runs the workers (`ROLE=all`), one pool per async tenant, so no extra Render service is needed. The seeded `reviewer` tenant stays sync (instant results from curl); `acme-pharma` and `bioco` are async: a webhook returns `202 queued` and a worker processes it a moment later (read it with `GET /api/v1/events/{id}`; the mock sender follows it automatically). Free web services sleep when idle and the workers sleep with them; queued jobs wait safely in Postgres and drain when the service wakes. To scale workers separately later, add a second service from the same image with `ROLE=worker` and set the web service to `ROLE=api` (Render background workers are a paid service type).

**Async mode:** the web service also runs the workers (`ROLE=all`), one pool per async tenant, so no extra Render service is needed. The seeded `reviewer` tenant stays sync (instant results from curl); `acme-pharma` and `bioco` are async: a webhook returns `202 queued` and a worker processes it a moment later (read it with `GET /api/v1/events/{id}`; the mock sender follows it automatically). Free web services sleep when idle and the workers sleep with them; queued jobs wait safely in Postgres and drain when the service wakes. To scale workers separately later, add a second service from the same image with `ROLE=worker` and set the web service to `ROLE=api` (Render background workers are a paid service type).

**Multiple containers:** the same image runs as `ROLE=api` (webhooks) or `ROLE=worker` (processing). `make up-split` runs one API and two worker containers locally to prove it; `render.yaml` has the paid worker service commented out. Rationale and test results: `docs/adr/0001-deployment-topology.md`.

**Alerts:** the console shows them on the Dashboard and Monitoring pages. To also be notified, set `ALERT_WEBHOOK_URL` (for example a Slack incoming webhook) in the Render dashboard.

**Admin console:** the blueprint asks you for `ADMIN_TOKEN` (any long random string you choose; keep it in your password manager). With it set, `https://<your-service>.onrender.com/admin` is the operator console; leave it empty and the console does not exist. Optionally set `PUBLIC_BASE_URL` (shown in webhook setup), `LOGS_URL` (the service's Logs page in Render) and `DASHBOARD_URL`.

Free Postgres expires 30 days after creation (plus a 14-day grace). Create it on deploy day. Recovery: new database → update `DATABASE_URL` → the service migrates on start → re-seed and re-create keys.

## 3. Seed the database from your laptop
Free web services have no shell, so use the database's **External Database URL** (dashboard → `paxafe-db` → Connect). Append `?sslmode=require`.

```bash
export DATABASE_URL='<external url>?sslmode=require'
go run ./cmd/api seed
go run ./cmd/api keys create --tenant reviewer --name paxafe-reviewer --expires 2026-12-15
go run ./cmd/api keys create --tenant reviewer --name sender
```

- `seed` creates the tenants `reviewer`, `acme-pharma` and `bioco` and prints one key per tenant. Keep only what you need: revoke the rest with `keys revoke ID`. The tenant of every request is the key's; the payload's `AccountId` is not checked.
- The **reviewer key** goes in the submission email. The **sender key** goes in the sender service's `SENDER_API_KEY` environment variable and nowhere else.
- Plaintext keys are printed once.

## 4. Verify
```bash
scripts/smoke.sh https://<api>.onrender.com <reviewer-key>
curl https://<api>.onrender.com/readyz
```
Expect 13 PASS, `/readyz` ok, and `https://<api>.onrender.com/docs` rendering.

## 5. Measure the cold start (for the README)
The free instance sleeps after 15 minutes idle. After it has slept:
```bash
time curl -s -o /dev/null https://<api>.onrender.com/healthz
```
Note the number; the sender pings `/healthz` on load for this reason.

## Exit criteria (DESIGN.md section 23, M5)
- [ ] `scripts/smoke.sh https://<api>.onrender.com <reviewer-key>` passes 13/13
- [ ] `/readyz` green and `/docs` reachable
- [ ] cold start measured

## Sender (M6)
The Mock Tive Sender lives in its own repo (`paxafe-mock-sender`) with its own `render.yaml`. After the API is up:
1. Create the service from its blueprint (New -> Blueprint).
2. Set `API_BASE_URL` to the API's public URL and `SENDER_API_KEY` to the `sender` key from step 3 above.
3. Open the sender URL: it wakes the API, then **Provided samples** should show 13 passed and 0 missing (M6 exit criterion).
4. From your laptop, for load numbers: `sender run --scenario load --rate 100 --duration 30s --url https://<api>.onrender.com --key <key>`.

## Changing rules without a deploy
Rules are configurable on the fly. From your laptop, with the database's external URL (as in step 3):
```bash
export DATABASE_URL='<external url>?sslmode=require'
go run ./cmd/api rules default > my.yaml     # edit it and change `version:`
go run ./cmd/api rules validate --file my.yaml
go run ./cmd/api rules set --file my.yaml    # the live service adopts it within ~10 s, no restart
go run ./cmd/api rules reset                 # back to the defaults
```
`GET /api/v1/rules` (with a key) shows the version, source and load time actually in force.

## Fixing rules or conflicts, then reprocessing what was already received
After changing rules (`api rules set`) or a tenant's conflict policy (`api tenants set-conflict`), reprocess the stored payloads. From your laptop with the database's external URL:
```bash
export DATABASE_URL='<external url>?sslmode=require'
# 1. See what would change. This is a dry run: nothing is written.
go run ./cmd/api replay --tenant reviewer --from 2026-11-01 --to 2026-11-08
# 2. Narrow it if you want: one device (IMEI or name), one account, some statuses, or the reading's own time.
go run ./cmd/api replay --tenant reviewer --from 2026-11-01 --to 2026-11-08 --device A571992 --status conflict,rejected
# 3. Apply it.
go run ./cmd/api replay --tenant reviewer --from 2026-11-01 --to 2026-11-08 --apply
go run ./cmd/api replay --all-tenants --from 2026-11-01 --to 2026-11-08 --apply   # every tenant, each isolated
go run ./cmd/api replay runs                                                        # audit of applied replays
```
A dry run holds locks on the readings it touches until it finishes, so run it off-peak on a large window. Replay uses the ruleset in force (published with `rules set`, else the file, else the default).
