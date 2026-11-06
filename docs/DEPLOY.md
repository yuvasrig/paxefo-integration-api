# Deploying the Integration API to Render (M5)

Everything here is done in the Render dashboard or on your laptop. Nothing below has been run yet: the free
Render tier needs your account, and a free web service has no shell, so seeding runs from your laptop.

## 1. Push the repo
Create a GitHub repository and push `main`. CI (`.github/workflows/ci.yml`) runs vet, lint and tests with a Postgres service container.

## 2. Create the services from the blueprint
Render dashboard → **New → Blueprint** → pick the repo. `render.yaml` creates:
- Postgres `paxafe-db` (free, region `oregon`)
- Web service `paxafe-api` (Docker, free, same region, health check `/healthz`, deploys `main` after CI passes)

`DATABASE_URL` is wired to the database's **internal** connection string. `ROLE=all`, `INGEST_MODE=sync`, `LOG_LEVEL=info` are set by the blueprint. Migrations run when the service starts.

Free Postgres expires 30 days after creation (plus a 14-day grace). Create it on deploy day. Recovery: new database → update `DATABASE_URL` → the service migrates on start → re-seed and re-create keys.

## 3. Seed the database from your laptop
Free web services have no shell, so use the database's **External Database URL** (dashboard → `paxafe-db` → Connect). Append `?sslmode=require`.

```bash
export DATABASE_URL='<external url>?sslmode=require'
go run ./cmd/api seed
go run ./cmd/api keys create --tenant reviewer --name paxafe-reviewer --expires 2026-12-15
go run ./cmd/api keys create --tenant reviewer --name sender
```

- `seed` creates the tenants, maps Tive accounts (478 → `reviewer`; 999 stays unmapped) and prints one key per tenant. Keep none of those except what you need: revoke the rest with `keys revoke ID`.
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
