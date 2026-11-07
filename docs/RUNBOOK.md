# Runbook

Where to look first: the console (`/admin`) Dashboard and Monitoring pages show per-tenant arrival and completion rates, queue depth, oldest wait and workers. Alerts appear as banners and, when `ALERT_WEBHOOK_URL` is set, are posted once when they start and once when they clear.

| Alert / symptom | Meaning | What to do |
|---|---|---|
| **No workers** (critical) | An async tenant has jobs waiting and no worker runs for it | Check the service is up (`/readyz`). On Render free tier the service may be waking; wait a minute. Check `WORKERS_MAX` and `workers_total_max` (the cap may be 0 or used up by other tenants) on the Config page. |
| **Workers are behind** | The oldest job has waited longer than the tenant's lag target | Raise the tenant's worker max (Config, per tenant), or run separate worker containers (`ROLE=worker`, ADR 0001). |
| **Queue nearly full** | 80% of `queue_max_depth`; at the limit new payloads get `503` + `Retry-After` and are not stored | Add workers first; raise the queue limit only if the database has room. Tive retries a 5xx only once, so avoid reaching the limit. |
| **Dead events** | An event failed every attempt | Open it (Payloads, then the event) for the error; fix the cause (rules or data); replay with the event's Replay button or `api replay --status dead --apply`. |
| **High reject rate** | Many events rejected in 15 minutes | Open Compare for an affected device to see which fields differ; check the sender or the rules. |
| **Conflicts piling up** | Same device and timestamp sent with different content | Conflicts page shows the differing fields; choose the tenant's conflict policy, then preview and apply a replay. |
| **Console says figures are partial** | The database is slow; the console degrades rather than hanging | Check database load and connection count (`DB_MAX_CONNS`); the console uses its own small pool. |

## Routine tasks
- **New tenant and key:** Tenants page (key shown once) or `api tenants create` / `api keys create`. Revoke a key from the same page.
- **Change rules without a deploy:** Rules page (plain-English editor) or `api rules set --file`; instances adopt within about 10 seconds; every event records its rules version.
- **Reprocess after a fix:** `api replay` (dry run by default), see the README.
- **Rotate the admin token:** change `ADMIN_TOKEN` in the service environment; the service redeploys and old sessions end.
- **Database:** the free Render Postgres expires after about 30 days; export or upgrade before then.

## Deploying
Push to `main`: CI (tests, vet, lint) runs, and Render deploys when checks pass (`autoDeployTrigger: checksPass`). Migrations run on start. Roll back from the Render dashboard (Events, then Rollback); migrations are additive, so an older build keeps working.
