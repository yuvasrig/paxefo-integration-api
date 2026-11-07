# User guide

Two apps work together. The **Mock Tive Sender** pretends to be Tive and sends payloads; the **Integration API** receives them, turns them into PAXAFE records and has an operator console. The screenshots were taken on a local run with demo data.

| App | Live | Local |
|---|---|---|
| Integration API (console at `/admin`) | https://paxafe-api.onrender.com | http://localhost:8080 |
| Mock Tive Sender | https://paxafe-sender.onrender.com | `go run ./cmd/sender serve` in the sender repo |

Free Render services sleep when idle; the first request can take about a minute (the sender's home page wakes the API for you).

## The short version

1. **Get an API key.** An operator makes a tenant in the console's **Tenants** tab (the key is shown once) or runs `api seed` for the demo tenants.
2. **Send data.** Open the sender, paste the key, pick a scenario and press **Run**. (Or point Tive at `POST <api>/api/webhook/tive` with the key in the `x-api-key` header.)
3. **Check the answer.** The sender's report shows what was sent and what the API stored. The console's **Payloads** and **Compare** tabs show the same from the API's side.
4. **Tune it.** Change rules, settings or a tenant's conflict policy in the console; no restart needed. **Conflicts** can then reprocess old events.

## Mock Tive Sender

![The sender's home page](img/sender-home.png)

**Scenario.** Pick one of: *Provided samples* (the 13 payloads from the exercise), *Realistic route* (a drive with temperature and position), *Temperature excursion*, *Sensor fault* (sentinel values), *Duplicates and conflicts*, *Malformed burst*, *Auth failures*, *Load burst*.

**Run options.** API key (leave empty to use the sender's own key; a key you type is used for that run only and is never stored), route points, and for load scenarios the rate and duration (the web version is capped; use the CLI for bigger loads).

**Load generator.** Build a plan of several *loads*. Each load has its own tenant key (several keys run it on every tenant at once), one or more scenarios (requests are drawn evenly from them), a rate, a duration and a start time (default now). Use **Add another load** for more rows. This is how to test several tenants at the same time.

**The report** (after Run):

![A report: counts, charts, and sent versus stored per event](img/sender-report-full.png)

- *Counts:* sent, acknowledged, processed, rejected (expected) and missing. Anything the API acknowledged must be found again; "missing" should be 0.
- *Checks:* each request's HTTP status, outcome and flags against what the scenario expects.
- *What was sent vs what the API stored:* temperature and location charts (sent as a line, stored as dots; a red ring is something sent but not stored) and a row per event with a verdict (**match**, **fixed** by a rule, **dropped** by a rule, or **differ** if unexplained) and the reason. Download it as CSV.
- *Devices used:* the device ids and IMEIs of the run, to look them up in the console.
- For an **async** tenant the sender waits for the workers to finish each event before comparing.

Command line (same engine): `go run ./cmd/sender list`, `go run ./cmd/sender run --scenario route`, see the sender repo's README.

## Integration API console

Open `/admin` and sign in with the operator token (`ADMIN_TOKEN`). Without that variable set the console does not exist. The tabs, left to right:

### Dashboard
![Dashboard](img/admin-dashboard.png)

Live figures per tenant: events arriving per minute, devices active, completed per minute, queue depth and oldest wait, latency, and the workers running. Alert banners appear at the top (dead events, no workers, workers behind, queue nearly full, high reject rate). The page refreshes itself.

### Tenants
![Tenants](img/admin-tenants.png)

Add a tenant (this also issues its first API key and shows the webhook URL and Tive setup once), choose **sync** (processed before the answer) or **async** (answers `202`, workers process it), set the tenant's conflict policy (*keep first*, *keep last*, *reject*) and the status code it answers (200 or 409), issue or revoke keys, and jump to the tenant's own settings.

### Payloads
![Payloads](img/admin-payloads.png)

The PAXAFE sensor and location records the API produced, per tenant, newest first. Filter by time (when the API received it, or the reading's own time; last 15 minutes to 7 days or a custom UTC window), device (name or IMEI), status and "only events that produced a record". The device's **compare** link opens the Compare tab. Download the selection as JSON.

Open an event to see it in detail:

![An event: received versus processed, flags, PAXAFE payload beside the Tive payload](img/admin-event.png)

A field-by-field table of the value received and the value processed with the flag and rule behind any change, the quality flags, why it was rejected (if it was), the PAXAFE JSON and the Tive JSON exactly as received (never modified).

### Compare
Choose a tenant and window. With no device chosen it lists the devices that sent events:

![Compare: devices](img/admin-compare-devices.png)

Pick a device:

![Compare: one device](img/admin-compare-device.png)

You get counts (events, changed by the API, no PAXAFE record), the flags raised, a temperature chart and a location chart (received as a line, produced as blue dots, a received value outside the produced range as a red ring, for example a sensor fault), and below them a list of the events where a value changed by more than rounding.

### Conflicts
![Conflicts](img/admin-conflicts.png)

When the same device and timestamp arrive with different content, the pair is listed here with the fields that really differ and what the tenant's policy did. After changing the policy or the rules, **preview** (a dry run) and then **apply** a replay of the conflicting events from the last N hours.

### Rules
![Rules](img/admin-rules.png)

Every check or fix is a rule shown as a plain-English sentence. Switch rules on or off, edit thresholds, action, flag code and severity, reorder or delete them, or add one from dropdowns. Pick a tenant to override a rule for that tenant only. Each change is validated, versioned automatically and live on every instance within seconds; every event records the rules version that processed it. The raw YAML editor, history and reset are under *Advanced*.

### Config
![Config](img/admin-config.png)

Settings (queue limit, worker minimum and maximum, jobs per worker, lag target, retry attempts, reject-rate alert) are global, and each tenant can override them (choose the scope at the top). Changes apply without a restart. Settings that need a restart are listed read-only; secrets are never shown.

### Monitoring
![Monitoring](img/admin-monitoring.png)

Live rates and latency, recent rejects, conflicts and failures, recent events, the workers, and a prediction of whether the workers are keeping up or how long a backlog needs to clear. Links to `/metrics`, your logs and your dashboard.

## Common tasks

| I want to… | Do this |
|---|---|
| Try the exercise samples | Sender, scenario *Provided samples*, Run. Or `scripts/smoke.sh <api> <key>`. |
| See what a rule did to one reading | Payloads, open the event, read *Received vs processed*. |
| Compare a device's readings with what was stored | Compare tab, pick the device. |
| Test several tenants at once | Sender, Load generator, one load with several keys. |
| Change a threshold for one tenant only | Rules (pick the tenant) or Config (pick the tenant). |
| Fix a rule and reprocess old events | Rules (change), then Conflicts (preview, apply) or `api replay`. |
| Find out why something is slow | Dashboard alerts, Monitoring, then the [runbook](RUNBOOK.md). |
