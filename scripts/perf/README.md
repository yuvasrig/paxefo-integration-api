# Performance test scripts

These drive the mock sender against the local Docker stack and sample it while it runs (CPU and memory of the API
container, database connections, queue depth, oldest waiting job, workers). Results: `docs/PERFORMANCE.md`.

```bash
make up                                           # the stack (API with its workers, and Postgres)
(cd ../paxafe-mock-sender && go build -o /tmp/sender ./cmd/sender)

# three dedicated tenants; keys go in scripts/perf/keys.env (not committed)
curl -s -c /tmp/cj -X POST localhost:8080/admin/login -d token=local-admin-token -o /dev/null
mk() { curl -s -b /tmp/cj -X POST localhost:8080/admin/tenants --data-urlencode "id=$1" --data-urlencode "name=$1" \
         --data-urlencode "mode=$2" | grep -o 'X-API-Key: pxk_test_[A-Za-z0-9]*' | head -1 | sed 's/X-API-Key: //'; }
printf 'PERF_KEY_A=%s\nPERF_KEY_B=%s\nPERF_KEY_S=%s\n' "$(mk perf-a async)" "$(mk perf-b async)" "$(mk perf-s sync)" > scripts/perf/keys.env

python3 scripts/perf/perf.py A B C D E F K T      # baseline, step ramp, steady, burst, noisy neighbour, mixed, knee, tuning
python3 scripts/perf/perf2.py G H I                # idempotency, failure and recovery, soak
```

Notes: stage `T` restarts the API with `DB_MAX_CONNS=40` and `WORKERS_MAX=16`, then restores the defaults; stage `H` runs the
split layout (`docker-compose.split.yml`: one API container, two worker containers) and restores the default stack
afterwards. Each stage writes `results-<stage>.json` next to the script. The whole run takes about 45 minutes and uses most of
the machine, so do not use the machine for anything else meanwhile.
