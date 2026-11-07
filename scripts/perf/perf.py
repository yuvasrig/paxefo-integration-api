#!/usr/bin/env python3
"""Performance test orchestrator for the PAXAFE integration API (local Docker stack).
Usage: perf.py STAGES...   (A B C D E F G H I)
Runs the sender CLI, samples the stack while it runs, and writes results-<stage>.json."""
import json, os, re, subprocess, sys, threading, time, urllib.request, urllib.parse, http.cookiejar

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))   # the integration API repo (where docker-compose.yml is)
URL = "http://localhost:8080"
SENDER = os.environ.get("SENDER", "/tmp/sender")             # the mock sender binary: go build -o /tmp/sender ./cmd/sender
KEYS = dict(l.strip().split("=", 1) for l in open(os.path.join(HERE, "keys.env")) if "=" in l)
KA, KB, KS = KEYS["PERF_KEY_A"], KEYS["PERF_KEY_B"], KEYS["PERF_KEY_S"]
ENV = dict(os.environ, PERF_KEY_A=KA, PERF_KEY_B=KB, PERF_KEY_S=KS)

def log(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)

def sh(cmd, timeout=60):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=REPO).stdout.strip()

def psql(sql):
    return sh(f"docker compose exec -T db psql -U paxafe -tA -F, -c \"{sql}\"")

SAMPLE_SQL = ("select (select count(*) from pg_stat_activity where datname='paxafe'),"
              "(select count(*) from ingest_jobs where status='pending'),"
              "(select count(*) from ingest_jobs where status='processing'),"
              "coalesce((select extract(epoch from now()-min(enqueued_at)) from ingest_jobs where status='pending'),0),"
              "coalesce((select sum(workers) from worker_heartbeats where last_seen > now()-interval '30 seconds'),0)")

class Sampler(threading.Thread):
    def __init__(self, container="paxefo-api-1", every=2.0):
        super().__init__(daemon=True); self.container, self.every, self.rows, self.stop = container, every, [], False
    def run(self):
        t0 = time.time()
        while not self.stop:
            try:
                st = sh(f"docker stats --no-stream --format '{{{{.CPUPerc}}}} {{{{.MemUsage}}}}' {self.container}", 20).split()
                cpu = float(st[0].rstrip('%')); mem = float(st[1].replace('MiB','').replace('GiB','')) * (1024 if 'GiB' in st[1] else 1)
                conns, pend, proc, oldest, workers = [float(x) for x in psql(SAMPLE_SQL).split(",")]
                self.rows.append(dict(abs=time.time(), t=round(time.time()-t0,1), cpu=cpu, mem_mib=mem, db_conns=conns, pending=pend, processing=proc, oldest_s=round(oldest,1), workers=workers))
            except Exception as e:
                pass
            time.sleep(self.every)
    def peak(self, k): return max((r[k] for r in self.rows), default=0)

def sender(loads, verify=500, wait="120s", timeout=900):
    args = [SENDER, "run", "--url", URL, "--json", "--verify", str(verify), "--wait", wait]
    for l in loads: args += ["--load", l]
    p = subprocess.run(args, capture_output=True, text=True, env=ENV, timeout=timeout)
    try:
        rep = json.loads(p.stdout)
    except Exception:
        raise RuntimeError("sender failed: " + p.stderr[-500:] + p.stdout[-300:])
    return rep

def drain(timeout=240):
    """Seconds until no job is waiting (None if it never drains within the timeout)."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        pend, proc = [int(x) for x in psql("select (select count(*) from ingest_jobs where status='pending'),(select count(*) from ingest_jobs where status='processing')").split(",")]
        if pend + proc == 0: return round(time.time() - t0, 1)
        time.sleep(1)
    return None

def summarize(rep, label, rate=None, sampler=None, drain_s=None):
    s = rep["summary"]
    out = dict(step=label, rate=rate, sent=s["sent"], acknowledged=s["acknowledged"], throughput=round(s["throughput_per_sec"], 1),
               p50=round(s["p50_ms"], 1), p95=round(s["p95_ms"], 1), p99=round(s["p99_ms"], 1), max=round(s["max_ms"], 1),
               error_rate=round(s["error_rate"], 4), missing=s["missing"], not_processed=s["not_processed"], refused=s["refused"],
               failed_checks=s["failed"], verified=s["verified"], compared=s.get("compared", 0), matched=s.get("matched", 0),
               fixed=s.get("fixed", 0), dropped=s.get("dropped", 0), differed=s.get("differed", 0))
    if sampler:
        out.update(peak_queue=sampler.peak("pending"), peak_oldest_s=sampler.peak("oldest_s"), peak_cpu=sampler.peak("cpu"),
                   peak_mem_mib=sampler.peak("mem_mib"), peak_db_conns=sampler.peak("db_conns"), peak_workers=sampler.peak("workers"))
    if drain_s is not None or sampler: out["drain_s"] = drain_s
    if rep.get("loads"): out["loads"] = [dict(target=l["target"], sent=l["sent"], failed=l["failed"], p95=round(l["p95_ms"], 1), throughput=round(l["throughput_per_sec"], 1)) for l in rep["loads"]]
    return out

def run_step(label, loads, rate=None, duration=30, settle=True):
    smp = Sampler(); smp.start()
    time.sleep(1.5)  # let the first sample land before the load starts
    t0 = time.time()
    rep = sender(loads)
    t_end = time.time()
    d = drain() if settle else None
    smp.stop = True; time.sleep(0.2)
    res = summarize(rep, label, rate, smp, d)
    res["wall_s"] = round(t_end - t0, 1)
    # Drain time as the sampler saw it: from the end of the load (start + duration) until the last sample that still had a backlog.
    load_end = t0 + duration + 1.0  # the sender wakes the API (~1 s) before it starts
    backlog = [r["abs"] for r in smp.rows if r["pending"] + r["processing"] > 0]
    res["drain_after_load_s"] = round(max(0.0, (max(backlog) - load_end)), 1) if backlog else 0.0
    res["series"] = [(r["t"], r["pending"], r["workers"], r["cpu"]) for r in smp.rows][::2]
    log(label, {k: res[k] for k in ("sent","acknowledged","refused","throughput","p50","p95","p99","error_rate","missing","not_processed","peak_queue","peak_oldest_s","peak_cpu","peak_db_conns","peak_workers","drain_after_load_s") if k in res})
    return res, smp

def save(stage, data):
    json.dump(data, open(os.path.join(HERE, f"results-{stage}.json"), "w"), indent=1)

def load_spec(scen, rate, dur, key="PERF_KEY_A", label=None, start=None):
    s = f"scenarios={scen} rate={rate} duration={dur}s key=env:{key}"
    if label: s += f" label={label}"
    if start: s += f" start={start}"
    return s

# ---------------------------------------------------------------- stages
def stage_A():
    log("== A: baseline (async, 10/s, 60 s)")
    res, _ = run_step("A baseline async", [load_spec("route,fault", 10, 60)], 10, 60)
    save("A", res); return res

def knee_check(r, base_p95):
    reasons = []
    if r["p95"] > max(100, 3 * base_p95): reasons.append(f"p95 {r['p95']} ms")
    if r["error_rate"] > 0: reasons.append("errors")
    if r["missing"] or r["not_processed"]: reasons.append("lost/unprocessed")
    if r["rate"] and r["throughput"] < 0.9 * r["rate"]: reasons.append(f"throughput {r['throughput']}/s")
    if r.get("drain_s") is None or r["drain_s"] > 60: reasons.append(f"drain {r.get('drain_s')} s")
    return reasons

def stage_B():
    out = {}
    for mode, key in (("async", "PERF_KEY_A"), ("sync", "PERF_KEY_S")):
        log(f"== B: step ramp ({mode})")
        steps, base = [], None
        for rate in (10, 25, 50, 100, 200, 400, 800):
            r, _ = run_step(f"B {mode} {rate}/s", [load_spec("route,fault", rate, 30, key)], rate, 30)
            if base is None: base = r["p95"]
            r["knee_reasons"] = knee_check(r, base)
            steps.append(r)
            if r["knee_reasons"]:
                log(f"   knee at {rate}/s ({mode}): {r['knee_reasons']}"); break
            time.sleep(3)
        passing = [s for s in steps if not s["knee_reasons"]]
        out[mode] = dict(steps=steps, capacity=passing[-1]["rate"] if passing else None, knee=steps[-1]["rate"] if steps[-1]["knee_reasons"] else None)
        save("B", out)
    return out

def stage_C(capacity):
    rate = max(5, int(capacity * 0.5))
    log(f"== C: steady at 50% of async capacity = {rate}/s for 180 s")
    res, smp = run_step(f"C steady async {rate}/s", [load_spec("route,fault", rate, 180)], rate, 180)
    res["queue_series"] = [(r["t"], r["pending"], r["workers"]) for r in smp.rows][::5]
    save("C", res); return res, rate

def parse_prediction(tenant):
    jar = http.cookiejar.CookieJar(); op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.open(urllib.request.Request(URL + "/admin/login", urllib.parse.urlencode({"token": "local-admin-token"}).encode()))
    html = op.open(URL + "/admin/monitoring/live").read().decode()
    m = re.search(r"<tr>\s*<td><a[^>]*>%s</a></td>(.*?)</tr>" % re.escape(tenant), html, re.S)
    cells = [re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<td[^>]*>(.*?)</td>", m.group(1), re.S)] if m else []
    top = re.search(r'<div class="pred[^"]*">(.*?)</div>', html, re.S)
    return dict(row=cells, banner=re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", top.group(1))).strip() if top else "")

def stage_D(steady):
    rate = steady * 5
    log(f"== D: burst {rate}/s (5x steady) for 30 s, then drain")
    smp = Sampler(); smp.start()
    preds = []
    def poll():
        t0 = time.time()
        for off in (12, 22, 32, 45):
            time.sleep(max(0, off - (time.time() - t0)))
            try: preds.append(dict(at_s=off, **parse_prediction("perf-a")))
            except Exception as e: preds.append(dict(at_s=off, error=str(e)))
    th = threading.Thread(target=poll); th.start()
    t0 = time.time(); rep = sender([load_spec("route,fault", rate, 30)]); t_sent = time.time()
    d = drain(300); smp.stop = True; th.join()
    res = summarize(rep, f"D burst {rate}/s", rate, smp, d)
    res["load_ended_to_drained_s"] = d
    res["predictions"] = preds
    log("D", {k: res[k] for k in ("sent","p95","missing","not_processed","peak_queue","peak_oldest_s","drain_s")}, preds[:3])
    save("D", res); return res

def stage_E(knee_rate):
    log("== E: noisy neighbour")
    base, _ = run_step("E baseline perf-b alone 5/s", [load_spec("route,fault", 5, 30, "PERF_KEY_B", "B")], 5, 30)
    flood = max(20, knee_rate)
    res, smp = run_step(f"E perf-a floods {flood}/s while perf-b sends 5/s",
                        [load_spec("route,fault", flood, 60, "PERF_KEY_A", "A-flood"), load_spec("route,fault", 5, 60, "PERF_KEY_B", "B-victim")], None, 60)
    b = [l for l in res["loads"] if l["target"].startswith("B-victim")][0]
    out = dict(baseline_b_p95=base["p95"], flood_rate=flood, together=res, victim_p95=b["p95"], victim_failed=b["failed"],
               ratio=round(b["p95"] / max(base["p95"], 0.1), 2))
    log("E victim p95 alone", base["p95"], "ms; during flood", b["p95"], "ms; ratio", out["ratio"])
    save("E", out); return out

def stage_F():
    log("== F: mixed realistic traffic on two tenants")
    res, _ = run_step("F mixed", [load_spec("route,fault,malformed,duplicates", 30, 60, "PERF_KEY_A", "A"), load_spec("route,fault,malformed,duplicates", 30, 60, "PERF_KEY_B", "B")], 60, 60)
    save("F", res); return res

def refine(mode, key, rates, tag):
    steps = []
    for rate in rates:
        r, _ = run_step(f"{tag} {mode} {rate}/s", [load_spec("route,fault", rate, 30, key)], rate, 30)
        r["refused_share"] = round(r["refused"] / max(r["sent"], 1), 3)
        steps.append(r)
        time.sleep(5)
    return steps

def stage_K():
    log("== K: refine the knee (async and sync above 800/s)")
    out = dict(async_=refine("async", "PERF_KEY_A", (1000, 1200, 1400, 1600, 2000), "K"),
               sync=refine("sync", "PERF_KEY_S", (1200, 1600, 2400, 3200), "K"))
    save("K", out); return out

def compose(cmd, timeout=180):
    return sh(f"docker compose {cmd}", timeout)

def wait_ready():
    for _ in range(60):
        try:
            if urllib.request.urlopen(URL + "/readyz", timeout=2).status == 200: return True
        except Exception: pass
        time.sleep(1)
    return False

def stage_T():
    log("== T: tuning experiment: DB_MAX_CONNS 10 -> 40, workers_max 8 -> 16")
    override = os.path.join(HERE, "tuned.yml")
    open(override, "w").write("services:\n  api:\n    environment:\n      DB_MAX_CONNS: \"40\"\n      WORKERS_MAX: \"16\"\n")
    compose(f"-f docker-compose.yml -f {override} up -d --force-recreate api"); assert wait_ready(); time.sleep(3)
    steps = refine("async-tuned", "PERF_KEY_A", (800, 1200, 1600, 2000), "T")
    out = dict(config="DB_MAX_CONNS=40 WORKERS_MAX=16", steps=steps)
    # Burst again under the tuned settings
    smp = Sampler(); smp.start(); time.sleep(1.5)
    t0 = time.time(); rep = sender([load_spec("route,fault", 2000, 30)]); d = drain(300); smp.stop = True
    r = summarize(rep, "T burst 2000/s tuned", 2000, smp, d); r["refused_share"] = round(r["refused"] / max(r["sent"], 1), 3)
    out["burst"] = r
    # Noisy neighbour under the tuned settings
    base = out["steps"][0]
    e, _ = run_step("T noisy neighbour tuned", [load_spec("route,fault", 1600, 60, "PERF_KEY_A", "A-flood"), load_spec("route,fault", 5, 60, "PERF_KEY_B", "B-victim")], None, 60)
    b = [l for l in e["loads"] if l["target"].startswith("B-victim")][0]
    out["noisy"] = dict(victim_p95=b["p95"], victim_failed=b["failed"], flood_refused=e["refused"], flood_sent=e["sent"])
    log("T victim p95 under flood (tuned):", b["p95"])
    save("T", out)
    compose("up -d --force-recreate api"); wait_ready()   # back to the default settings
    return out

STAGES = {"A": stage_A, "B": stage_B, "F": stage_F}

if __name__ == "__main__":
    state_file = os.path.join(HERE, "state.json")
    state = json.load(open(state_file)) if os.path.exists(state_file) else {}
    for st in sys.argv[1:]:
        if st == "A": state["A"] = stage_A()
        elif st == "B":
            state["B"] = stage_B(); state["capacity_async"] = state["B"]["async"]["capacity"] or 10; state["capacity_sync"] = state["B"]["sync"]["capacity"] or 10
        elif st == "C": state["C"], state["steady"] = stage_C(state["capacity_async"])
        elif st == "D": state["D"] = stage_D(state["steady"])
        elif st == "E": state["E"] = stage_E(state.get("B", {}).get("async", {}).get("knee") or state["capacity_async"] * 2)
        elif st == "F": state["F"] = stage_F()
        elif st == "K": state["K"] = stage_K()
        elif st == "T": state["T"] = stage_T()
        json.dump(state, open(state_file, "w"), indent=1)
    log("done", sys.argv[1:])
