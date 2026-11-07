#!/usr/bin/env python3
"""Stages G (idempotency under concurrency), H (failure and recovery), I (soak). Usage: perf2.py G H I"""
import concurrent.futures as cf, copy, random, sys
from perf import *

TEMPLATE = json.load(open(os.path.join(REPO, "testdata", "samples", "02.json")))

def post(key, payload, timeout=30):
    req = urllib.request.Request(URL + "/api/webhook/tive", json.dumps(payload).encode(), {"X-API-Key": key, "Content-Type": "application/json"})
    try:
        r = urllib.request.urlopen(req, timeout=timeout); return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None

def fresh_device():
    imei = "86" + "".join(random.choice("0123456789") for _ in range(13))
    return imei, "PERF" + imei[-6:]

def payload(imei, name, ts, temp=None):
    p = copy.deepcopy(TEMPLATE)
    p["DeviceId"], p["DeviceName"], p["EntityName"], p["EntryTimeEpoch"] = imei, name, name, ts
    p["EntryTimeUtc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts / 1000))
    if temp is not None: p["Temperature"] = {"Celsius": temp, "Fahrenheit": round(temp * 9 / 5 + 32, 3)}
    return p

def admin():
    jar = http.cookiejar.CookieJar(); op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.open(urllib.request.Request(URL + "/admin/login", urllib.parse.urlencode({"token": "local-admin-token"}).encode()))
    return op

def admin_get(path): return admin().open(URL + path).read().decode()
def admin_post(path, form):
    return admin().open(urllib.request.Request(URL + path, urllib.parse.urlencode(form).encode(), {"X-Admin-Token": "local-admin-token"})).status

def alerts_now():
    html = admin_get("/admin/monitoring/live")
    return [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m)).strip() for m in re.findall(r'<div class="alert [a-z]+" role="alert">(.*?)</div>', html, re.S)]

def count(sql): return int(psql(sql).split()[0])

# ---------------------------------------------------------------- G
def stage_G():
    log("== G: idempotency under concurrency (sync tenant perf-s)")
    out = {}
    imei, name = fresh_device(); ts = int(time.time() * 1000)
    with cf.ThreadPoolExecutor(50) as ex:
        res = list(ex.map(lambda _: post(KS, payload(imei, name, ts)), range(200)))
    st = {}
    for code, body in res: st[(code, body and body["status"])] = st.get((code, body and body["status"]), 0) + 1
    where = f"tenant_id='perf-s' and device_imei='{imei}' and ts={ts}"
    out["G1_identical_200_concurrent"] = dict(responses={f"{k[0]}/{k[1]}": v for k, v in st.items()},
        sensor_rows=count(f"select count(*) from sensor_readings where {where}"), location_rows=count(f"select count(*) from location_readings where {where}"),
        raw_events_stored=count(f"select count(*) from raw_events where tenant_id='perf-s' and payload->>'DeviceId'='{imei}'"),
        outcomes=psql(f"select o.status||':'||count(*) from event_outcomes o join raw_events r on r.id=o.raw_event_id where r.tenant_id='perf-s' and r.payload->>'DeviceId'='{imei}' group by o.status order by 1").replace("\n", " "))
    log("G1", out["G1_identical_200_concurrent"])

    imei2, name2 = fresh_device(); ts2 = int(time.time() * 1000)
    jobs = [payload(imei2, name2, ts2, temp=5.0 + v) for v in range(5) for _ in range(40)]; random.shuffle(jobs)
    with cf.ThreadPoolExecutor(50) as ex:
        res = list(ex.map(lambda p: post(KS, p), jobs))
    st = {}
    for code, body in res: st[(code, body and body["status"])] = st.get((code, body and body["status"]), 0) + 1
    where2 = f"tenant_id='perf-s' and device_imei='{imei2}' and ts={ts2}"
    out["G2_5_versions_x40_concurrent"] = dict(responses={f"{k[0]}/{k[1]}": v for k, v in st.items()},
        sensor_rows=count(f"select count(*) from sensor_readings where {where2}"),
        raw_events_stored=count(f"select count(*) from raw_events where tenant_id='perf-s' and payload->>'DeviceId'='{imei2}'"),
        conflict_rows=count(f"select count(*) from reading_conflicts where tenant_id='perf-s' and device_imei='{imei2}' and ts={ts2}"),
        outcomes=psql(f"select o.status||':'||count(*) from event_outcomes o join raw_events r on r.id=o.raw_event_id where r.tenant_id='perf-s' and r.payload->>'DeviceId'='{imei2}' group by o.status order by 1").replace("\n", " "))
    log("G2", out["G2_5_versions_x40_concurrent"])
    save("G", out); return out

# ---------------------------------------------------------------- H
def split_up():
    compose = lambda c: sh(f"docker compose {c}", 240)
    compose("-f docker-compose.yml -f docker-compose.split.yml up -d --remove-orphans --force-recreate")
    assert wait_ready_(); time.sleep(10)

def wait_ready_():
    for _ in range(90):
        try:
            if urllib.request.urlopen(URL + "/readyz", timeout=2).status == 200: return True
        except Exception: pass
        time.sleep(1)
    return False

def default_up():
    sh("docker compose -f docker-compose.yml -f docker-compose.split.yml rm -sf worker", 120)
    sh("docker compose up -d --force-recreate --remove-orphans api", 240)
    assert wait_ready_(); time.sleep(5)

def raw_count(tenant): return count(f"select count(*) from raw_events where tenant_id='{tenant}'")

def threaded_sender(loads, holder, wait="150s"):
    t = threading.Thread(target=lambda: holder.update(rep=sender(loads, wait=wait))); t.start(); return t

def stage_H():
    log("== H: failure and recovery (API in one container, workers in two others)")
    out = {}
    split_up()

    # H1: kill one worker container mid-load.
    smp = Sampler(); smp.start(); time.sleep(1.5); h = {}
    th = threaded_sender([load_spec("route,fault", 100, 40)], h); time.sleep(16)
    sh("docker stop paxefo-worker-1"); log("   stopped worker-1"); th.join(); d = drain(180); smp.stop = True
    sh("docker start paxefo-worker-1")
    out["H1_kill_one_worker"] = summarize(h["rep"], "H1", 100, smp, d); out["H1_kill_one_worker"]["note"] = "worker-1 stopped 16 s into a 40 s load at 100/s"
    log("H1", {k: out["H1_kill_one_worker"][k] for k in ("sent","refused","missing","not_processed","peak_queue","peak_oldest_s","drain_s")})
    time.sleep(10)

    # H2: all workers down for ~40 s, then back.
    smp = Sampler(); smp.start(); time.sleep(1.5); h = {}; seen = {}
    t_start = time.time(); th = threaded_sender([load_spec("route,fault", 60, 40)], h)
    time.sleep(10); sh("docker stop paxefo-worker-1 paxefo-worker-2"); t_stop = time.time(); log("   stopped both workers")
    time.sleep(22); seen["alerts_while_down"] = alerts_now()
    time.sleep(max(0, 48 - (time.time() - t_start))); sh("docker start paxefo-worker-1 paxefo-worker-2"); t_restart = time.time(); log("   restarted workers")
    th.join(); d = drain(240); smp.stop = True
    backlog = [r["abs"] for r in smp.rows if r["pending"] + r["processing"] > 0]
    r = summarize(h["rep"], "H2", 60, smp, d)
    r.update(alerts_while_down=seen["alerts_while_down"], backlog_at_restart=max((x["pending"] for x in smp.rows if x["abs"] <= t_restart), default=0),
             drained_after_restart_s=round(max(0, max(backlog) - t_restart), 1) if backlog else 0,
             note="both workers stopped 10 s in, restarted at 48 s; load 60/s for 40 s")
    out["H2_all_workers_down"] = r
    log("H2", {k: r[k] for k in ("sent","missing","not_processed","peak_queue","peak_oldest_s","backlog_at_restart","drained_after_restart_s")}, r["alerts_while_down"][:2])
    time.sleep(10)

    # H3: API restart mid-load.
    before = raw_count("perf-a"); smp = Sampler(); smp.start(); time.sleep(1.5); h = {}
    th = threaded_sender([load_spec("route,fault", 100, 30)], h); time.sleep(10)
    t0 = time.time(); sh("docker restart paxefo-api-1", 120); outage = time.time() - t0; log(f"   API restart took {outage:.1f} s")
    th.join(); d = drain(180); smp.stop = True
    after = raw_count("perf-a"); r = summarize(h["rep"], "H3", 100, smp, d)
    r.update(api_restart_s=round(outage, 1), stored_in_db=after - before, acked_by_api=r["acknowledged"],
             stored_but_not_acked=(after - before) - r["acknowledged"], failed_requests=r["sent"] - r["acknowledged"],
             note="docker restart of the API container 10 s into a 30 s load at 100/s")
    out["H3_api_restart"] = r
    log("H3", {k: r[k] for k in ("sent","acknowledged","failed_requests","stored_in_db","stored_but_not_acked","missing","not_processed")})
    wait_ready_(); time.sleep(8)

    # H4: the hard cap on workers, changed live while a backlog is waiting.
    sh("docker stop paxefo-worker-1 paxefo-worker-2"); h = {}
    th = threaded_sender([load_spec("route,fault", 150, 15)], h, wait="6s"); th.join()
    backlog_before = count("select count(*) from ingest_jobs where status='pending'")
    admin_post("/admin/config/set", {"key": "workers_total_max", "value": "2"}); time.sleep(4)
    sh("docker start paxefo-worker-1 paxefo-worker-2")
    per_instance_max, total_max = 0, 0
    for _ in range(20):
        rows = psql("select instance, sum(workers) from worker_heartbeats where last_seen > now()-interval '10 seconds' group by 1").splitlines()
        vals = [int(x.split(",")[1]) for x in rows if "," in x]
        per_instance_max = max([per_instance_max] + vals); total_max = max(total_max, sum(vals)); time.sleep(1)
    d = drain(240)
    admin_post("/admin/config/reset", {"key": "workers_total_max"})
    out["H4_hard_cap"] = dict(cap_per_process=2, backlog_when_workers_started=backlog_before, max_workers_in_any_process=per_instance_max, max_workers_total=total_max, drained_in_s=d)
    log("H4", out["H4_hard_cap"])
    save("H", out)
    default_up()
    return out

# ---------------------------------------------------------------- I
def stage_I(rate=200, seconds=300):
    log(f"== I: soak at {rate}/s for {seconds} s")
    size0 = psql("select pg_database_size('paxafe')"); events0 = count("select count(*) from raw_events")
    smp = Sampler(every=3.0); smp.start(); time.sleep(1.5)
    rep = sender([load_spec("route,fault", rate, seconds)], verify=800); d = drain(300); smp.stop = True
    r = summarize(rep, f"I soak {rate}/s", rate, smp, d)
    rows = smp.rows; third = max(1, len(rows) // 3)
    mean = lambda xs: sum(xs) / max(len(xs), 1)
    r.update(mem_first_third_mib=round(mean([x["mem_mib"] for x in rows[:third]]), 1), mem_last_third_mib=round(mean([x["mem_mib"] for x in rows[-third:]]), 1),
             queue_first_third=round(mean([x["pending"] for x in rows[:third]]), 1), queue_last_third=round(mean([x["pending"] for x in rows[-third:]]), 1),
             cpu_mean=round(mean([x["cpu"] for x in rows]), 1), db_bytes_added=int(psql("select pg_database_size('paxafe')")) - int(size0),
             events_added=count("select count(*) from raw_events") - events0)
    r["bytes_per_event"] = round(r["db_bytes_added"] / max(r["events_added"], 1))
    log("I", {k: r[k] for k in ("sent","p95","p99","missing","not_processed","mem_first_third_mib","mem_last_third_mib","queue_first_third","queue_last_third","cpu_mean","bytes_per_event")})
    save("I", r); return r

if __name__ == "__main__":
    for st in sys.argv[1:]:
        {"G": stage_G, "H": stage_H, "I": stage_I}[st]()
    log("done", sys.argv[1:])
