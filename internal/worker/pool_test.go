package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
	"github.com/yuvasrig/paxefo-integration-api/internal/worker"
)

var ctx = context.Background()

// flaky fails the commit of chosen events: always (poison) or for the first n tries.
type flaky struct {
	*store.PGStore
	mu    sync.Mutex
	fails map[string]int // event id -> remaining failures; -1 = always
}

func (f *flaky) Commit(c context.Context, in store.CommitInput) (store.CommitResult, error) {
	f.mu.Lock()
	left, ok := f.fails[in.RawEventID]
	if ok && left != 0 {
		if left > 0 {
			f.fails[in.RawEventID] = left - 1
		}
		f.mu.Unlock()
		return store.CommitResult{}, errors.New("injected failure")
	}
	f.mu.Unlock()
	return f.PGStore.Commit(c, in)
}

type obs struct {
	mu      sync.Mutex
	results map[string]int
}

func (o *obs) JobFinished(r string)                        { o.mu.Lock(); o.results[r]++; o.mu.Unlock() }
func (o *obs) QueueState([]store.QueueRow, map[string]int) {}
func (o *obs) get(r string) int                            { o.mu.Lock(); defer o.mu.Unlock(); return o.results[r] }

type env struct {
	st  *flaky
	svc *ingest.Service
	mgr *worker.Manager
	obs *obs
	set *settings.Holder
}

func newEnv(t *testing.T, tenants ...string) *env {
	t.Helper()
	pg, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{MaxConns: 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range tenants {
		if err := pg.CreateTenant(ctx, id, id, nil); err != nil {
			t.Fatal(err)
		}
		if err := pg.SetTenantIngestMode(ctx, id, "async"); err != nil {
			t.Fatal(err)
		}
	}
	rs, _ := rules.Default()
	st := &flaky{PGStore: pg, fails: map[string]int{}}
	set := settings.NewHolder(settings.Values{QueueMaxDepth: 1000, WorkersMin: 2, WorkersMax: 4, JobsPerWorker: 5, LagSLOSeconds: 10, MaxAttempts: 3})
	svc := &ingest.Service{
		Store: st, Provider: tive.NewDynamic(rules.NewHolder(rs, rules.SourceDefault, time.Now()), config.Default()),
		Now: func() time.Time { return time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC) }, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DefaultMode: ingest.ModeSync, Settings: set,
	}
	o := &obs{results: map[string]int{}}
	mgr := &worker.Manager{
		Source: st, Processor: svc, Settings: set, Instance: "test", Role: "all", DefaultMode: "sync", Observer: o,
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), PollInterval: 10 * time.Millisecond,
		ControlEvery: 20 * time.Millisecond, HeartbeatEvery: 20 * time.Millisecond, ScaleDownAfter: 50 * time.Millisecond,
		StuckAfter: time.Minute, Now: time.Now,
	}
	return &env{st: st, svc: svc, mgr: mgr, obs: o, set: set}
}

func (e *env) send(t *testing.T, tenant string, sample int) string {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", sample))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.svc.Ingest(ctx, ingest.Request{TenantID: tenant, RequestID: "r", Body: b})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "queued" || resp.HTTPStatus != 202 {
		t.Fatalf("async tenants get 202 queued: %+v", resp)
	}
	return resp.EventID
}

func (e *env) run(t *testing.T) (stop func()) {
	t.Helper()
	c, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { e.mgr.Run(c); close(done) }()
	return func() { cancel(); <-done }
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (e *env) status(tenant, id string) string {
	ev, err := e.st.GetEvent(ctx, tenant, id)
	if err != nil {
		return "?"
	}
	return ev.Status
}

func TestPoolProcessesEveryQueuedJob(t *testing.T) {
	e := newEnv(t, "reviewer", "acme-pharma")
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, e.send(t, "reviewer", 2)) // same payload: first accepted, the rest duplicates
	}
	other := e.send(t, "acme-pharma", 0)
	stop := e.run(t)
	defer stop()
	eventually(t, "all jobs done", func() bool { return e.obs.get("done") == 6 })
	for _, id := range ids {
		if s := e.status("reviewer", id); s != "accepted" && s != "duplicate" {
			t.Fatalf("%s: %s", id, s)
		}
	}
	if s := e.status("acme-pharma", other); s != "flagged" {
		t.Fatalf("%s", s)
	}
	depth, _ := e.st.PendingDepth(ctx, "reviewer", 100)
	if depth != 0 {
		t.Fatalf("queue should be empty: %d", depth)
	}
	eventually(t, "heartbeats for both tenants", func() bool {
		hbs, _ := e.st.ActiveHeartbeats(ctx, time.Minute)
		seen := map[string]int{}
		for _, h := range hbs {
			seen[h.TenantID] = h.Workers
		}
		return len(hbs) == 2 && seen["reviewer"] >= 1 && seen["acme-pharma"] >= 1
	})
}

func TestFailingJobIsRetriedThenSucceeds(t *testing.T) {
	e := newEnv(t, "reviewer")
	id := e.send(t, "reviewer", 2)
	e.st.fails[id] = 1
	// Backoff is 2s, so make retries immediate by moving the clock the pool uses.
	e.mgr.Now = func() time.Time { return time.Now().Add(-time.Hour) }
	stop := e.run(t)
	defer stop()
	eventually(t, "retry then done", func() bool { return e.status("reviewer", id) == "accepted" })
	if e.obs.get("retry") != 1 || e.obs.get("done") != 1 {
		t.Fatalf("%v", e.obs.results)
	}
}

func TestPoisonJobEndsDeadAndDoesNotBlockOthers(t *testing.T) {
	e := newEnv(t, "reviewer")
	poison := e.send(t, "reviewer", 2)
	good := e.send(t, "reviewer", 0)
	e.st.fails[poison] = -1
	e.mgr.Now = func() time.Time { return time.Now().Add(-time.Hour) }
	stop := e.run(t)
	defer stop()
	eventually(t, "poison dead", func() bool { return e.status("reviewer", poison) == "dead" })
	eventually(t, "good done", func() bool { return e.status("reviewer", good) == "flagged" })
	ev, _ := e.st.GetEvent(ctx, "reviewer", poison)
	if ev.Error == "" || len(ev.Raw) == 0 {
		t.Fatalf("a dead event keeps its error and raw payload: %+v", ev)
	}
	if ok, _ := e.st.HasJob(ctx, poison); ok {
		t.Fatal("dead jobs leave the queue")
	}
	if e.obs.get("dead") != 1 {
		t.Fatalf("%v", e.obs.results)
	}
}

type panicker struct{ worker.Processor }

func (p panicker) ProcessJob(c context.Context, j store.Job) error { panic("boom") }

func TestPanicInProcessingDoesNotKillTheWorker(t *testing.T) {
	e := newEnv(t, "reviewer")
	id := e.send(t, "reviewer", 2)
	e.mgr.Processor = panicker{e.svc}
	e.mgr.Now = func() time.Time { return time.Now().Add(-time.Hour) }
	stop := e.run(t)
	defer stop()
	eventually(t, "dead after panics", func() bool { return e.status("reviewer", id) == "dead" })
	ev, _ := e.st.GetEvent(ctx, "reviewer", id)
	if ev.Error == "" {
		t.Fatal("the panic is recorded as the error")
	}
}

func TestPoolScalesUpWithBacklogAndDownAfterwards(t *testing.T) {
	e := newEnv(t, "reviewer")
	e.set.Set(settings.Values{QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3})
	// Hold the jobs back with a far-future retry time so they stay pending (a backlog) without being processed.
	for i := 0; i < 20; i++ {
		e.send(t, "reviewer", 2)
	}
	if _, err := e.st.Pool().Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	stop := e.run(t)
	defer stop()
	eventually(t, "scale up to max", func() bool { return e.mgr.Workers()["reviewer"] == 4 })
	if _, err := e.st.Pool().Exec(ctx, `DELETE FROM ingest_jobs`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "scale down to min", func() bool { return e.mgr.Workers()["reviewer"] == 1 })
}

func TestShutdownFinishesTheClaimedBatchAndRemovesTheHeartbeat(t *testing.T) {
	e := newEnv(t, "reviewer")
	for i := 0; i < 8; i++ {
		e.send(t, "reviewer", 2)
	}
	stop := e.run(t)
	eventually(t, "some work started", func() bool { return e.mgr.TotalWorkers() > 0 && e.obs.get("done") > 0 })
	stop()
	// Nothing may be left in the 'processing' state: a claimed job is always finished or handed back.
	rows, _ := e.st.QueueStats(ctx)
	for _, r := range rows {
		if r.Processing != 0 {
			t.Fatalf("jobs left claimed after shutdown: %+v", r)
		}
	}
	if hbs, _ := e.st.ActiveHeartbeats(ctx, time.Minute); len(hbs) != 0 {
		t.Fatalf("a clean shutdown removes the heartbeat: %+v", hbs)
	}
}

// Every tenant has its own pool, sized from its own backlog: one tenant's burst does not add workers for another.
func TestEachTenantHasItsOwnPool(t *testing.T) {
	e := newEnv(t, "reviewer", "acme-pharma")
	e.set.Set(settings.Values{QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3})
	for i := 0; i < 20; i++ { // a big backlog for reviewer only, held back so it stays a backlog
		e.send(t, "reviewer", 2)
	}
	if _, err := e.st.Pool().Exec(ctx, `UPDATE ingest_jobs SET next_attempt_at = now() + interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	stop := e.run(t)
	defer stop()
	eventually(t, "reviewer scales to its max", func() bool { return e.mgr.Workers()["reviewer"] == 4 })
	if got := e.mgr.Workers()["acme-pharma"]; got != 1 {
		t.Fatalf("acme has no backlog, so it stays at its own minimum: %d", got)
	}

	// While reviewer is swamped, acme's jobs are still processed at once, by acme's own workers.
	id := e.send(t, "acme-pharma", 0)
	eventually(t, "acme job done", func() bool { return e.status("acme-pharma", id) == "flagged" })
}

// A tenant's own settings replace the global ones, and changing them at runtime resizes its pool.
func TestTenantSettingsOverrideTheGlobalOnes(t *testing.T) {
	e := newEnv(t, "reviewer", "acme-pharma")
	global := settings.Values{QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3}
	e.set.Set(global)
	// The Syncer loads each tenant's own settings from the database, exactly as in production.
	sctx, stopSync := context.WithCancel(ctx)
	defer stopSync()
	sy := &settings.Syncer{Source: e.st, Holder: e.set, Base: global, Interval: 20 * time.Millisecond}
	go sy.Run(sctx)

	for k, v := range map[string]string{"workers_min": "3", "workers_max": "3"} {
		if err := e.st.SetTenantSetting(ctx, "acme-pharma", k, v); err != nil {
			t.Fatal(err)
		}
	}
	stop := e.run(t)
	defer stop()
	eventually(t, "acme runs its own minimum", func() bool { return e.mgr.Workers()["acme-pharma"] == 3 })
	if got := e.mgr.Workers()["reviewer"]; got != 1 {
		t.Fatalf("reviewer follows the global values: %d", got)
	}
	// Changing the tenant's setting at runtime resizes its pool.
	for k, v := range map[string]string{"workers_min": "1", "workers_max": "1"} {
		if err := e.st.SetTenantSetting(ctx, "acme-pharma", k, v); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "acme shrinks to its new limit", func() bool { return e.mgr.Workers()["acme-pharma"] == 1 })
	// Removing the override makes the tenant follow the global value again.
	for _, k := range []string{"workers_min", "workers_max"} {
		if err := e.st.ClearTenantSetting(ctx, "acme-pharma", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.st.SetSetting(ctx, "workers_min", "2", "test"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "acme follows the global minimum", func() bool { return e.mgr.Workers()["acme-pharma"] == 2 })
}

// A tenant's own retry limit decides when its poison job is given up on, while others use the global one.
func TestTenantMaxAttemptsOverride(t *testing.T) {
	e := newEnv(t, "reviewer", "acme-pharma")
	global := settings.Values{QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 2, JobsPerWorker: 5, LagSLOSeconds: 10, MaxAttempts: 5}
	e.set.SetAll(global, map[string]settings.Values{"acme-pharma": func() settings.Values { v := global; v.MaxAttempts = 1; return v }()})
	a := e.send(t, "acme-pharma", 2)
	r := e.send(t, "reviewer", 2)
	e.st.fails[a], e.st.fails[r] = -1, -1
	e.mgr.Now = func() time.Time { return time.Now().Add(-time.Hour) }
	stop := e.run(t)
	defer stop()
	eventually(t, "both poison jobs end dead", func() bool {
		return e.status("acme-pharma", a) == "dead" && e.status("reviewer", r) == "dead"
	})
	// acme gives up at once (its limit is 1 attempt: no retry); reviewer uses the global limit of 5 (4 retries, then dead).
	// If acme had used the global limit too, there would be 8 retries.
	if got := e.obs.get("retry"); got != 4 {
		t.Fatalf("retries = %d, want 4 (reviewer's four; acme's own limit leaves none)", got)
	}
	if got := e.obs.get("dead"); got != 2 {
		t.Fatalf("dead = %d", got)
	}
}

// A sync tenant has no queue, so it gets no workers.
func TestSyncTenantsRunNoWorkers(t *testing.T) {
	e := newEnv(t, "reviewer")
	if err := e.st.CreateTenant(ctx, "syncco", "Sync Co", nil); err != nil {
		t.Fatal(err)
	}
	stop := e.run(t)
	defer stop()
	eventually(t, "reviewer runs workers", func() bool { return e.mgr.Workers()["reviewer"] >= 1 })
	if _, has := e.mgr.Workers()["syncco"]; has {
		t.Fatalf("sync tenants have no pool: %v", e.mgr.Workers())
	}
}

func TestTarget(t *testing.T) {
	global := settings.Values{WorkersMin: 2, WorkersMax: 8, JobsPerWorker: 10, LagSLOSeconds: 10}
	cfg := func(raw string) store.TenantInfo { return store.TenantInfo{Status: "active", Config: []byte(raw)} }
	tenantOwn := func(f func(*settings.Values)) settings.Values { v := global; f(&v); return v }
	async := `{"ingest_mode":"async"}`
	cases := []struct {
		name   string
		info   store.TenantInfo
		mode   string
		depth  int
		oldest time.Duration
		s      settings.Values // the tenant's own settings: its overrides on top of the global ones
		want   int
	}{
		{"async, idle: its minimum", cfg(async), "sync", 0, 0, global, 2},
		{"async, backlog: one worker per JobsPerWorker", cfg(async), "sync", 35, 0, global, 4},
		{"async, huge backlog: capped at its max", cfg(async), "sync", 10000, 0, global, 8},
		{"async, behind the lag target: one more", cfg(async), "sync", 30, 20 * time.Second, global, 4},
		{"its own max wins", cfg(async), "sync", 10000, 0, tenantOwn(func(v *settings.Values) { v.WorkersMin, v.WorkersMax = 1, 3 }), 3},
		{"its own minimum when idle", cfg(async), "sync", 0, 0, tenantOwn(func(v *settings.Values) { v.WorkersMin, v.WorkersMax = 5, 6 }), 5},
		{"its own jobs per worker", cfg(async), "sync", 35, 0, tenantOwn(func(v *settings.Values) { v.JobsPerWorker = 5 }), 7},
		{"its own lag target", cfg(async), "sync", 30, 20 * time.Second, tenantOwn(func(v *settings.Values) { v.LagSLOSeconds = 60 }), 3},
		{"sync and idle: none", cfg(`{}`), "sync", 0, 0, global, 0},
		{"mode from the service default", cfg(`{}`), "async", 0, 0, global, 2},
		{"switched to sync with jobs left over: drain them", cfg(`{"ingest_mode":"sync"}`), "async", 25, 0, global, 3},
		{"suspended: none", store.TenantInfo{Status: "suspended", Config: []byte(async)}, "sync", 50, 0, global, 0},
	}
	for _, c := range cases {
		if got := worker.Target(c.info, c.mode, c.depth, c.oldest, c.s); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
