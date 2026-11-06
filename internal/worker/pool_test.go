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
	set := settings.NewHolder(settings.Values{RateLimitPerSec: 100, RateLimitBurst: 100, QueueMaxDepth: 1000, WorkersMin: 2, WorkersMax: 4, JobsPerWorker: 5, LagSLOSeconds: 10, MaxAttempts: 3})
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
	e.set.Set(settings.Values{RateLimitPerSec: 100, RateLimitBurst: 100, QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3})
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
	e.set.Set(settings.Values{RateLimitPerSec: 100, RateLimitBurst: 100, QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3})
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

// A tenant's own limits replace the service-wide ones.
func TestTenantLimitsOverrideTheDefaults(t *testing.T) {
	e := newEnv(t, "reviewer", "acme-pharma")
	e.set.Set(settings.Values{RateLimitPerSec: 100, RateLimitBurst: 100, QueueMaxDepth: 1000, WorkersMin: 1, WorkersMax: 4, JobsPerWorker: 2, LagSLOSeconds: 10, MaxAttempts: 3})
	l, _ := store.NewWorkerLimits(3, 3)
	if err := e.st.SetTenantWorkers(ctx, "acme-pharma", &l); err != nil {
		t.Fatal(err)
	}
	stop := e.run(t)
	defer stop()
	eventually(t, "acme runs its own minimum", func() bool { return e.mgr.Workers()["acme-pharma"] == 3 })
	if got := e.mgr.Workers()["reviewer"]; got != 1 {
		t.Fatalf("reviewer keeps the defaults: %d", got)
	}
	// Changing the limits at runtime resizes the pool.
	l2, _ := store.NewWorkerLimits(1, 1)
	if err := e.st.SetTenantWorkers(ctx, "acme-pharma", &l2); err != nil {
		t.Fatal(err)
	}
	eventually(t, "acme shrinks to its new limit", func() bool { return e.mgr.Workers()["acme-pharma"] == 1 })
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
	s := settings.Values{WorkersMin: 2, WorkersMax: 8, JobsPerWorker: 10, LagSLOSeconds: 10}
	cfg := func(raw string) store.TenantInfo { return store.TenantInfo{Status: "active", Config: []byte(raw)} }
	cases := []struct {
		name   string
		info   store.TenantInfo
		mode   string
		depth  int
		oldest time.Duration
		want   int
	}{
		{"async, idle: its minimum", cfg(`{"ingest_mode":"async"}`), "sync", 0, 0, 2},
		{"async, backlog: one worker per JobsPerWorker", cfg(`{"ingest_mode":"async"}`), "sync", 35, 0, 4},
		{"async, huge backlog: capped at its max", cfg(`{"ingest_mode":"async"}`), "sync", 10000, 0, 8},
		{"async, behind the lag target: one more", cfg(`{"ingest_mode":"async"}`), "sync", 30, 20 * time.Second, 4},
		{"its own limits win", cfg(`{"ingest_mode":"async","workers":{"min":1,"max":3}}`), "sync", 10000, 0, 3},
		{"its own minimum when idle", cfg(`{"ingest_mode":"async","workers":{"min":5,"max":6}}`), "sync", 0, 0, 5},
		{"invalid limits fall back to the defaults", cfg(`{"ingest_mode":"async","workers":{"min":9,"max":3}}`), "sync", 0, 0, 2},
		{"sync and idle: none", cfg(`{}`), "sync", 0, 0, 0},
		{"mode from the service default", cfg(`{}`), "async", 0, 0, 2},
		{"switched to sync with jobs left over: drain them", cfg(`{"ingest_mode":"sync"}`), "async", 25, 0, 3},
		{"suspended: none", store.TenantInfo{Status: "suspended", Config: []byte(`{"ingest_mode":"async"}`)}, "sync", 50, 0, 0},
	}
	for _, c := range cases {
		if got := worker.Target(c.info, c.mode, c.depth, c.oldest, s); got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
