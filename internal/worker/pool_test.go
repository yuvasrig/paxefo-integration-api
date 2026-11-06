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

func (o *obs) JobFinished(r string)             { o.mu.Lock(); o.results[r]++; o.mu.Unlock() }
func (o *obs) QueueState([]store.QueueRow, int) {}
func (o *obs) get(r string) int                 { o.mu.Lock(); defer o.mu.Unlock(); return o.results[r] }

type env struct {
	st   *flaky
	svc  *ingest.Service
	pool *worker.Pool
	obs  *obs
	set  *settings.Holder
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
	pool := &worker.Pool{
		Source: st, Processor: svc, Settings: set, Instance: "test", Role: "all", Observer: o,
		Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), PollInterval: 10 * time.Millisecond,
		ControlEvery: 20 * time.Millisecond, HeartbeatEvery: 20 * time.Millisecond, ScaleDownAfter: 50 * time.Millisecond,
		StuckAfter: time.Minute, Now: time.Now,
	}
	return &env{st: st, svc: svc, pool: pool, obs: o, set: set}
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
	go func() { e.pool.Run(c); close(done) }()
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
	hbs, _ := e.st.ActiveHeartbeats(ctx, time.Minute)
	if len(hbs) != 1 || hbs[0].Workers < 2 {
		t.Fatalf("the pool reports itself: %+v", hbs)
	}
}

func TestFailingJobIsRetriedThenSucceeds(t *testing.T) {
	e := newEnv(t, "reviewer")
	id := e.send(t, "reviewer", 2)
	e.st.fails[id] = 1
	// Backoff is 2s, so make retries immediate by moving the clock the pool uses.
	e.pool.Now = func() time.Time { return time.Now().Add(-time.Hour) }
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
	e.pool.Now = func() time.Time { return time.Now().Add(-time.Hour) }
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
	e.pool.Processor = panicker{e.svc}
	e.pool.Now = func() time.Time { return time.Now().Add(-time.Hour) }
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
	eventually(t, "scale up to max", func() bool { return e.pool.Workers() == 4 })
	if _, err := e.st.Pool().Exec(ctx, `DELETE FROM ingest_jobs`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "scale down to min", func() bool { return e.pool.Workers() == 1 })
}

func TestShutdownFinishesTheClaimedBatchAndRemovesTheHeartbeat(t *testing.T) {
	e := newEnv(t, "reviewer")
	for i := 0; i < 8; i++ {
		e.send(t, "reviewer", 2)
	}
	stop := e.run(t)
	eventually(t, "some work started", func() bool { return e.pool.Processed() > 0 })
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
