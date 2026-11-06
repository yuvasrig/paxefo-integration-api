package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// ManagerSource is what the manager reads and writes beyond the jobs themselves (implemented by *store.PGStore).
type ManagerSource interface {
	JobSource
	ListTenantInfo(ctx context.Context) ([]store.TenantInfo, error)
	QueueStats(ctx context.Context) ([]store.QueueRow, error)
	RescueStuckJobs(ctx context.Context, olderThan time.Duration) (int, error)
	ReportHeartbeat(ctx context.Context, h store.Heartbeat) error
	RemoveHeartbeat(ctx context.Context, instance string) error
}

// Observer receives worker activity for metrics. It may be nil.
type Observer interface {
	JobFinished(result string) // "done", "retry" or "dead"
	QueueState(rows []store.QueueRow, workers map[string]int)
}

// Manager runs the workers of one process: a dedicated Pool per tenant, each sized from that tenant's own backlog
// between that tenant's own minimum and maximum (or the service-wide defaults). It also reports the process's
// heartbeats and returns jobs held by dead workers to the queue.
//
// Scale up is immediate, scale down is slow (one worker at a time, after the target has stayed lower for a while),
// so a burst is met quickly and a lull does not make the pool flap.
type Manager struct {
	Source      ManagerSource
	Processor   Processor
	Settings    *settings.Holder
	Instance    string
	Role        string
	DefaultMode string // INGEST_MODE: the mode of tenants that set none of their own
	Log         *slog.Logger
	Observer    Observer

	PollInterval   time.Duration // a tenant's idle workers wait this long between looks at its queue (default 200ms)
	ControlEvery   time.Duration // how often pools are resized (default 1s)
	HeartbeatEvery time.Duration // how often this process reports in (default 5s)
	ScaleDownAfter time.Duration // how long a target must stay lower before a pool shrinks (default 60s)
	StuckAfter     time.Duration // a job claimed longer ago than this is returned to the queue (default 5m)
	Batch          int           // jobs claimed per round (default 10)
	Now            func() time.Time

	mu        sync.Mutex
	pools     map[string]*Pool
	belowFrom map[string]time.Time
	started   time.Time
}

func (m *Manager) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	set(&m.PollInterval, 200*time.Millisecond)
	set(&m.ControlEvery, time.Second)
	set(&m.HeartbeatEvery, 5*time.Second)
	set(&m.ScaleDownAfter, 60*time.Second)
	set(&m.StuckAfter, 5*time.Minute)
	if m.Now == nil {
		m.Now = time.Now
	}
	if m.Log == nil {
		m.Log = slog.Default()
	}
	m.mu.Lock() // Workers() may be called while Run starts
	if m.pools == nil {
		m.pools = map[string]*Pool{}
		m.belowFrom = map[string]time.Time{}
	}
	m.mu.Unlock()
}

// Workers is how many workers each tenant's pool runs right now.
func (m *Manager) Workers() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int, len(m.pools))
	for t, p := range m.pools {
		out[t] = p.Workers()
	}
	return out
}

// TotalWorkers is the sum over tenants.
func (m *Manager) TotalWorkers() int {
	n := 0
	for _, w := range m.Workers() {
		n += w
	}
	return n
}

// Run manages the pools until ctx ends. On shutdown every worker finishes the jobs it already claimed, so Run
// returns when they are done.
func (m *Manager) Run(ctx context.Context) {
	m.defaults()
	m.started = m.Now()
	m.control(ctx)
	m.heartbeat(ctx)

	control := time.NewTicker(m.ControlEvery)
	beat := time.NewTicker(m.HeartbeatEvery)
	rescue := time.NewTicker(m.StuckAfter / 2)
	defer control.Stop()
	defer beat.Stop()
	defer rescue.Stop()
	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			rm, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = m.Source.RemoveHeartbeat(rm, m.Instance) // stop counting at once rather than after the timeout
			cancel()
			return
		case <-control.C:
			m.control(ctx)
		case <-beat.C:
			m.heartbeat(ctx)
		case <-rescue.C:
			if n, err := m.Source.RescueStuckJobs(ctx, m.StuckAfter); err != nil {
				m.Log.Error("could not rescue stuck jobs", "error", err)
			} else if n > 0 {
				m.Log.Warn("returned stuck jobs to the queue", "jobs", n)
			}
		}
	}
}

// Target is the number of workers a tenant's pool should run. s is the tenant's own settings (its overrides on top of
// the global values), so the minimum, maximum, jobs per worker and lag target can all differ per tenant. It is a pure
// function of its arguments, so the rule is easy to test: async tenants run between their min and max; a sync tenant
// has no workers unless jobs are left over from before it was switched.
func Target(info store.TenantInfo, defaultMode string, depth int, oldest time.Duration, s settings.Values) int {
	if info.Status != "active" {
		return 0
	}
	min := s.WorkersMin
	mode := store.ParseIngestMode(info.Config)
	if mode == "" {
		mode = defaultMode
	}
	if mode != "async" {
		min = 0
	}
	return Desired(depth, oldest, SizeConfig{Min: min, Max: s.WorkersMax, JobsPerWorker: s.JobsPerWorker, LagSLO: time.Duration(s.LagSLOSeconds) * time.Second})
}

// control resizes every tenant's pool from its own backlog.
func (m *Manager) control(ctx context.Context) {
	infos, err := m.Source.ListTenantInfo(ctx)
	if err != nil {
		m.Log.Error("could not list tenants; keeping the pools as they are", "error", err)
		return
	}
	rows, err := m.Source.QueueStats(ctx)
	if err != nil {
		m.Log.Error("could not read queue stats; keeping the pools as they are", "error", err)
		return
	}
	queue := map[string]store.QueueRow{}
	for _, r := range rows {
		queue[r.TenantID] = r
	}
	global := m.Settings.Current()

	seen := map[string]bool{}
	targets := map[string]int{}
	for _, info := range infos {
		seen[info.ID] = true
		q := queue[info.ID]
		targets[info.ID] = Target(info, m.DefaultMode, q.Pending, q.OldestPendingAge, m.Settings.For(info.ID))
	}
	for id, n := range CapTotal(targets, global.WorkersTotalMax) { // the process-wide hard cap, on the targets
		m.scale(ctx, id, n)
	}
	m.enforceCap(ctx, global.WorkersTotalMax)
	m.mu.Lock()
	for id, p := range m.pools { // a tenant that no longer exists keeps no workers
		if !seen[id] {
			go p.Stop()
			delete(m.pools, id)
			delete(m.belowFrom, id)
		}
	}
	m.mu.Unlock()
	if m.Observer != nil {
		m.Observer.QueueState(rows, m.Workers())
	}
}

// enforceCap makes the cap hold for the workers actually running: pools shrink slowly toward their targets, and a
// pool that has just grown or not yet shrunk must not carry the process over the limit. Excess workers go at once.
func (m *Manager) enforceCap(ctx context.Context, limit int) {
	if limit <= 0 {
		return
	}
	cur := m.Workers()
	for id, n := range CapTotal(cur, limit) {
		if n >= cur[id] {
			continue
		}
		m.mu.Lock()
		p := m.pools[id]
		m.mu.Unlock()
		if p != nil {
			p.Resize(ctx, n)
		}
	}
}

// scale moves one tenant's pool toward target: up at once, down one worker at a time after ScaleDownAfter.
func (m *Manager) scale(ctx context.Context, tenant string, target int) {
	m.mu.Lock()
	p := m.pools[tenant]
	if p == nil {
		if target == 0 {
			m.mu.Unlock()
			return // no pool until the tenant needs workers
		}
		p = &Pool{
			Tenant: tenant, Source: m.Source, Processor: m.Processor, Settings: m.Settings, Instance: m.Instance,
			Log: m.Log, PollInterval: m.PollInterval, Batch: m.Batch, Now: m.Now,
		}
		if m.Observer != nil {
			p.Observer = m.Observer
		}
		m.pools[tenant] = p
	}
	m.mu.Unlock()

	cur := p.Workers()
	switch {
	case target > cur:
		m.setBelow(tenant, time.Time{})
		p.Resize(ctx, target)
	case target < cur:
		now := m.Now()
		from := m.getBelow(tenant)
		if from.IsZero() {
			from = now
			m.setBelow(tenant, from)
		}
		if now.Sub(from) >= m.ScaleDownAfter {
			p.Resize(ctx, cur-1)
			m.setBelow(tenant, now)
		}
	default:
		m.setBelow(tenant, time.Time{})
	}
}

func (m *Manager) getBelow(t string) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.belowFrom[t]
}
func (m *Manager) setBelow(t string, v time.Time) {
	m.mu.Lock()
	m.belowFrom[t] = v
	m.mu.Unlock()
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	pools := make([]*Pool, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, p)
	}
	m.mu.Unlock()
	var wg sync.WaitGroup
	for _, p := range pools {
		wg.Add(1)
		go func(p *Pool) { defer wg.Done(); p.Stop() }(p)
	}
	wg.Wait()
}

// heartbeat reports one row per tenant pool, so the console can show each tenant's active workers.
func (m *Manager) heartbeat(ctx context.Context) {
	m.mu.Lock()
	pools := make(map[string]*Pool, len(m.pools))
	for t, p := range m.pools {
		pools[t] = p
	}
	m.mu.Unlock()
	if len(pools) == 0 { // keep the process visible even when no tenant needs workers
		pools[""] = &Pool{}
	}
	for t, p := range pools {
		err := m.Source.ReportHeartbeat(ctx, store.Heartbeat{
			Instance: m.Instance, TenantID: t, Role: m.Role, Workers: p.Workers(), InFlight: p.InFlight(),
			ProcessedTotal: p.Processed(), StartedAt: m.started,
		})
		if err != nil && ctx.Err() == nil {
			m.Log.Error("could not report worker heartbeat", "tenant_id", t, "error", err)
		}
	}
}
