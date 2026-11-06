package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// Source is the queue the pool works from (implemented by *store.PGStore).
type Source interface {
	ClaimJobs(ctx context.Context, worker string, batch, perTenant int) ([]store.Job, error)
	QueueStats(ctx context.Context) ([]store.QueueRow, error)
	RetryJob(ctx context.Context, rawEventID, errMsg string, next time.Time) error
	BuryJob(ctx context.Context, tenantID, rawEventID, errMsg string) error
	RescueStuckJobs(ctx context.Context, olderThan time.Duration) (int, error)
	ReportHeartbeat(ctx context.Context, h store.Heartbeat) error
	RemoveHeartbeat(ctx context.Context, instance string) error
}

// Processor runs one claimed job through the ingest pipeline. A nil error means the job is done (its outcome is
// written and the job removed in the same transaction); an error means it failed and should be retried.
type Processor interface {
	ProcessJob(ctx context.Context, job store.Job) error
}

// Observer receives pool activity for metrics. It may be nil.
type Observer interface {
	JobFinished(result string) // "done", "retry" or "dead"
	QueueState(rows []store.QueueRow, workers int)
}

// Pool is the elastic worker pool of one process. Its size follows the backlog (Desired); it scales up at once and
// down slowly, so a burst is met quickly and a lull does not make the pool flap.
type Pool struct {
	Source    Source
	Processor Processor
	Settings  *settings.Holder
	Instance  string // identifies this process in the heartbeat table
	Role      string
	Log       *slog.Logger
	Observer  Observer

	PollInterval   time.Duration // wait when the queue is empty (default 200ms)
	ControlEvery   time.Duration // how often the pool is resized (default 5s)
	HeartbeatEvery time.Duration // how often this process reports in (default 5s)
	ScaleDownAfter time.Duration // how long the target must stay lower before shrinking (default 60s)
	StuckAfter     time.Duration // a job claimed longer ago than this is returned to the queue (default 5m)
	Batch          int           // jobs claimed per round (default 10)
	PerTenant      int           // most from one tenant per round (default 5)
	Now            func() time.Time

	mu        sync.Mutex
	cancels   []context.CancelFunc
	wg        sync.WaitGroup
	belowFrom time.Time
	started   time.Time
	inFlight  atomic.Int64
	processed atomic.Int64
}

func (p *Pool) defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	set(&p.PollInterval, 200*time.Millisecond)
	set(&p.ControlEvery, 5*time.Second)
	set(&p.HeartbeatEvery, 5*time.Second)
	set(&p.ScaleDownAfter, 60*time.Second)
	set(&p.StuckAfter, 5*time.Minute)
	if p.Batch <= 0 {
		p.Batch = 10
	}
	if p.PerTenant <= 0 {
		p.PerTenant = 5
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
}

// Workers is how many workers this process runs right now.
func (p *Pool) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.cancels)
}

// Processed is how many jobs this process has finished.
func (p *Pool) Processed() int64 { return p.processed.Load() }

// Run starts the minimum number of workers, then resizes the pool, reports in and rescues stuck jobs until ctx ends.
// On shutdown each worker finishes the jobs it already claimed, so Run returns when they are done.
func (p *Pool) Run(ctx context.Context) {
	p.defaults()
	p.started = p.Now()
	p.resize(ctx, p.Settings.Current().WorkersMin)
	p.heartbeat(ctx)

	control := time.NewTicker(p.ControlEvery)
	beat := time.NewTicker(p.HeartbeatEvery)
	rescue := time.NewTicker(p.StuckAfter / 2)
	defer control.Stop()
	defer beat.Stop()
	defer rescue.Stop()
	for {
		select {
		case <-ctx.Done():
			p.stopAll()
			p.wg.Wait()
			rm, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = p.Source.RemoveHeartbeat(rm, p.Instance) // stop counting at once rather than after the timeout
			cancel()
			return
		case <-control.C:
			p.control(ctx)
		case <-beat.C:
			p.heartbeat(ctx)
		case <-rescue.C:
			if n, err := p.Source.RescueStuckJobs(ctx, p.StuckAfter); err != nil {
				p.Log.Error("could not rescue stuck jobs", "error", err)
			} else if n > 0 {
				p.Log.Warn("returned stuck jobs to the queue", "jobs", n)
			}
		}
	}
}

// control resizes the pool from the backlog.
func (p *Pool) control(ctx context.Context) {
	rows, err := p.Source.QueueStats(ctx)
	if err != nil {
		p.Log.Error("could not read queue stats; keeping the pool size", "error", err)
		return
	}
	depth, oldest := 0, time.Duration(0)
	for _, r := range rows {
		depth += r.Pending
		if r.OldestPendingAge > oldest {
			oldest = r.OldestPendingAge
		}
	}
	s := p.Settings.Current()
	target := Desired(depth, oldest, SizeConfig{Min: s.WorkersMin, Max: s.WorkersMax, JobsPerWorker: s.JobsPerWorker, LagSLO: time.Duration(s.LagSLOSeconds) * time.Second})
	cur := p.Workers()
	switch {
	case target > cur: // scale up immediately
		p.belowFrom = time.Time{}
		p.resize(ctx, target)
	case target < cur: // scale down one at a time, and only after the target has stayed lower for a while
		now := p.Now()
		if p.belowFrom.IsZero() {
			p.belowFrom = now
		}
		if now.Sub(p.belowFrom) >= p.ScaleDownAfter {
			p.resize(ctx, cur-1)
			p.belowFrom = now
		}
	default:
		p.belowFrom = time.Time{}
	}
	if p.Observer != nil {
		p.Observer.QueueState(rows, p.Workers())
	}
}

// resize sets the number of workers to n.
func (p *Pool) resize(ctx context.Context, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.cancels) < n {
		wctx, cancel := context.WithCancel(ctx)
		id := fmt.Sprintf("%s/w%d", p.Instance, len(p.cancels)+1)
		p.cancels = append(p.cancels, cancel)
		p.wg.Add(1)
		go p.work(wctx, id)
	}
	for len(p.cancels) > n {
		last := len(p.cancels) - 1
		p.cancels[last]() // that worker finishes its current batch, then exits
		p.cancels = p.cancels[:last]
	}
}

func (p *Pool) stopAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.cancels {
		c()
	}
	p.cancels = nil
}

func (p *Pool) heartbeat(ctx context.Context) {
	err := p.Source.ReportHeartbeat(ctx, store.Heartbeat{
		Instance: p.Instance, Role: p.Role, Workers: p.Workers(), InFlight: int(p.inFlight.Load()),
		ProcessedTotal: p.processed.Load(), StartedAt: p.started,
	})
	if err != nil && ctx.Err() == nil {
		p.Log.Error("could not report worker heartbeat", "error", err)
	}
}

// work is one worker: claim a batch, process each job, repeat; wait a little when the queue is empty.
func (p *Pool) work(ctx context.Context, id string) {
	defer p.wg.Done()
	for ctx.Err() == nil {
		jobs, err := p.Source.ClaimJobs(ctx, id, p.Batch, p.PerTenant)
		if err != nil {
			if ctx.Err() == nil {
				p.Log.Error("could not claim jobs", "worker", id, "error", err)
			}
			p.sleep(ctx, time.Second)
			continue
		}
		if len(jobs) == 0 {
			p.sleep(ctx, p.PollInterval)
			continue
		}
		// A claimed batch is always finished, even if this worker is told to stop meanwhile: the jobs are ours.
		jobCtx := context.WithoutCancel(ctx)
		for _, j := range jobs {
			p.run(jobCtx, j)
		}
	}
}

func (p *Pool) run(ctx context.Context, j store.Job) {
	p.inFlight.Add(1)
	defer p.inFlight.Add(-1)

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil { // a panic in processing must not kill the worker or lose the job
				err = fmt.Errorf("panic while processing: %v", r)
			}
		}()
		return p.Processor.ProcessJob(ctx, j)
	}()
	if err == nil {
		p.processed.Add(1)
		p.finished("done")
		return
	}

	s := p.Settings.Current()
	if j.Attempts >= s.MaxAttempts {
		p.Log.Error("job failed every attempt: marked dead", "event_id", j.RawEventID, "tenant_id", j.TenantID, "attempts", j.Attempts, "error", err)
		if berr := p.Source.BuryJob(ctx, j.TenantID, j.RawEventID, err.Error()); berr != nil {
			p.Log.Error("could not mark job dead", "event_id", j.RawEventID, "error", berr)
		}
		p.finished("dead")
		return
	}
	next := p.Now().Add(Backoff(j.Attempts))
	p.Log.Warn("job failed; will retry", "event_id", j.RawEventID, "tenant_id", j.TenantID, "attempt", j.Attempts, "retry_at", next, "error", err)
	if rerr := p.Source.RetryJob(ctx, j.RawEventID, err.Error(), next); rerr != nil {
		p.Log.Error("could not requeue job", "event_id", j.RawEventID, "error", rerr)
	}
	p.finished("retry")
}

func (p *Pool) finished(result string) {
	if p.Observer != nil {
		p.Observer.JobFinished(result)
	}
}

func (p *Pool) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
