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

// JobSource is the queue operations a tenant's workers need (implemented by *store.PGStore).
type JobSource interface {
	ClaimJobs(ctx context.Context, worker, tenantID string, batch int) ([]store.Job, error)
	RetryJob(ctx context.Context, rawEventID, errMsg string, next time.Time) error
	BuryJob(ctx context.Context, tenantID, rawEventID, errMsg string) error
}

// Processor runs one claimed job through the ingest pipeline. A nil error means the job is done (its outcome is
// written and the job removed in the same transaction); an error means it failed and should be retried.
type Processor interface {
	ProcessJob(ctx context.Context, job store.Job) error
}

// Pool is the workers of ONE tenant in this process. Every tenant has its own pool, so a tenant's backlog is worked
// off by its own workers and one tenant's load can neither starve nor slow another's. The Manager creates the pools
// and sets how many workers each runs.
type Pool struct {
	Tenant    string
	Source    JobSource
	Processor Processor
	Settings  *settings.Holder // for MaxAttempts
	Instance  string
	Log       *slog.Logger
	Observer  interface{ JobFinished(result string) } // may be nil

	PollInterval time.Duration // wait when the tenant's queue is empty (default 200ms)
	Batch        int           // jobs claimed per round (default 10)
	Now          func() time.Time

	mu        sync.Mutex
	cancels   []context.CancelFunc
	wg        sync.WaitGroup
	inFlight  atomic.Int64
	processed atomic.Int64
}

func (p *Pool) defaults() {
	if p.PollInterval <= 0 {
		p.PollInterval = 200 * time.Millisecond
	}
	if p.Batch <= 0 {
		p.Batch = 10
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
}

// Workers is how many workers this pool runs right now.
func (p *Pool) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.cancels)
}

// InFlight is how many jobs its workers are processing right now.
func (p *Pool) InFlight() int { return int(p.inFlight.Load()) }

// Processed is how many jobs this pool has finished.
func (p *Pool) Processed() int64 { return p.processed.Load() }

// Resize sets the number of workers to n. A worker told to stop finishes the jobs it already claimed first.
func (p *Pool) Resize(ctx context.Context, n int) {
	p.defaults()
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.cancels) < n {
		wctx, cancel := context.WithCancel(ctx)
		id := fmt.Sprintf("%s/%s/w%d", p.Instance, p.Tenant, len(p.cancels)+1)
		p.cancels = append(p.cancels, cancel)
		p.wg.Add(1)
		go p.work(wctx, id)
	}
	for len(p.cancels) > n {
		last := len(p.cancels) - 1
		p.cancels[last]()
		p.cancels = p.cancels[:last]
	}
}

// Stop tells every worker to stop and waits until they have finished their claimed jobs.
func (p *Pool) Stop() {
	p.mu.Lock()
	for _, c := range p.cancels {
		c()
	}
	p.cancels = nil
	p.mu.Unlock()
	p.wg.Wait()
}

// work is one worker: claim a batch of this tenant's jobs, process each, repeat; wait a little when the queue is empty.
func (p *Pool) work(ctx context.Context, id string) {
	defer p.wg.Done()
	for ctx.Err() == nil {
		jobs, err := p.Source.ClaimJobs(ctx, id, p.Tenant, p.Batch)
		if err != nil {
			if ctx.Err() == nil {
				p.Log.Error("could not claim jobs", "worker", id, "tenant_id", p.Tenant, "error", err)
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

	if j.Attempts >= p.Settings.For(j.TenantID).MaxAttempts {
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
