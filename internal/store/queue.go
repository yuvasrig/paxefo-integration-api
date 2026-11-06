package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Job is a queued payload waiting for, or being given to, a worker.
type Job struct {
	RawEventID string
	TenantID   string
	Attempts   int // including the current attempt
	EnqueuedAt time.Time
}

// PendingDepth counts a tenant's jobs waiting to be processed, counting at most cap+1 so the cost stays bounded
// however large the backlog (used for backpressure).
func (s *PGStore) PendingDepth(ctx context.Context, tenantID string, cap int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (SELECT 1 FROM ingest_jobs WHERE tenant_id = $1 AND status = 'pending' LIMIT $2) j`, tenantID, cap+1).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: pending depth: %w", err)
	}
	return n, nil
}

// ClaimJobs hands a worker up to batch ready jobs, at most perTenant from any one tenant, so a tenant with a large
// backlog cannot starve the others. FOR UPDATE SKIP LOCKED lets many workers claim at once without waiting on each other.
func (s *PGStore) ClaimJobs(ctx context.Context, worker string, batch, perTenant int) ([]Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		WITH ready AS (
			SELECT DISTINCT tenant_id FROM ingest_jobs WHERE status = 'pending' AND next_attempt_at <= now()
		), picked AS (
			SELECT j.raw_event_id FROM ready t
			CROSS JOIN LATERAL (
				SELECT raw_event_id FROM ingest_jobs
				WHERE tenant_id = t.tenant_id AND status = 'pending' AND next_attempt_at <= now()
				ORDER BY next_attempt_at, raw_event_id
				LIMIT $2 FOR UPDATE SKIP LOCKED
			) j
			LIMIT $1
		)
		UPDATE ingest_jobs SET status = 'processing', attempts = attempts + 1, locked_at = now(), locked_by = $3
		WHERE raw_event_id IN (SELECT raw_event_id FROM picked)
		RETURNING raw_event_id, tenant_id, attempts, enqueued_at`, batch, perTenant, worker)
	if err != nil {
		return nil, fmt.Errorf("store: claim jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.RawEventID, &j.TenantID, &j.Attempts, &j.EnqueuedAt); err != nil {
			return nil, fmt.Errorf("store: scan job: %w", err)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RetryJob puts a failed job back in the queue to be tried again at next.
func (s *PGStore) RetryJob(ctx context.Context, rawEventID, errMsg string, next time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		UPDATE ingest_jobs SET status = 'pending', next_attempt_at = $3, last_error = $2, locked_at = NULL, locked_by = NULL
		WHERE raw_event_id = $1`, rawEventID, errMsg, next)
	if err != nil {
		return fmt.Errorf("store: retry job: %w", err)
	}
	return nil
}

// BuryJob gives up on a job that failed every attempt: the event's outcome becomes 'dead' and the job is removed, in
// one transaction. The raw payload stays, and `api replay` or the replay endpoint can bring it back.
func (s *PGStore) BuryJob(ctx context.Context, tenantID, rawEventID, errMsg string) error {
	return s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO event_outcomes (raw_event_id, tenant_id, status, error) VALUES ($1, $2, 'dead', $3)
			ON CONFLICT (raw_event_id) DO UPDATE SET status = 'dead', error = EXCLUDED.error, processed_at = now(),
				attempts = event_outcomes.attempts + 1`, rawEventID, tenantID, errMsg)
		if err != nil {
			return fmt.Errorf("store: bury job outcome: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM ingest_jobs WHERE raw_event_id = $1`, rawEventID); err != nil {
			return fmt.Errorf("store: bury job: %w", err)
		}
		return nil
	})
}

// RescueStuckJobs returns jobs a dead worker left in 'processing' for longer than olderThan to the queue.
func (s *PGStore) RescueStuckJobs(ctx context.Context, olderThan time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE ingest_jobs SET status = 'pending', next_attempt_at = now(), locked_at = NULL, locked_by = NULL,
			last_error = 'worker did not finish; returned to the queue'
		WHERE status = 'processing' AND locked_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("store: rescue jobs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// HasJob reports whether the event is still in the queue.
func (s *PGStore) HasJob(ctx context.Context, rawEventID string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var ok bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM ingest_jobs WHERE raw_event_id = $1)`, rawEventID).Scan(&ok); err != nil {
		return false, fmt.Errorf("store: has job: %w", err)
	}
	return ok, nil
}

// QueueRow is one tenant's queue state.
type QueueRow struct {
	TenantID         string
	Pending          int
	Processing       int
	OldestPendingAge time.Duration // how long the oldest ready job has waited
}

// QueueStats returns the queue state per tenant that has jobs.
func (s *PGStore) QueueStats(ctx context.Context) ([]QueueRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT tenant_id,
		       count(*) FILTER (WHERE status = 'pending'),
		       count(*) FILTER (WHERE status = 'processing'),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(enqueued_at) FILTER (WHERE status = 'pending')), 0)
		FROM ingest_jobs GROUP BY tenant_id ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("store: queue stats: %w", err)
	}
	defer rows.Close()
	var out []QueueRow
	for rows.Next() {
		var r QueueRow
		var age float64
		if err := rows.Scan(&r.TenantID, &r.Pending, &r.Processing, &age); err != nil {
			return nil, fmt.Errorf("store: scan queue row: %w", err)
		}
		r.OldestPendingAge = time.Duration(age * float64(time.Second))
		out = append(out, r)
	}
	return out, rows.Err()
}

// Heartbeat is a worker process reporting in.
type Heartbeat struct {
	Instance       string
	Role           string
	Workers        int
	InFlight       int
	ProcessedTotal int64
	StartedAt      time.Time
	LastSeen       time.Time
}

// ReportHeartbeat records that a process runs Workers workers right now.
func (s *PGStore) ReportHeartbeat(ctx context.Context, h Heartbeat) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO worker_heartbeats (instance, role, workers, in_flight, processed_total, started_at, last_seen)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (instance) DO UPDATE SET role = EXCLUDED.role, workers = EXCLUDED.workers, in_flight = EXCLUDED.in_flight,
			processed_total = EXCLUDED.processed_total, last_seen = now()`,
		h.Instance, h.Role, h.Workers, h.InFlight, h.ProcessedTotal, h.StartedAt)
	if err != nil {
		return fmt.Errorf("store: heartbeat: %w", err)
	}
	return nil
}

// ActiveHeartbeats lists processes that reported within the last `within`.
func (s *PGStore) ActiveHeartbeats(ctx context.Context, within time.Duration) ([]Heartbeat, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT instance, role, workers, in_flight, processed_total, started_at, last_seen FROM worker_heartbeats
		WHERE last_seen > now() - make_interval(secs => $1) ORDER BY instance`, within.Seconds())
	if err != nil {
		return nil, fmt.Errorf("store: heartbeats: %w", err)
	}
	defer rows.Close()
	var out []Heartbeat
	for rows.Next() {
		var h Heartbeat
		if err := rows.Scan(&h.Instance, &h.Role, &h.Workers, &h.InFlight, &h.ProcessedTotal, &h.StartedAt, &h.LastSeen); err != nil {
			return nil, fmt.Errorf("store: scan heartbeat: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// RemoveHeartbeat deletes a process's row on clean shutdown, so it stops counting at once.
func (s *PGStore) RemoveHeartbeat(ctx context.Context, instance string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `DELETE FROM worker_heartbeats WHERE instance = $1`, instance); err != nil {
		return fmt.Errorf("store: remove heartbeat: %w", err)
	}
	return nil
}

// TenantIngestMode returns the tenant's own ingest mode ("sync" or "async") from its config, or "" if it has none.
func (s *PGStore) TenantIngestMode(ctx context.Context, tenantID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var mode *string
	err := s.pool.QueryRow(ctx, `SELECT config->>'ingest_mode' FROM tenants WHERE id = $1`, tenantID).Scan(&mode)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: tenant ingest mode: %w", err)
	}
	if mode == nil {
		return "", nil
	}
	return *mode, nil
}

// SetTenantIngestMode stores the tenant's ingest mode ("sync" or "async"), leaving the rest of its config alone.
func (s *PGStore) SetTenantIngestMode(ctx context.Context, tenantID, mode string) error {
	if mode != "sync" && mode != "async" {
		return fmt.Errorf("store: ingest mode must be sync or async (got %q)", mode)
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE tenants SET config = jsonb_set(CASE WHEN jsonb_typeof(config) = 'object' THEN config ELSE '{}'::jsonb END, '{ingest_mode}', to_jsonb($2::text))
		WHERE id = $1`, tenantID, mode)
	if err != nil {
		return fmt.Errorf("store: set ingest mode: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
