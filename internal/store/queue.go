package store

import (
	"context"
	"encoding/json"
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

// ClaimJobs hands one tenant's worker up to batch of that tenant's ready jobs, oldest first. Each tenant has its own
// workers, so a worker never takes another tenant's job. FOR UPDATE SKIP LOCKED lets that tenant's workers claim at
// once without waiting on each other, and never hand one job to two of them.
func (s *PGStore) ClaimJobs(ctx context.Context, worker, tenantID string, batch int) ([]Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT raw_event_id FROM ingest_jobs
			WHERE tenant_id = $1 AND status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at, raw_event_id
			LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		UPDATE ingest_jobs j SET status = 'processing', attempts = attempts + 1, locked_at = now(), locked_by = $3
		FROM picked WHERE j.raw_event_id = picked.raw_event_id
		RETURNING j.raw_event_id, j.tenant_id, j.attempts, j.enqueued_at`, tenantID, batch, worker)
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
	TenantID       string // the tenant whose pool this row describes
	Role           string
	Workers        int
	InFlight       int
	ProcessedTotal int64
	StartedAt      time.Time
	LastSeen       time.Time
}

// ReportHeartbeat records that a process runs Workers workers for one tenant right now.
func (s *PGStore) ReportHeartbeat(ctx context.Context, h Heartbeat) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO worker_heartbeats (instance, tenant_id, role, workers, in_flight, processed_total, started_at, last_seen)
		VALUES ($1, $7, $2, $3, $4, $5, $6, now())
		ON CONFLICT (instance, tenant_id) DO UPDATE SET role = EXCLUDED.role, workers = EXCLUDED.workers, in_flight = EXCLUDED.in_flight,
			processed_total = EXCLUDED.processed_total, last_seen = now()`,
		h.Instance, h.Role, h.Workers, h.InFlight, h.ProcessedTotal, h.StartedAt, h.TenantID)
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
		SELECT instance, tenant_id, role, workers, in_flight, processed_total, started_at, last_seen FROM worker_heartbeats
		WHERE last_seen > now() - make_interval(secs => $1) ORDER BY instance, tenant_id`, within.Seconds())
	if err != nil {
		return nil, fmt.Errorf("store: heartbeats: %w", err)
	}
	defer rows.Close()
	var out []Heartbeat
	for rows.Next() {
		var h Heartbeat
		if err := rows.Scan(&h.Instance, &h.TenantID, &h.Role, &h.Workers, &h.InFlight, &h.ProcessedTotal, &h.StartedAt, &h.LastSeen); err != nil {
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

// ParseIngestMode reads a tenant's own ingest mode from its config ("" when it has none).
func ParseIngestMode(config []byte) string {
	var c struct {
		IngestMode string `json:"ingest_mode"`
	}
	if json.Unmarshal(config, &c) != nil || (c.IngestMode != "sync" && c.IngestMode != "async") {
		return ""
	}
	return c.IngestMode
}

// SetTenantSetting stores one of the tenant's own runtime settings (tenants.config.settings), leaving the rest of its
// config alone. The caller validates key and value (package settings).
func (s *PGStore) SetTenantSetting(ctx context.Context, tenantID, key, value string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE tenants SET config = jsonb_set(
			CASE WHEN jsonb_typeof(config) = 'object' THEN config ELSE '{}'::jsonb END, '{settings}',
			COALESCE(CASE WHEN jsonb_typeof(config->'settings') = 'object' THEN config->'settings' END, '{}'::jsonb) || jsonb_build_object($2::text, $3::text))
		WHERE id = $1`, tenantID, key, value)
	if err != nil {
		return fmt.Errorf("store: set tenant setting: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ClearTenantSetting removes one of the tenant's own settings, so it uses the global value again.
func (s *PGStore) ClearTenantSetting(ctx context.Context, tenantID, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE tenants SET config = CASE WHEN jsonb_typeof(config) = 'object' THEN config #- ARRAY['settings', $2::text] ELSE config END
		WHERE id = $1`, tenantID, key)
	if err != nil {
		return fmt.Errorf("store: clear tenant setting: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
