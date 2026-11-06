package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// TenantStats is what the admin console shows for one tenant. Event and device figures come from the tenant's own
// rows (read under row-level security); queue figures come from the work queue.
type TenantStats struct {
	TenantID                string
	TotalEvents             int
	EventsLastMinute        int            // payloads received in the last 60 s
	ActiveDevicesLastMinute int            // distinct devices among them
	CompletedLastMinute     int            // events whose outcome was written in the last 60 s
	Statuses15m             map[string]int // outcomes written in the last 15 min, by status
	Pending                 int            // queued, waiting for a worker
	Processing              int            // claimed by a worker right now
	OldestPendingAge        time.Duration  // how long the oldest waiting job has waited
	Dead                    int            // events whose job failed every attempt
	LatencyP50, LatencyP95  time.Duration  // received -> outcome written, for outcomes of the last 5 minutes
}

// TenantStats gathers the figures for one tenant.
func (s *PGStore) TenantStats(ctx context.Context, tenantID string) (TenantStats, error) {
	st := TenantStats{TenantID: tenantID, Statuses15m: map[string]int{}}
	err := s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM raw_events WHERE tenant_id = $1`, tenantID).Scan(&st.TotalEvents); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			SELECT count(*), count(DISTINCT COALESCE(payload->>'DeviceId', payload->>'DeviceName'))
			FROM raw_events WHERE tenant_id = $1 AND received_at > now() - interval '1 minute'`, tenantID).
			Scan(&st.EventsLastMinute, &st.ActiveDevicesLastMinute); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM event_outcomes WHERE tenant_id = $1 AND processed_at > now() - interval '1 minute'`, tenantID).
			Scan(&st.CompletedLastMinute); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM event_outcomes WHERE tenant_id = $1 AND status = 'dead'`, tenantID).Scan(&st.Dead); err != nil {
			return err
		}
		var p50, p95 *float64
		if err := tx.QueryRow(ctx, `
			SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY lat), percentile_cont(0.95) WITHIN GROUP (ORDER BY lat)
			FROM (SELECT EXTRACT(EPOCH FROM o.processed_at - r.received_at) AS lat
			      FROM event_outcomes o JOIN raw_events r ON r.id = o.raw_event_id
			      WHERE o.tenant_id = $1 AND o.processed_at > now() - interval '5 minutes') x`, tenantID).Scan(&p50, &p95); err != nil {
			return err
		}
		if p50 != nil && p95 != nil {
			st.LatencyP50 = time.Duration(*p50 * float64(time.Second))
			st.LatencyP95 = time.Duration(*p95 * float64(time.Second))
		}
		rows, err := tx.Query(ctx, `
			SELECT status, count(*) FROM event_outcomes WHERE tenant_id = $1 AND processed_at > now() - interval '15 minutes' GROUP BY status`, tenantID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var n int
			if err := rows.Scan(&status, &n); err != nil {
				return err
			}
			st.Statuses15m[status] = n
		}
		return rows.Err()
	})
	if err != nil {
		return TenantStats{}, fmt.Errorf("store: tenant stats: %w", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var age float64
	err = s.pool.QueryRow(ctx2, `
		SELECT count(*) FILTER (WHERE status = 'pending'), count(*) FILTER (WHERE status = 'processing'),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(enqueued_at) FILTER (WHERE status = 'pending')), 0)
		FROM ingest_jobs WHERE tenant_id = $1`, tenantID).Scan(&st.Pending, &st.Processing, &age)
	if err != nil {
		return TenantStats{}, fmt.Errorf("store: tenant queue stats: %w", err)
	}
	st.OldestPendingAge = time.Duration(age * float64(time.Second))
	return st, nil
}

// RecentEvent is one line of the console's recent-events table.
type RecentEvent struct {
	ID         string
	ReceivedAt time.Time
	Device     string
	Status     string // "pending" while it waits for a worker
	Flags      []string
	Reasons    []string // why it was rejected
	Error      string
}

// RecentEvents returns a tenant's latest events, newest first.
func (s *PGStore) RecentEvents(ctx context.Context, tenantID string, limit int, statuses []string) ([]RecentEvent, error) {
	if limit <= 0 {
		limit = 20
	}
	if statuses == nil {
		statuses = []string{}
	}
	var out []RecentEvent
	err := s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT r.id, r.received_at, COALESCE(r.payload->>'DeviceName', r.payload->>'DeviceId', ''), COALESCE(o.status, 'pending'), o.flags, o.reasons, COALESCE(o.error, '')
			FROM raw_events r LEFT JOIN event_outcomes o ON o.raw_event_id = r.id
			WHERE r.tenant_id = $1 AND (cardinality($3::text[]) = 0 OR COALESCE(o.status, 'pending') = ANY($3::text[]))
			ORDER BY r.received_at DESC, r.id DESC LIMIT $2`, tenantID, limit, statuses)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var e RecentEvent
			var flags, reasons []byte
			if err := rows.Scan(&e.ID, &e.ReceivedAt, &e.Device, &e.Status, &flags, &reasons, &e.Error); err != nil {
				return err
			}
			var fl []struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(flags, &fl)
			for _, f := range fl {
				e.Flags = append(e.Flags, f.Code)
			}
			var rs []struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(reasons, &rs)
			for _, r := range rs {
				e.Reasons = append(e.Reasons, r.Code)
			}
			out = append(out, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("store: recent events: %w", err)
	}
	return out, nil
}

// TenantInfo is a tenant row for the console.
type TenantInfo struct {
	ID         string
	Name       string
	Status     string
	CreatedAt  time.Time
	Config     []byte
	ActiveKeys int
}

// ListTenantInfo returns every tenant with its config and its number of active API keys.
func (s *PGStore) ListTenantInfo(ctx context.Context) ([]TenantInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.name, t.status, t.created_at, COALESCE(t.config, '{}'::jsonb),
		       (SELECT count(*) FROM api_keys k WHERE k.tenant_id = t.id AND k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at > now()))
		FROM tenants t ORDER BY t.id`)
	if err != nil {
		return nil, fmt.Errorf("store: list tenant info: %w", err)
	}
	defer rows.Close()
	var out []TenantInfo
	for rows.Next() {
		var t TenantInfo
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.Config, &t.ActiveKeys); err != nil {
			return nil, fmt.Errorf("store: scan tenant info: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// FieldChange is one payload field whose value differs between the first and the later version of a reading.
type FieldChange struct {
	Field         string
	First, Second string // "–" when that version had no value
}

// ConflictRow is two payloads that claimed the same reading (same device and time) with different content.
type ConflictRow struct {
	DeviceIMEI   string
	TS           int64
	Resolution   string // kept_first, replaced or rejected: what the tenant's policy did
	CreatedAt    time.Time
	EventID      string // the later payload
	FirstEventID string // the payload that owned the reading
	Changes      []FieldChange
	MoreChanges  int // other fields that differ: present in only one version, or beyond the ones listed
}

const maxConflictFields = 8

// ListConflicts returns a tenant's latest conflicts, newest first, each with the fields that differ.
func (s *PGStore) ListConflicts(ctx context.Context, tenantID string, limit int) ([]ConflictRow, error) {
	if limit <= 0 {
		limit = 25
	}
	var out []ConflictRow
	err := s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT device_imei, ts, resolution, created_at, raw_event_id, first_raw_event_id, diff
			FROM reading_conflicts WHERE tenant_id = $1 ORDER BY created_at DESC, raw_event_id DESC LIMIT $2`, tenantID, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c ConflictRow
			var diff []byte
			if err := rows.Scan(&c.DeviceIMEI, &c.TS, &c.Resolution, &c.CreatedAt, &c.EventID, &c.FirstEventID, &diff); err != nil {
				return err
			}
			c.Changes, c.MoreChanges = changesOf(diff)
			out = append(out, c)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("store: list conflicts: %w", err)
	}
	return out, nil
}

// changesOf lists the fields where both versions carry a value and the values differ (capped at maxConflictFields).
// Fields present in only one version are only counted: a later payload that simply omits most fields would otherwise
// bury the values that really changed.
func changesOf(diff []byte) (changes []FieldChange, other int) {
	var m map[string]struct {
		First  any `json:"first"`
		Second any `json:"second"`
	}
	if json.Unmarshal(diff, &m) != nil {
		return nil, 0
	}
	for field, d := range m {
		if d.First == nil || d.Second == nil {
			other++
			continue
		}
		changes = append(changes, FieldChange{Field: field, First: fmt.Sprint(d.First), Second: fmt.Sprint(d.Second)})
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Field < changes[j].Field })
	if len(changes) > maxConflictFields {
		other += len(changes) - maxConflictFields
		changes = changes[:maxConflictFields]
	}
	return changes, other
}
