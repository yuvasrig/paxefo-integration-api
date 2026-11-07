package store

import (
	"context"
	"fmt"
	"time"
)

// AlertRecord is one occurrence of an alert: open until ClearedAt is set.
type AlertRecord struct {
	ID        int64
	Key       string
	Severity  string
	TenantID  string
	Title     string
	Detail    string
	StartedAt time.Time
	LastSeen  time.Time
	ClearedAt *time.Time // nil while the alert is still firing
}

// AlertState is one alert that is firing now.
type AlertState struct{ Key, Severity, TenantID, Title, Detail string }

// SyncAlerts makes the history match the alerts firing now: an alert with no open row gets one, an open row whose alert
// is no longer firing is closed, and a still-firing alert has its detail and last-seen time refreshed. It is safe to
// call from several instances at once: they converge on the same rows.
func (s *PGStore) SyncAlerts(ctx context.Context, firing []AlertState) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	keys := make([]string, 0, len(firing))
	for _, a := range firing {
		keys = append(keys, a.Key)
		_, err := s.pool.Exec(ctx, `
			INSERT INTO alert_history (key, severity, tenant_id, title, detail)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (key) WHERE cleared_at IS NULL
			DO UPDATE SET severity = EXCLUDED.severity, detail = EXCLUDED.detail, last_seen = now()`,
			a.Key, a.Severity, a.TenantID, a.Title, a.Detail)
		if err != nil {
			return fmt.Errorf("store: record alert: %w", err)
		}
	}
	if _, err := s.pool.Exec(ctx, `UPDATE alert_history SET cleared_at = now() WHERE cleared_at IS NULL AND NOT (key = ANY($1))`, keys); err != nil {
		return fmt.Errorf("store: close alerts: %w", err)
	}
	return nil
}

// ListAlertHistory returns the most recent alert occurrences, firing ones first and then newest first.
func (s *PGStore) ListAlertHistory(ctx context.Context, limit int) ([]AlertRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT id, key, severity, tenant_id, title, detail, started_at, last_seen, cleared_at
		FROM alert_history
		ORDER BY (cleared_at IS NULL) DESC, started_at DESC, id DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list alert history: %w", err)
	}
	defer rows.Close()
	var out []AlertRecord
	for rows.Next() {
		var a AlertRecord
		if err := rows.Scan(&a.ID, &a.Key, &a.Severity, &a.TenantID, &a.Title, &a.Detail, &a.StartedAt, &a.LastSeen, &a.ClearedAt); err != nil {
			return nil, fmt.Errorf("store: scan alert: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
