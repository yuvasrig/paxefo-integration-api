package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// Which timestamp a replay window is measured on.
const (
	TimeReceived = "received" // when we received the payload (raw_events.received_at); every event has one
	TimeEvent    = "event"    // the reading's own time, EntryTimeEpoch in the payload; events without a numeric epoch are not selected
)

// secondsBelowMs mirrors the TS-SECONDS rule: an epoch below this is in seconds. It is used only to pick events by
// event time; the pipeline itself still applies the configured rules.
const secondsBelowMs = 1_000_000_000_000

// ReplayFilter selects stored events for reprocessing. Every filter narrows the selection; an empty one is not applied.
type ReplayFilter struct {
	TenantID  string
	TimeField string    // TimeReceived (default) or TimeEvent
	From, To  time.Time // From inclusive, To exclusive
	Device    string    // the payload's DeviceId (IMEI) or DeviceName
	Account   string    // the payload's AccountId (metadata)
	Statuses  []string  // current outcome status; "pending" = no outcome yet
}

// ReplayEvent is a stored event as a replay sees it, with its current outcome.
type ReplayEvent struct {
	ID           string
	ReceivedAt   time.Time
	Raw          []byte
	Status       string // "pending" when the event has no outcome
	Flags        []rules.Flag
	Outputs      json.RawMessage // the outcome's expected_outputs ({sensor, location})
	RulesVersion string
}

// ReplayCursor continues a listing after the last event returned.
type ReplayCursor struct {
	ReceivedAt time.Time
	ID         string
}

// ListReplayEvents returns up to limit events matching the filter, oldest received first (so "last received wins"
// holds when they are replayed in this order), after the cursor. next is the cursor for the following page;
// done is true when there are no more.
func (s *PGStore) ListReplayEvents(ctx context.Context, f ReplayFilter, after ReplayCursor, limit int) (events []ReplayEvent, next ReplayCursor, done bool, err error) {
	if limit <= 0 {
		limit = 200
	}
	field := f.TimeField
	if field == "" {
		field = TimeReceived
	}
	if field != TimeReceived && field != TimeEvent {
		return nil, after, true, fmt.Errorf("store: time field must be %q or %q (got %q)", TimeReceived, TimeEvent, field)
	}
	statuses := f.Statuses
	if statuses == nil {
		statuses = []string{}
	}

	err = s.WithTenantTx(ctx, f.TenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT r.id, r.received_at, r.payload_raw, COALESCE(o.status, 'pending'), o.flags, o.expected_outputs, COALESCE(o.rules_version, '')
			FROM raw_events r
			LEFT JOIN event_outcomes o ON o.raw_event_id = r.id
			LEFT JOIN LATERAL (
				SELECT CASE WHEN jsonb_typeof(r.payload->'EntryTimeEpoch') = 'number' THEN
				            CASE WHEN (r.payload->>'EntryTimeEpoch')::numeric < $13 THEN (r.payload->>'EntryTimeEpoch')::numeric * 1000
				                 ELSE (r.payload->>'EntryTimeEpoch')::numeric END
				       END AS ms
			) ev ON true
			WHERE r.tenant_id = $1
			  AND (
			        ($2 = 'received' AND r.received_at >= $3 AND r.received_at < $4)
			     OR ($2 = 'event'    AND ev.ms >= $5 AND ev.ms < $6)
			  )
			  AND ($7 = '' OR r.payload->>'DeviceId' = $7 OR r.payload->>'DeviceName' = $7)
			  AND ($8 = '' OR r.payload->>'AccountId' = $8)
			  AND (cardinality($9::text[]) = 0 OR COALESCE(o.status, 'pending') = ANY($9::text[]))
			  AND (r.received_at, r.id) > ($10, $11)
			ORDER BY r.received_at, r.id
			LIMIT $12`,
			f.TenantID, field, f.From, f.To, f.From.UnixMilli(), f.To.UnixMilli(), f.Device, f.Account, statuses,
			after.ReceivedAt, after.ID, limit+1, int64(secondsBelowMs))
		if err != nil {
			return fmt.Errorf("store: list replay events: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var e ReplayEvent
			var flags, outputs []byte
			if err := rows.Scan(&e.ID, &e.ReceivedAt, &e.Raw, &e.Status, &flags, &outputs, &e.RulesVersion); err != nil {
				return fmt.Errorf("store: scan replay event: %w", err)
			}
			_ = json.Unmarshal(flags, &e.Flags)
			e.Outputs = outputs
			events = append(events, e)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, after, true, err
	}
	if len(events) > limit {
		events = events[:limit]
		last := events[len(events)-1]
		return events, ReplayCursor{ReceivedAt: last.ReceivedAt, ID: last.ID}, false, nil
	}
	return events, after, true, nil
}

// ListTenants returns every tenant id, for `api replay --all-tenants`.
func (s *PGStore) ListTenants(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id FROM tenants ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("store: list tenants: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan tenant: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReplayRun is the audit record of one applied replay for one tenant.
type ReplayRun struct {
	RunID       string
	TenantID    string
	RequestedBy string
	Params      map[string]any
	StartedAt   time.Time
	FinishedAt  time.Time
	Selected    int
	Reprocessed int
	Changed     int
	Errors      int
	Transitions map[string]int // "conflict -> accepted": 3
}

// RecordReplayRun stores the audit record of an applied replay.
func (s *PGStore) RecordReplayRun(ctx context.Context, r ReplayRun) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	params, _ := json.Marshal(r.Params)
	trans, _ := json.Marshal(r.Transitions)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO replay_runs (run_id, tenant_id, requested_by, params, started_at, finished_at, selected, reprocessed, changed, errors, transitions)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		r.RunID, r.TenantID, r.RequestedBy, params, r.StartedAt, r.FinishedAt, r.Selected, r.Reprocessed, r.Changed, r.Errors, trans)
	if err != nil {
		return fmt.Errorf("store: record replay run: %w", err)
	}
	return nil
}

// ListReplayRuns returns recent applied replays, newest first.
func (s *PGStore) ListReplayRuns(ctx context.Context, limit int) ([]ReplayRun, error) {
	if limit <= 0 {
		limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT run_id, tenant_id, requested_by, params, started_at, finished_at, selected, reprocessed, changed, errors, transitions
		FROM replay_runs ORDER BY finished_at DESC, run_id DESC, tenant_id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list replay runs: %w", err)
	}
	defer rows.Close()
	var out []ReplayRun
	for rows.Next() {
		var r ReplayRun
		var params, trans []byte
		if err := rows.Scan(&r.RunID, &r.TenantID, &r.RequestedBy, &params, &r.StartedAt, &r.FinishedAt, &r.Selected, &r.Reprocessed, &r.Changed, &r.Errors, &trans); err != nil {
			return nil, fmt.Errorf("store: scan replay run: %w", err)
		}
		_ = json.Unmarshal(params, &r.Params)
		_ = json.Unmarshal(trans, &r.Transitions)
		out = append(out, r)
	}
	return out, rows.Err()
}
