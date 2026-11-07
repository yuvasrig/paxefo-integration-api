package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PayloadFilter selects the PAXAFE payloads (the sensor and location records the API produced) one tenant has stored.
// Every filter narrows the selection; an empty one is not applied.
type PayloadFilter struct {
	TenantID   string
	TimeField  string    // TimeReceived (default) or TimeEvent (the reading's own time)
	From, To   time.Time // From inclusive, To exclusive; the zero time leaves that end open
	Device     string    // a device id (name) or IMEI, from the payload or from the PAXAFE record
	Status     string    // an outcome status; "pending" = not processed yet; "" = any
	OnlyOutput bool      // only events that produced a PAXAFE sensor or location record
	Limit      int       // page size (default 25, at most 200)
	After      *PayloadCursor
}

// PayloadCursor continues a listing after the last row returned (newest first).
type PayloadCursor struct {
	ReceivedAt time.Time
	ID         string
}

// PayloadRow is one stored event with the PAXAFE payload it produced.
type PayloadRow struct {
	ID         string
	ReceivedAt time.Time
	Status     string // "pending" when it has no outcome yet
	Device     string // the device as the payload names it
	Flags      []string
	Reasons    []string
	Raw        json.RawMessage // the Tive payload as received
	Sensor     json.RawMessage // the PAXAFE sensor record, or nil
	Location   json.RawMessage // the PAXAFE location record, or nil
}

const (
	defaultPayloadPage = 25
	maxPayloadPage     = 200
)

// ListPayloads returns one page of a tenant's payloads, newest received first, and the cursor for the next (nil when
// this is the last page). It reads under the tenant's row-level security, so it can only ever see that tenant's rows.
func (s *PGStore) ListPayloads(ctx context.Context, f PayloadFilter) (rows []PayloadRow, next *PayloadCursor, err error) {
	limit := f.Limit
	switch {
	case limit <= 0:
		limit = defaultPayloadPage
	case limit > maxPayloadPage:
		limit = maxPayloadPage
	}
	field := f.TimeField
	if field == "" {
		field = TimeReceived
	}
	if field != TimeReceived && field != TimeEvent {
		return nil, nil, fmt.Errorf("store: time field must be %q or %q (got %q)", TimeReceived, TimeEvent, field)
	}
	var from, to any // nil leaves an end open
	if !f.From.IsZero() {
		from = f.From
	}
	if !f.To.IsZero() {
		to = f.To
	}
	// The cursor parameters: far in the future when there is none, so "(received_at, id) < (cursor)" keeps everything.
	cursorAt, cursorID := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), "\U0010FFFF"
	if f.After != nil {
		cursorAt, cursorID = f.After.ReceivedAt, f.After.ID
	}

	err = s.WithTenantTx(ctx, f.TenantID, func(tx pgx.Tx) error {
		q, err := tx.Query(ctx, `
			SELECT r.id, r.received_at, COALESCE(o.status, 'pending'),
			       COALESCE(r.payload->>'DeviceName', r.payload->>'DeviceId', ''),
			       o.flags, o.reasons, o.expected_outputs, r.payload
			FROM raw_events r
			LEFT JOIN event_outcomes o ON o.raw_event_id = r.id
			LEFT JOIN LATERAL (
				SELECT CASE WHEN jsonb_typeof(r.payload->'EntryTimeEpoch') = 'number' THEN
				            CASE WHEN (r.payload->>'EntryTimeEpoch')::numeric < $11 THEN (r.payload->>'EntryTimeEpoch')::numeric * 1000
				                 ELSE (r.payload->>'EntryTimeEpoch')::numeric END
				       END AS ms
			) ev ON true
			WHERE r.tenant_id = $1
			  AND (
			        ($2 = 'received' AND ($3::timestamptz IS NULL OR r.received_at >= $3) AND ($4::timestamptz IS NULL OR r.received_at < $4))
			     OR ($2 = 'event' AND ev.ms IS NOT NULL
			         AND ($3::timestamptz IS NULL OR ev.ms >= extract(epoch FROM $3::timestamptz) * 1000)
			         AND ($4::timestamptz IS NULL OR ev.ms < extract(epoch FROM $4::timestamptz) * 1000))
			      )
			  AND ($5 = '' OR r.payload->>'DeviceId' = $5 OR r.payload->>'DeviceName' = $5
			       OR o.expected_outputs->'sensor'->>'device_id' = $5 OR o.expected_outputs->'sensor'->>'device_imei' = $5
			       OR o.expected_outputs->'location'->>'device_id' = $5 OR o.expected_outputs->'location'->>'device_imei' = $5)
			  AND ($6 = '' OR COALESCE(o.status, 'pending') = $6)
			  AND (NOT $7 OR jsonb_typeof(o.expected_outputs->'sensor') = 'object' OR jsonb_typeof(o.expected_outputs->'location') = 'object')
			  AND (r.received_at, r.id) < ($8, $9)
			ORDER BY r.received_at DESC, r.id DESC
			LIMIT $10`,
			f.TenantID, field, from, to, f.Device, f.Status, f.OnlyOutput, cursorAt, cursorID, limit+1, int64(secondsBelowMs))
		if err != nil {
			return fmt.Errorf("store: list payloads: %w", err)
		}
		defer q.Close()
		for q.Next() {
			var row PayloadRow
			var flags, reasons, outputs, raw []byte
			if err := q.Scan(&row.ID, &row.ReceivedAt, &row.Status, &row.Device, &flags, &reasons, &outputs, &raw); err != nil {
				return fmt.Errorf("store: scan payload: %w", err)
			}
			row.Raw = raw
			var fl []struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(flags, &fl)
			for _, x := range fl {
				row.Flags = append(row.Flags, x.Code)
			}
			var rs []struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(reasons, &rs)
			for _, x := range rs {
				row.Reasons = append(row.Reasons, x.Code)
			}
			var o struct {
				Sensor   json.RawMessage `json:"sensor"`
				Location json.RawMessage `json:"location"`
			}
			if json.Unmarshal(outputs, &o) == nil {
				row.Sensor, row.Location = nullToNil(o.Sensor), nullToNil(o.Location)
			}
			rows = append(rows, row)
		}
		return q.Err()
	})
	if err != nil {
		return nil, nil, err
	}
	if len(rows) > limit { // one extra row was read to know whether another page exists
		rows = rows[:limit]
		last := rows[len(rows)-1]
		next = &PayloadCursor{ReceivedAt: last.ReceivedAt, ID: last.ID}
	}
	return rows, next, nil
}
