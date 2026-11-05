package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// SaveRaw is transaction 1: the raw payload and one ledger row, committed before anything else (F1-R2).
// It does no validation, so even garbage bytes are kept.
func (s *PGStore) SaveRaw(ctx context.Context, ev RawEvent) error {
	// jsonb cannot hold \u0000; the exact bytes are always kept in payload_raw regardless.
	var payload any
	if json.Valid(ev.Raw) && !bytes.Contains(ev.Raw, []byte(`\u0000`)) {
		payload = json.RawMessage(ev.Raw)
	}
	return s.WithTenantTx(ctx, ev.TenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO raw_events (id, tenant_id, submitted_by_tenant_id, provider, payload_raw, payload, payload_hash, received_at, request_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			ev.ID, ev.TenantID, ev.SubmittedBy, ev.Provider, ev.Raw, payload, ev.Hash, ev.ReceivedAt, ev.RequestID)
		if err != nil {
			return fmt.Errorf("store: insert raw event: %w", err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ingest_ledger (raw_event_id, tenant_id) VALUES ($1, $2)`, ev.ID, ev.TenantID); err != nil {
			return fmt.Errorf("store: insert ledger: %w", err)
		}
		return nil
	})
}

// Commit is transaction 2. Inside one transaction it decides duplicate / conflict for the reading key,
// writes the normalized rows (first write wins, F4), upserts device and shipment, and records the outcome.
func (s *PGStore) Commit(ctx context.Context, in CommitInput) (CommitResult, error) {
	var out CommitResult
	err := s.WithTenantTx(ctx, in.TenantID, func(tx pgx.Tx) error {
		res := in.Result
		out = CommitResult{Status: res.Status, Flags: append([]rules.Flag(nil), res.Flags...)}

		if res.Status != provider.StatusRejected {
			if err := s.writeReadings(ctx, tx, in, &out); err != nil {
				return err
			}
		}
		return upsertOutcome(ctx, tx, in, out)
	})
	if err != nil {
		return CommitResult{}, err
	}
	return out, nil
}

// readingKey returns the (imei, ts) of the result's records.
func readingKey(res provider.Result) (imei string, ts int64) {
	if res.Sensor != nil {
		return res.Sensor.DeviceIMEI, res.Sensor.Timestamp
	}
	return res.Location.DeviceIMEI, res.Location.Timestamp
}

func (s *PGStore) writeReadings(ctx context.Context, tx pgx.Tx, in CommitInput, out *CommitResult) error {
	res := in.Result
	imei, ts := readingKey(res)

	// Serialize concurrent writers of the same key; the unique index remains the backstop (F4-R4).
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%s|%s|%d", in.TenantID, imei, ts)); err != nil {
		return fmt.Errorf("store: lock key: %w", err)
	}

	var first string
	err := tx.QueryRow(ctx, `
		SELECT raw_event_id FROM sensor_readings   WHERE tenant_id = $1 AND device_imei = $2 AND ts = $3
		UNION ALL
		SELECT raw_event_id FROM location_readings WHERE tenant_id = $1 AND device_imei = $2 AND ts = $3
		LIMIT 1`, in.TenantID, imei, ts).Scan(&first)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return insertReadings(ctx, tx, in)
	case err != nil:
		return fmt.Errorf("store: look up key: %w", err)
	case first == in.RawEventID:
		return nil // replay of an event whose rows already exist: idempotent
	}

	out.FirstRawEventID = first
	var firstHash string
	var firstRaw []byte
	if err := tx.QueryRow(ctx, `SELECT payload_hash, payload_raw FROM raw_events WHERE id = $1`, first).Scan(&firstHash, &firstRaw); err != nil {
		return fmt.Errorf("store: load first event: %w", err)
	}
	var thisHash string
	var thisRaw []byte
	if err := tx.QueryRow(ctx, `SELECT payload_hash, payload_raw FROM raw_events WHERE id = $1`, in.RawEventID).Scan(&thisHash, &thisRaw); err != nil {
		return fmt.Errorf("store: load this event: %w", err)
	}
	if firstHash == thisHash {
		out.Status = StatusDuplicate // F4-R2
		return nil
	}

	// F4-R3: keep the first, store this version as a conflict.
	out.Status = StatusConflict
	out.Flags = append(out.Flags, rules.Flag{
		Code: rules.CodeDuplicateConflict, Severity: rules.SeverityWarn, Field: "(tenant_id,device_imei,ts)", Action: rules.ActionKept,
	})
	diff, err := json.Marshal(payloadDiff(firstRaw, thisRaw))
	if err != nil {
		return fmt.Errorf("store: marshal diff: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO reading_conflicts (tenant_id, device_imei, ts, raw_event_id, first_raw_event_id, diff)
		VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (raw_event_id) DO NOTHING`,
		in.TenantID, imei, ts, in.RawEventID, first, diff)
	if err != nil {
		return fmt.Errorf("store: insert conflict: %w", err)
	}
	return nil
}

func insertReadings(ctx context.Context, tx pgx.Tx, in CommitInput) error {
	res := in.Result
	flags, err := json.Marshal(res.Flags)
	if err != nil {
		return fmt.Errorf("store: marshal flags: %w", err)
	}

	if r := res.Sensor; r != nil {
		extras, _ := json.Marshal(res.Aux.SensorExtras)
		var src *string
		if r.TemperatureSource != "" {
			src = &r.TemperatureSource
		}
		var ax, ay, az, ag *float64
		if r.Accelerometer != nil {
			ax, ay, az, ag = r.Accelerometer.X, r.Accelerometer.Y, r.Accelerometer.Z, r.Accelerometer.Magnitude
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO sensor_readings (tenant_id, device_imei, ts, device_id, temperature, temperature_source, probe_temperature,
				humidity, light_lux, accel_x, accel_y, accel_z, accel_g, quality_flags, extras, raw_event_id, transform_version)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
			in.TenantID, r.DeviceIMEI, r.Timestamp, r.DeviceID, r.Temperature, src, res.Aux.ProbeTemperature,
			r.Humidity, r.LightLevel, ax, ay, az, ag, flags, extras, in.RawEventID, res.TransformVersion)
		if err != nil {
			return fmt.Errorf("store: insert sensor reading: %w", err)
		}
	}

	if l := res.Location; l != nil {
		extras, _ := json.Marshal(res.Aux.LocationExtras)
		var street, locality, state, country, postal, full *string
		if a := l.Address; a != nil {
			street, locality, state, country, postal, full = a.Street, a.Locality, a.State, a.Country, a.PostalCode, a.FullAddress
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO location_readings (tenant_id, device_imei, ts, device_id, lat, lon, accuracy_m, accuracy_category, source,
				street, locality, state, country, postal_code, full_address, battery_pct, cellular_dbm, wifi_aps,
				quality_flags, extras, raw_event_id, transform_version)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
			in.TenantID, l.DeviceIMEI, l.Timestamp, l.DeviceID, l.Latitude, l.Longitude, l.LocationAccuracy, l.AccuracyCategory, l.LocationSource,
			street, locality, state, country, postal, full, l.BatteryLevel, l.CellularDbm, l.WifiAccessPoints,
			flags, extras, in.RawEventID, res.TransformVersion)
		if err != nil {
			return fmt.Errorf("store: insert location reading: %w", err)
		}
	}

	imei, ts := readingKey(res)
	name := res.Sensor
	deviceName := ""
	if name != nil {
		deviceName = name.DeviceID
	} else {
		deviceName = res.Location.DeviceID
	}
	var lat, lon *float64
	if res.Location != nil {
		lat, lon = &res.Location.Latitude, &res.Location.Longitude
	}
	if err := upsertDevice(ctx, tx, in.TenantID, imei, deviceName, ts, lat, lon); err != nil {
		return err
	}
	return upsertShipment(ctx, tx, in.TenantID, res.Aux.Shipment)
}

// upsertDevice registers the device on first sight and moves last_* forward only when the reading is newer
// (processing order is not event order). If the IMEI is already assigned to another tenant it is left alone;
// moving a tracker is an operator action (ReassignDevice).
func upsertDevice(ctx context.Context, tx pgx.Tx, tenantID, imei, name string, ts int64, lat, lon *float64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO devices (tenant_id, imei, device_name, valid_from, first_seen, last_seen, last_lat, last_lon, last_ts)
		VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000), to_timestamp($4::double precision / 1000),
		        to_timestamp($4::double precision / 1000), $5, $6, $4)
		ON CONFLICT DO NOTHING`, tenantID, imei, name, ts, lat, lon)
	if err != nil {
		return fmt.Errorf("store: insert device: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE devices SET first_seen = LEAST(first_seen, to_timestamp($3::double precision / 1000))
		WHERE tenant_id = $1 AND imei = $2 AND valid_to IS NULL`, tenantID, imei, ts)
	if err != nil {
		return fmt.Errorf("store: update device first_seen: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE devices SET device_name = $3, last_seen = to_timestamp($4::double precision / 1000),
			last_lat = COALESCE($5, last_lat), last_lon = COALESCE($6, last_lon), last_ts = $4
		WHERE tenant_id = $1 AND imei = $2 AND valid_to IS NULL AND (last_ts IS NULL OR last_ts < $4)`,
		tenantID, imei, name, ts, lat, lon)
	if err != nil {
		return fmt.Errorf("store: update device last_*: %w", err)
	}
	return nil
}

func upsertShipment(ctx context.Context, tx pgx.Tx, tenantID string, sh *provider.Shipment) error {
	if sh == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO shipments (tenant_id, shipment_id, public_id, carrier, description, from_lat, from_lon, from_address, to_lat, to_lon, to_address)
		VALUES ($1, $2, NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), $6, $7, NULLIF($8,''), $9, $10, NULLIF($11,''))
		ON CONFLICT (tenant_id, shipment_id) DO UPDATE SET
			public_id    = COALESCE(EXCLUDED.public_id, shipments.public_id),
			carrier      = COALESCE(EXCLUDED.carrier, shipments.carrier),
			description  = COALESCE(EXCLUDED.description, shipments.description),
			from_lat     = COALESCE(EXCLUDED.from_lat, shipments.from_lat),
			from_lon     = COALESCE(EXCLUDED.from_lon, shipments.from_lon),
			from_address = COALESCE(EXCLUDED.from_address, shipments.from_address),
			to_lat       = COALESCE(EXCLUDED.to_lat, shipments.to_lat),
			to_lon       = COALESCE(EXCLUDED.to_lon, shipments.to_lon),
			to_address   = COALESCE(EXCLUDED.to_address, shipments.to_address),
			updated_at   = now()`,
		tenantID, sh.ID, sh.PublicID, sh.Carrier, sh.Description, sh.FromLat, sh.FromLon, sh.FromAddress, sh.ToLat, sh.ToLon, sh.ToAddress)
	if err != nil {
		return fmt.Errorf("store: upsert shipment: %w", err)
	}
	return nil
}

func upsertOutcome(ctx context.Context, tx pgx.Tx, in CommitInput, out CommitResult) error {
	res := in.Result
	reasons, _ := json.Marshal(nonNil(res.Errors))
	flags, _ := json.Marshal(nonNilFlags(out.Flags))
	outputs, err := json.Marshal(map[string]any{"sensor": res.Sensor, "location": res.Location})
	if err != nil {
		return fmt.Errorf("store: marshal outputs: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO event_outcomes (raw_event_id, tenant_id, submitted_by_tenant_id, status, reasons, flags, expected_outputs, transform_version, rules_version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (raw_event_id) DO UPDATE SET status = EXCLUDED.status, reasons = EXCLUDED.reasons, flags = EXCLUDED.flags,
			expected_outputs = EXCLUDED.expected_outputs, transform_version = EXCLUDED.transform_version,
			rules_version = EXCLUDED.rules_version, processed_at = now(), attempts = event_outcomes.attempts + 1, error = ''`,
		in.RawEventID, in.TenantID, in.SubmittedBy, out.Status, reasons, flags, outputs, res.TransformVersion, res.RulesVersion)
	if err != nil {
		return fmt.Errorf("store: upsert outcome: %w", err)
	}
	return nil
}

func nonNil(p []rules.Problem) []rules.Problem {
	if p == nil {
		return []rules.Problem{}
	}
	return p
}

func nonNilFlags(f []rules.Flag) []rules.Flag {
	if f == nil {
		return []rules.Flag{}
	}
	return f
}

// RecordFailure marks an event failed after its raw payload was saved (F7-R1). Replay can recover it.
func (s *PGStore) RecordFailure(ctx context.Context, tenantID, submittedBy, rawEventID, message string) error {
	return s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO event_outcomes (raw_event_id, tenant_id, submitted_by_tenant_id, status, error)
			VALUES ($1, $2, $3, 'failed', $4)
			ON CONFLICT (raw_event_id) DO UPDATE SET status = 'failed', error = EXCLUDED.error,
				processed_at = now(), attempts = event_outcomes.attempts + 1`,
			rawEventID, tenantID, submittedBy, message)
		if err != nil {
			return fmt.Errorf("store: record failure: %w", err)
		}
		return nil
	})
}

// GetEvent returns an event and its outcome as seen by viewerTenant: the owner and the submitter can read it.
func (s *PGStore) GetEvent(ctx context.Context, viewerTenant, id string) (*EventRecord, error) {
	var ev EventRecord
	err := s.WithTenantTx(ctx, viewerTenant, func(tx pgx.Tx) error {
		var status, tv, rv, errMsg *string
		var reasons, flags, outputs []byte
		var processed *time.Time
		var attempts *int
		err := tx.QueryRow(ctx, `
			SELECT r.id, r.tenant_id, r.submitted_by_tenant_id, r.provider, r.request_id, r.payload_raw, r.received_at,
			       o.status, o.reasons, o.flags, o.expected_outputs, o.transform_version, o.rules_version, o.processed_at, o.attempts, o.error
			FROM raw_events r LEFT JOIN event_outcomes o ON o.raw_event_id = r.id
			WHERE r.id = $1`, id).Scan(
			&ev.ID, &ev.TenantID, &ev.SubmittedBy, &ev.Provider, &ev.RequestID, &ev.Raw, &ev.ReceivedAt,
			&status, &reasons, &flags, &outputs, &tv, &rv, &processed, &attempts, &errMsg)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("store: get event: %w", err)
		}
		if status == nil {
			return nil
		}
		ev.HasOutcome, ev.Status = true, *status
		ev.TransformVersion, ev.RulesVersion, ev.Error = deref(tv), deref(rv), deref(errMsg)
		if attempts != nil {
			ev.Attempts = *attempts
		}
		if processed != nil {
			ev.ProcessedAt = *processed
		}
		_ = json.Unmarshal(reasons, &ev.Reasons)
		_ = json.Unmarshal(flags, &ev.Flags)
		var o struct {
			Sensor   json.RawMessage `json:"sensor"`
			Location json.RawMessage `json:"location"`
		}
		if err := json.Unmarshal(outputs, &o); err == nil {
			ev.Sensor, ev.Location = nullToNil(o.Sensor), nullToNil(o.Location)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func nullToNil(m json.RawMessage) json.RawMessage {
	if string(m) == "null" {
		return nil
	}
	return m
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// payloadDiff lists the fields that differ between two raw payloads, for reading_conflicts.diff.
func payloadDiff(first, second []byte) map[string]map[string]any {
	diff := map[string]map[string]any{}
	a, errA := doc.Parse(first)
	b, errB := doc.Parse(second)
	if errA != nil || errB != nil {
		diff["(payload)"] = map[string]any{"first": "unparsable", "second": "unparsable"}
		return diff
	}
	fa, fb := map[string]any{}, map[string]any{}
	flatten("", map[string]any(a), fa)
	flatten("", map[string]any(b), fb)
	for k, va := range fa {
		if vb, ok := fb[k]; !ok || fmt.Sprint(va) != fmt.Sprint(vb) {
			diff[k] = map[string]any{"first": va, "second": fb[k]}
		}
	}
	for k, vb := range fb {
		if _, ok := fa[k]; !ok {
			diff[k] = map[string]any{"first": nil, "second": vb}
		}
	}
	return diff
}

func flatten(prefix string, m map[string]any, out map[string]any) {
	for k, v := range m {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		if sub, ok := v.(map[string]any); ok {
			flatten(path, sub, out)
			continue
		}
		out[path] = v
	}
}
