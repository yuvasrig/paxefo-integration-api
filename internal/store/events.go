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
			INSERT INTO raw_events (id, tenant_id, provider, payload_raw, payload, payload_hash, received_at, request_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			ev.ID, ev.TenantID, ev.Provider, ev.Raw, payload, ev.Hash, ev.ReceivedAt, ev.RequestID)
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
// writes the normalized rows (conflicts follow the tenant's policy, F4), upserts device and shipment, and
// records the outcome.
func (s *PGStore) Commit(ctx context.Context, in CommitInput) (CommitResult, error) {
	var out CommitResult
	err := s.WithTenantTx(ctx, in.TenantID, func(tx pgx.Tx) error {
		var err error
		out, err = CommitTx(ctx, tx, in)
		return err
	})
	if err != nil {
		return CommitResult{}, err
	}
	return out, nil
}

// CommitTx is Commit inside a transaction the caller owns, so a replay can run many events in one transaction and
// roll it back (a dry run). The transaction must already be scoped to in.TenantID (WithTenantTx).
func CommitTx(ctx context.Context, tx pgx.Tx, in CommitInput) (CommitResult, error) {
	res := in.Result
	out := CommitResult{Status: res.Status, Flags: append([]rules.Flag(nil), res.Flags...)}

	if in.ReplayRunID != "" {
		if err := prepareReprocess(ctx, tx, in); err != nil {
			return CommitResult{}, err
		}
	}
	if res.Status != provider.StatusRejected {
		if err := writeReadings(ctx, tx, in, &out); err != nil {
			return CommitResult{}, err
		}
	}
	if err := upsertOutcome(ctx, tx, in, out); err != nil {
		return CommitResult{}, err
	}
	return out, nil
}

// prepareReprocess makes room for an event to be computed again: it archives the event's previous outcome, then
// removes the readings and conflict row that this event produced. The raw payload is never touched.
func prepareReprocess(ctx context.Context, tx pgx.Tx, in CommitInput) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO event_outcome_history (raw_event_id, tenant_id, replay_run_id, status, reasons, flags, expected_outputs,
			transform_version, rules_version, processed_at, attempts)
		SELECT raw_event_id, tenant_id, $2, status, reasons, flags, expected_outputs, transform_version, rules_version, processed_at, attempts
		FROM event_outcomes WHERE raw_event_id = $1`, in.RawEventID, in.ReplayRunID); err != nil {
		return fmt.Errorf("store: archive outcome: %w", err)
	}

	// Lock the keys this event owns before changing them, like a live write does.
	rows, err := tx.Query(ctx, `
		SELECT device_imei, ts FROM sensor_readings   WHERE raw_event_id = $1
		UNION
		SELECT device_imei, ts FROM location_readings WHERE raw_event_id = $1`, in.RawEventID)
	if err != nil {
		return fmt.Errorf("store: find own readings: %w", err)
	}
	type key struct {
		imei string
		ts   int64
	}
	var keys []key
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.imei, &k.ts); err != nil {
			rows.Close()
			return fmt.Errorf("store: scan own reading: %w", err)
		}
		keys = append(keys, k)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: own readings: %w", err)
	}
	for _, k := range keys {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%s|%s|%d", in.TenantID, k.imei, k.ts)); err != nil {
			return fmt.Errorf("store: lock key: %w", err)
		}
	}
	for _, table := range []string{"sensor_readings", "location_readings", "reading_conflicts"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE raw_event_id = $1", in.RawEventID); err != nil {
			return fmt.Errorf("store: remove own rows from %s: %w", table, err)
		}
	}
	return nil
}

// readingKey returns the (imei, ts) of the result's records.
func readingKey(res provider.Result) (imei string, ts int64) {
	if res.Sensor != nil {
		return res.Sensor.DeviceIMEI, res.Sensor.Timestamp
	}
	return res.Location.DeviceIMEI, res.Location.Timestamp
}

func writeReadings(ctx context.Context, tx pgx.Tx, in CommitInput, out *CommitResult) error {
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
	var firstAt time.Time
	if err := tx.QueryRow(ctx, `SELECT payload_hash, payload_raw, received_at FROM raw_events WHERE id = $1`, first).Scan(&firstHash, &firstRaw, &firstAt); err != nil {
		return fmt.Errorf("store: load first event: %w", err)
	}
	var thisHash string
	var thisRaw []byte
	var thisAt time.Time
	if err := tx.QueryRow(ctx, `SELECT payload_hash, payload_raw, received_at FROM raw_events WHERE id = $1`, in.RawEventID).Scan(&thisHash, &thisRaw, &thisAt); err != nil {
		return fmt.Errorf("store: load this event: %w", err)
	}
	if firstHash == thisHash {
		out.Status = StatusDuplicate // F4-R2
		return nil
	}

	// Same key, different content: the tenant's conflict policy decides (F4-R3, F4-R7, F4-R8).
	settings := conflictSettings(ctx, tx, in.TenantID)
	out.Status = StatusConflict
	out.ConflictHTTPStatus = settings.HTTPStatus
	flagAction, resolution := rules.ActionKept, ResolutionKeptFirst
	replace := false
	switch settings.Policy {
	case PolicyKeepLast:
		// "Last" means last received. A live payload always is, but a replay can bring back an older event, which
		// must not overwrite a reading that a later event produced.
		if replace = !thisAt.Before(firstAt); replace {
			flagAction, resolution = rules.ActionReplaced, ResolutionReplaced
		}
	case PolicyReject:
		flagAction, resolution = rules.ActionRejected, ResolutionRejected
	}
	out.Flags = append(out.Flags, rules.Flag{
		Code: rules.CodeDuplicateConflict, Severity: rules.SeverityWarn, Field: "(tenant_id,device_imei,ts)", Action: flagAction,
	})

	diff, err := json.Marshal(payloadDiff(firstRaw, thisRaw))
	if err != nil {
		return fmt.Errorf("store: marshal diff: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO reading_conflicts (tenant_id, device_imei, ts, raw_event_id, first_raw_event_id, diff, resolution)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (raw_event_id) DO NOTHING`,
		in.TenantID, imei, ts, in.RawEventID, first, diff, resolution)
	if err != nil {
		return fmt.Errorf("store: insert conflict: %w", err)
	}

	if !replace {
		return nil
	}
	// keep_last: the new version replaces the stored one in this transaction. The replaced event stays archived
	// (raw_events is append-only) and is named by first_raw_event_id above.
	for _, table := range []string{"sensor_readings", "location_readings"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE tenant_id = $1 AND device_imei = $2 AND ts = $3", in.TenantID, imei, ts); err != nil {
			return fmt.Errorf("store: replace reading in %s: %w", table, err)
		}
	}
	if err := insertReadings(ctx, tx, in); err != nil {
		return err
	}
	// The device's last position is "only if newer"; a replaced reading has the same timestamp, so refresh it explicitly.
	if res.Location != nil {
		_, err = tx.Exec(ctx, `
			UPDATE devices SET last_lat = $4, last_lon = $5
			WHERE tenant_id = $1 AND imei = $2 AND valid_to IS NULL AND last_ts = $3`,
			in.TenantID, imei, ts, res.Location.Latitude, res.Location.Longitude)
		if err != nil {
			return fmt.Errorf("store: refresh device position: %w", err)
		}
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
		INSERT INTO event_outcomes (raw_event_id, tenant_id, status, reasons, flags, expected_outputs, transform_version, rules_version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (raw_event_id) DO UPDATE SET status = EXCLUDED.status, reasons = EXCLUDED.reasons, flags = EXCLUDED.flags,
			expected_outputs = EXCLUDED.expected_outputs, transform_version = EXCLUDED.transform_version,
			rules_version = EXCLUDED.rules_version, processed_at = now(), attempts = event_outcomes.attempts + 1, error = ''`,
		in.RawEventID, in.TenantID, out.Status, reasons, flags, outputs, res.TransformVersion, res.RulesVersion)
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
func (s *PGStore) RecordFailure(ctx context.Context, tenantID, rawEventID, message string) error {
	return s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO event_outcomes (raw_event_id, tenant_id, status, error)
			VALUES ($1, $2, 'failed', $3)
			ON CONFLICT (raw_event_id) DO UPDATE SET status = 'failed', error = EXCLUDED.error,
				processed_at = now(), attempts = event_outcomes.attempts + 1`,
			rawEventID, tenantID, message)
		if err != nil {
			return fmt.Errorf("store: record failure: %w", err)
		}
		return nil
	})
}

// GetEvent returns an event and its outcome; only the owning tenant can read it.
func (s *PGStore) GetEvent(ctx context.Context, viewerTenant, id string) (*EventRecord, error) {
	var ev EventRecord
	err := s.WithTenantTx(ctx, viewerTenant, func(tx pgx.Tx) error {
		var status, tv, rv, errMsg *string
		var reasons, flags, outputs []byte
		var processed *time.Time
		var attempts *int
		err := tx.QueryRow(ctx, `
			SELECT r.id, r.tenant_id, r.provider, r.request_id, r.payload_raw, r.received_at,
			       o.status, o.reasons, o.flags, o.expected_outputs, o.transform_version, o.rules_version, o.processed_at, o.attempts, o.error
			FROM raw_events r LEFT JOIN event_outcomes o ON o.raw_event_id = r.id
			WHERE r.id = $1`, id).Scan(
			&ev.ID, &ev.TenantID, &ev.Provider, &ev.RequestID, &ev.Raw, &ev.ReceivedAt,
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
