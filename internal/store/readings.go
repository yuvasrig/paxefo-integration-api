package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
)

const (
	defaultReadingsLimit = 100
	maxReadingsLimit     = 1000
)

type readingKeyT struct {
	imei string
	ts   int64
}

// ListReadings returns a device's readings newest first, one item per (imei, timestamp), cursor-paginated.
func (s *PGStore) ListReadings(ctx context.Context, tenantID string, q ReadingsQuery) (ReadingsPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultReadingsLimit
	}
	if limit > maxReadingsLimit {
		limit = maxReadingsLimit
	}
	from, to := int64(0), int64(1<<62)
	if q.From != nil {
		from = *q.From
	}
	if q.To != nil {
		to = *q.To
	}
	var curTS *int64
	curIMEI := ""
	if q.Cursor != "" {
		parts := strings.SplitN(q.Cursor, ":", 2)
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || len(parts) != 2 {
			return ReadingsPage{}, fmt.Errorf("store: bad cursor %q", q.Cursor)
		}
		curTS, curIMEI = &ts, parts[1]
	}

	var page ReadingsPage
	err := s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT device_imei, ts FROM (
				SELECT device_imei, ts FROM sensor_readings   WHERE tenant_id = $1 AND device_id = $2 AND ts >= $3 AND ts < $4
				UNION
				SELECT device_imei, ts FROM location_readings WHERE tenant_id = $1 AND device_id = $2 AND ts >= $3 AND ts < $4
			) k
			WHERE $5::bigint IS NULL OR (ts, device_imei) < ($5, $6)
			ORDER BY ts DESC, device_imei DESC LIMIT $7`, tenantID, q.DeviceID, from, to, curTS, curIMEI, limit+1)
		if err != nil {
			return fmt.Errorf("store: list reading keys: %w", err)
		}
		var keys []readingKeyT
		for rows.Next() {
			var k readingKeyT
			if err := rows.Scan(&k.imei, &k.ts); err != nil {
				rows.Close()
				return fmt.Errorf("store: scan reading key: %w", err)
			}
			keys = append(keys, k)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("store: reading keys: %w", err)
		}
		if len(keys) > limit {
			keys = keys[:limit]
			last := keys[len(keys)-1]
			page.NextCursor = fmt.Sprintf("%d:%s", last.ts, last.imei)
		}
		if len(keys) == 0 {
			page.Items = []ReadingItem{}
			return nil
		}

		imeis, tss := make([]string, len(keys)), make([]int64, len(keys))
		for i, k := range keys {
			imeis[i], tss[i] = k.imei, k.ts
		}
		sensors, err := loadSensors(ctx, tx, tenantID, imeis, tss)
		if err != nil {
			return err
		}
		locations, err := loadLocations(ctx, tx, tenantID, imeis, tss)
		if err != nil {
			return err
		}
		for _, k := range keys {
			item := ReadingItem{Timestamp: k.ts}
			if s, ok := sensors[k]; ok {
				item.Sensor, item.EventID = s.rec, s.event
			}
			if l, ok := locations[k]; ok {
				item.Location = l.rec
				if item.EventID == "" {
					item.EventID = l.event
				}
			}
			page.Items = append(page.Items, item)
		}
		return nil
	})
	return page, err
}

type sensorRow struct {
	rec   *paxafe.SensorRecord
	event string
}

func loadSensors(ctx context.Context, tx pgx.Tx, tenantID string, imeis []string, tss []int64) (map[readingKeyT]sensorRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT device_imei, ts, device_id, temperature, temperature_source, humidity, light_lux, accel_x, accel_y, accel_z, accel_g, raw_event_id
		FROM sensor_readings WHERE tenant_id = $1 AND device_imei = ANY($2) AND ts = ANY($3)`, tenantID, imeis, tss)
	if err != nil {
		return nil, fmt.Errorf("store: load sensors: %w", err)
	}
	defer rows.Close()
	out := map[readingKeyT]sensorRow{}
	for rows.Next() {
		var r paxafe.SensorRecord
		var src *string
		var ax, ay, az, ag *float64
		var event string
		if err := rows.Scan(&r.DeviceIMEI, &r.Timestamp, &r.DeviceID, &r.Temperature, &src, &r.Humidity, &r.LightLevel, &ax, &ay, &az, &ag, &event); err != nil {
			return nil, fmt.Errorf("store: scan sensor: %w", err)
		}
		if src != nil {
			r.TemperatureSource = *src
		}
		if ax != nil || ay != nil || az != nil || ag != nil {
			r.Accelerometer = &paxafe.Accelerometer{X: ax, Y: ay, Z: az, Magnitude: ag}
		}
		out[readingKeyT{r.DeviceIMEI, r.Timestamp}] = sensorRow{&r, event}
	}
	return out, rows.Err()
}

type locationRow struct {
	rec   *paxafe.LocationRecord
	event string
}

func loadLocations(ctx context.Context, tx pgx.Tx, tenantID string, imeis []string, tss []int64) (map[readingKeyT]locationRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT device_imei, ts, device_id, lat, lon, accuracy_m, accuracy_category, source, street, locality, state, country,
		       postal_code, full_address, battery_pct, cellular_dbm, wifi_aps, raw_event_id
		FROM location_readings WHERE tenant_id = $1 AND device_imei = ANY($2) AND ts = ANY($3)`, tenantID, imeis, tss)
	if err != nil {
		return nil, fmt.Errorf("store: load locations: %w", err)
	}
	defer rows.Close()
	out := map[readingKeyT]locationRow{}
	for rows.Next() {
		var r paxafe.LocationRecord
		var street, locality, state, country, postal, full *string
		var event string
		if err := rows.Scan(&r.DeviceIMEI, &r.Timestamp, &r.DeviceID, &r.Latitude, &r.Longitude, &r.LocationAccuracy, &r.AccuracyCategory,
			&r.LocationSource, &street, &locality, &state, &country, &postal, &full, &r.BatteryLevel, &r.CellularDbm, &r.WifiAccessPoints, &event); err != nil {
			return nil, fmt.Errorf("store: scan location: %w", err)
		}
		if street != nil || locality != nil || state != nil || country != nil || postal != nil || full != nil {
			r.Address = &paxafe.Address{Street: street, Locality: locality, State: state, Country: country, PostalCode: postal, FullAddress: full}
		}
		out[readingKeyT{r.DeviceIMEI, r.Timestamp}] = locationRow{&r, event}
	}
	return out, rows.Err()
}
