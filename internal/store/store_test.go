package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

var ctx = context.Background()

func newStore(t *testing.T) *store.PGStore {
	t.Helper()
	st, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{MaxConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"reviewer", "acme-pharma", "bioco"} {
		if err := st.CreateTenant(ctx, id, id, nil); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func sample(t *testing.T, n int) []byte {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", n))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// dbResolver answers name lookups from the store, as ingest does.
type dbResolver map[string]string

func (r dbResolver) ResolveByName(n string) (string, bool) { v, ok := r[n]; return v, ok }

// process runs the same steps as the ingest pipeline: save raw, parse, transform, commit.
func process(t *testing.T, st *store.PGStore, tenant string, raw []byte) (store.CommitResult, string) {
	t.Helper()
	id := store.NewID("evt_")
	if err := st.SaveRaw(ctx, store.RawEvent{
		ID: id, TenantID: tenant, Provider: "Tive", Raw: raw,
		Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now(), RequestID: "req_test",
	}); err != nil {
		t.Fatalf("SaveRaw: %v", err)
	}
	return commit(t, st, tenant, id, raw), id
}

func commit(t *testing.T, st *store.PGStore, tenant, id string, raw []byte) store.CommitResult {
	t.Helper()
	rs, err := rules.Default()
	if err != nil {
		t.Fatal(err)
	}
	p := tive.New(rs, config.Default())
	d, err := p.Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	devs := dbResolver{}
	if name, ok := d["DeviceName"].(string); ok {
		if imei, found, err := st.ResolveDevice(ctx, tenant, name); err != nil {
			t.Fatal(err)
		} else if found {
			devs[name] = imei
		}
	}
	res, err := p.Transform(d, rules.Scope{Tenant: tenant, Now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), Devices: devs})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	out, err := st.Commit(ctx, store.CommitInput{RawEventID: id, TenantID: tenant, Result: res})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return out
}

func count(t *testing.T, st *store.PGStore, tenant, query string, args ...any) int {
	t.Helper()
	var n int
	err := st.WithTenantTx(ctx, tenant, func(tx pgx.Tx) error { return tx.QueryRow(ctx, query, args...).Scan(&n) })
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrate_F13R1(t *testing.T) {
	st := newStore(t)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate must be a no-op: %v", err)
	}
	if v, err := st.MigrationVersion(ctx); err != nil || v < 1 {
		t.Fatalf("version %d, %v", v, err)
	}
	if ok, _ := st.TenantExists(ctx, "common-pool"); ok {
		t.Fatal("no tenant is created by the migration: tenants come from `seed` / `tenants create`")
	}
	if !st.UsesAppRole() {
		t.Fatal("tenant transactions must run as the non-bypass application role")
	}
}

// F4-R1, F4-R4: the same key arriving concurrently ends with exactly one reading.
func TestConcurrentSameKey_F4R4(t *testing.T) {
	st := newStore(t)
	raw := sample(t, 2)
	const n = 8
	var wg sync.WaitGroup
	statuses := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, _ := process(t, st, "reviewer", raw)
			statuses <- out.Status
		}()
	}
	wg.Wait()
	close(statuses)

	got := map[string]int{}
	for s := range statuses {
		got[s]++
	}
	if got["accepted"] != 1 || got[store.StatusDuplicate] != n-1 {
		t.Fatalf("statuses = %v, want 1 accepted and %d duplicate", got, n-1)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM sensor_readings`); c != 1 {
		t.Fatalf("sensor_readings = %d, want 1", c)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM location_readings`); c != 1 {
		t.Fatalf("location_readings = %d, want 1", c)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM raw_events`); c != n {
		t.Fatalf("raw_events = %d, want %d (every attempt is archived)", c, n)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM ingest_ledger`); c != n {
		t.Fatalf("ledger = %d, want %d", c, n)
	}
}

// F4-R3: a different payload for an existing key keeps the first and stores a conflict.
func TestConflictKeepsFirst_F4R3(t *testing.T) {
	st := newStore(t)
	first, firstID := process(t, st, "reviewer", sample(t, 0))
	if first.Status != "flagged" { // sample 0 carries IDENTITY_MISMATCH
		t.Fatalf("first status %s", first.Status)
	}
	for _, n := range []int{1, 8} {
		out, _ := process(t, st, "reviewer", sample(t, n))
		if out.Status != store.StatusConflict || out.FirstRawEventID != firstID {
			t.Fatalf("sample %d: status %s first %s, want conflict against %s", n, out.Status, out.FirstRawEventID, firstID)
		}
		found := false
		for _, f := range out.Flags {
			found = found || f.Code == rules.CodeDuplicateConflict
		}
		if !found {
			t.Fatalf("sample %d: DUPLICATE_CONFLICT flag missing: %v", n, out.Flags)
		}
	}
	var temp float64
	if err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT temperature FROM sensor_readings`).Scan(&temp)
	}); err != nil || temp != 10.078125 {
		t.Fatalf("stored temperature = %v (%v), want the first reading's full-precision 10.078125", temp, err)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM reading_conflicts WHERE first_raw_event_id = $1`, firstID); c != 2 {
		t.Fatalf("conflicts = %d, want 2", c)
	}
	var diff string
	_ = st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT diff::text FROM reading_conflicts ORDER BY id LIMIT 1`).Scan(&diff)
	})
	if diff == "" || diff == "{}" {
		t.Fatalf("conflict diff should list the changed fields, got %q", diff)
	}
}

// F6-R5: row-level security hides other tenants' rows and blocks writes into them.
func TestRowLevelSecurity_F6R5(t *testing.T) {
	st := newStore(t)
	_, idA := process(t, st, "reviewer", sample(t, 2))
	process(t, st, "acme-pharma", sample(t, 3))

	for _, table := range []string{"raw_events", "event_outcomes", "ingest_ledger", "sensor_readings", "location_readings", "devices"} {
		if c := count(t, st, "reviewer", "SELECT count(*) FROM "+table); c != 1 {
			t.Fatalf("%s visible to reviewer = %d, want only its own 1 row", table, c)
		}
		if c := count(t, st, "nobody", "SELECT count(*) FROM "+table); c != 0 {
			t.Fatalf("%s visible to an unknown tenant = %d, want 0", table, c)
		}
	}
	if _, err := st.GetEvent(ctx, "acme-pharma", idA); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-tenant GetEvent err = %v, want ErrNotFound", err)
	}
	page, err := st.ListReadings(ctx, "acme-pharma", store.ReadingsQuery{DeviceID: "B234567"})
	if err != nil || len(page.Items) != 1 || page.Items[0].Timestamp != 1739305646000 {
		t.Fatalf("acme readings = %+v, %v (must not include the reviewer's sample 2)", page, err)
	}

	err = st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO ingest_ledger (raw_event_id, tenant_id) VALUES ($1, 'acme-pharma')`, idA)
		return err
	})
	if err == nil {
		t.Fatal("writing a row for another tenant must be rejected by the RLS policy")
	}
}

// Constitution #2: raw_events is append-only.
func TestRawEventsAppendOnly(t *testing.T) {
	st := newStore(t)
	_, id := process(t, st, "reviewer", sample(t, 2))
	for _, q := range []string{`UPDATE raw_events SET request_id = 'x'`, `DELETE FROM raw_events`} {
		err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error { _, err := tx.Exec(ctx, q); return err })
		if err == nil {
			t.Fatalf("%q must fail", q)
		}
	}
	if _, err := st.GetEvent(ctx, "reviewer", id); err != nil {
		t.Fatal(err)
	}
}

// F1-R2 / F3-R1: even bytes that are not JSON are archived untouched.
func TestSaveRawKeepsAnyBytes(t *testing.T) {
	st := newStore(t)
	for name, raw := range map[string][]byte{
		"not json":   []byte(`{not json`),
		"empty":      {},
		"nul escape": []byte(`{"DeviceName":"a\u0000b"}`),
		"binary":     {0xff, 0xfe, 0x00, 0x01},
	} {
		t.Run(name, func(t *testing.T) {
			id := store.NewID("evt_")
			if err := st.SaveRaw(ctx, store.RawEvent{ID: id, TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now()}); err != nil {
				t.Fatalf("SaveRaw: %v", err)
			}
			ev, err := st.GetEvent(ctx, "reviewer", id)
			if err != nil || string(ev.Raw) != string(raw) || ev.HasOutcome {
				t.Fatalf("event = %+v, %v", ev, err)
			}
		})
	}
}

// F5-R1: device last_* moves only forward; first_seen can move back.
func TestDeviceLastOnlyIfNewer_F5R1(t *testing.T) {
	st := newStore(t)
	process(t, st, "reviewer", sample(t, 11)) // 1739647646000
	process(t, st, "reviewer", sample(t, 2))  // 1739302046000 (older)
	var lastTS int64
	var firstSeen, lastSeen time.Time
	var lat float64
	if err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT last_ts, first_seen, last_seen, last_lat FROM devices WHERE imei = '866088073468440'`).Scan(&lastTS, &firstSeen, &lastSeen, &lat)
	}); err != nil {
		t.Fatal(err)
	}
	if lastTS != 1739647646000 || lat != 35.2271 {
		t.Fatalf("last_ts %d lat %v, want the newer reading's", lastTS, lat)
	}
	if firstSeen.UnixMilli() != 1739302046000 || lastSeen.UnixMilli() != 1739647646000 {
		t.Fatalf("first_seen %v last_seen %v", firstSeen, lastSeen)
	}
}

// F5-R3: a name that maps to two IMEIs is not resolved.
func TestResolveDevice_F5R3(t *testing.T) {
	st := newStore(t)
	process(t, st, "reviewer", sample(t, 0))
	if imei, ok, err := st.ResolveDevice(ctx, "reviewer", "A571992"); err != nil || !ok || imei != "863257063350583" {
		t.Fatalf("resolve = %q %v %v", imei, ok, err)
	}
	if _, ok, _ := st.ResolveDevice(ctx, "reviewer", "Unknown"); ok {
		t.Fatal("unknown name must not resolve")
	}
	if _, ok, _ := st.ResolveDevice(ctx, "acme-pharma", "A571992"); ok {
		t.Fatal("another tenant's device must not resolve")
	}
	err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO devices (tenant_id, imei, device_name, valid_from, first_seen, last_seen)
			VALUES ('reviewer', '999999999999999', 'A571992', now(), now(), now())`)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.ResolveDevice(ctx, "reviewer", "A571992"); ok {
		t.Fatal("a name with two IMEIs must not resolve")
	}
}

// F5-R4: a reused tracker keeps each tenant's history separate; payloads never move a device.
func TestDeviceReuseAcrossTenants_F5R4(t *testing.T) {
	st := newStore(t)
	process(t, st, "reviewer", sample(t, 2)) // B234567 / 866088073468440 -> reviewer

	// Same IMEI arriving for another tenant is stored, but the registry is left alone.
	process(t, st, "acme-pharma", sample(t, 11))
	if c := count(t, st, "acme-pharma", `SELECT count(*) FROM devices`); c != 0 {
		t.Fatalf("acme devices = %d: a payload must not take over another tenant's tracker", c)
	}
	if c := count(t, st, "acme-pharma", `SELECT count(*) FROM sensor_readings`); c != 1 {
		t.Fatalf("acme readings = %d, want 1", c)
	}

	at := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	if err := st.ReassignDevice(ctx, "866088073468440", "B234567", "reviewer", "acme-pharma", at); err != nil {
		t.Fatal(err)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM devices WHERE valid_to IS NOT NULL`); c != 1 {
		t.Fatalf("reviewer closed assignments = %d, want 1", c)
	}
	if _, ok, _ := st.ResolveDevice(ctx, "reviewer", "B234567"); ok {
		t.Fatal("reviewer's closed assignment must not resolve")
	}
	if imei, ok, _ := st.ResolveDevice(ctx, "acme-pharma", "B234567"); !ok || imei != "866088073468440" {
		t.Fatalf("acme should now own the device: %q %v", imei, ok)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM sensor_readings`); c != 1 {
		t.Fatalf("reviewer keeps its history, got %d readings", c)
	}
	if err := st.ReassignDevice(ctx, "866088073468440", "B234567", "reviewer", "bioco", at); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("reassigning from a tenant with no open assignment: %v", err)
	}
}

// F3-R7: a rejected event keeps its raw payload and reasons, and writes no readings.
func TestRejectedEvent_F3R7(t *testing.T) {
	st := newStore(t)
	out, id := process(t, st, "reviewer", sample(t, 12))
	if out.Status != provider.StatusRejected {
		t.Fatalf("status %s", out.Status)
	}
	ev, err := st.GetEvent(ctx, "reviewer", id)
	if err != nil || ev.Status != "rejected" || len(ev.Reasons) != 1 || ev.Reasons[0].Code != "NO_DEVICE_IDENTITY" || len(ev.Flags) != 1 {
		t.Fatalf("event = %+v, %v", ev, err)
	}
	if ev.TransformVersion == "" || ev.RulesVersion == "" {
		t.Fatal("versions must be stored for audit (F7-R5)")
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM sensor_readings`); c != 0 {
		t.Fatal("a rejected event writes no readings")
	}
}

// F7-R1..R3: a failure after the raw save is recorded; a later commit of the same event recovers it idempotently.
func TestFailureThenReplay_F7(t *testing.T) {
	st := newStore(t)
	raw := sample(t, 2)
	id := store.NewID("evt_")
	if err := st.SaveRaw(ctx, store.RawEvent{ID: id, TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordFailure(ctx, "reviewer", id, "boom"); err != nil {
		t.Fatal(err)
	}
	ev, _ := st.GetEvent(ctx, "reviewer", id)
	if ev.Status != store.StatusFailed || ev.Error != "boom" {
		t.Fatalf("event = %+v", ev)
	}

	if out := commit(t, st, "reviewer", id, raw); out.Status != "accepted" {
		t.Fatalf("replay status %s", out.Status)
	}
	if out := commit(t, st, "reviewer", id, raw); out.Status != "accepted" {
		t.Fatalf("a second replay must be idempotent and keep the status, got %s", out.Status)
	}
	ev, _ = st.GetEvent(ctx, "reviewer", id)
	if ev.Status != "accepted" || ev.Attempts != 3 || ev.Error != "" {
		t.Fatalf("after replay: status %s attempts %d error %q", ev.Status, ev.Attempts, ev.Error)
	}
	if c := count(t, st, "reviewer", `SELECT count(*) FROM sensor_readings`); c != 1 {
		t.Fatalf("readings = %d, want exactly 1", c)
	}
}

func TestListReadingsPaging(t *testing.T) {
	st := newStore(t)
	for _, n := range []int{2, 3, 11} { // B234567 at 1739302046000, 1739305646000, 1739647646000
		process(t, st, "reviewer", sample(t, n))
	}
	p1, err := st.ListReadings(ctx, "reviewer", store.ReadingsQuery{DeviceID: "B234567", Limit: 2})
	if err != nil || len(p1.Items) != 2 || p1.NextCursor == "" {
		t.Fatalf("page 1 = %+v, %v", p1, err)
	}
	if p1.Items[0].Timestamp != 1739647646000 || p1.Items[1].Timestamp != 1739305646000 {
		t.Fatalf("page 1 must be newest first: %+v", p1.Items)
	}
	if p1.Items[0].Sensor == nil || p1.Items[0].Location == nil || p1.Items[0].EventID == "" {
		t.Fatalf("items carry both records and the event id: %+v", p1.Items[0])
	}
	p2, err := st.ListReadings(ctx, "reviewer", store.ReadingsQuery{DeviceID: "B234567", Limit: 2, Cursor: p1.NextCursor})
	if err != nil || len(p2.Items) != 1 || p2.NextCursor != "" || p2.Items[0].Timestamp != 1739302046000 {
		t.Fatalf("page 2 = %+v, %v", p2, err)
	}
	from, to := int64(1739305646000), int64(1739647646000)
	p3, _ := st.ListReadings(ctx, "reviewer", store.ReadingsQuery{DeviceID: "B234567", From: &from, To: &to})
	if len(p3.Items) != 1 || p3.Items[0].Timestamp != 1739305646000 {
		t.Fatalf("range page = %+v (from inclusive, to exclusive)", p3)
	}
	if _, err := st.ListReadings(ctx, "reviewer", store.ReadingsQuery{DeviceID: "B234567", Cursor: "garbage"}); err == nil {
		t.Fatal("bad cursor must error")
	}
	// The stored sensor reading rounds only on output.
	b, _ := p1.Items[1].Sensor.MarshalJSON()
	if string(b) == "" || p1.Items[1].Sensor.Temperature == nil || *p1.Items[1].Sensor.Temperature != 4.625 {
		t.Fatalf("stored temperature must keep full precision 4.625, got %v", p1.Items[1].Sensor.Temperature)
	}
}

func TestAdminKeys(t *testing.T) {
	st := newStore(t)
	if ok, _ := st.TenantExists(ctx, "reviewer"); !ok {
		t.Fatal("reviewer should exist")
	}
	if ok, _ := st.TenantExists(ctx, "nobody"); ok {
		t.Fatal("nobody should not exist")
	}
	exp := time.Now().Add(time.Hour)
	if err := st.CreateAPIKey(ctx, "reviewer", "k1", "pxk_test_abcd", "hash1", &exp); err != nil {
		t.Fatal(err)
	}
	k, err := st.LookupAPIKey(ctx, "hash1")
	if err != nil || k.TenantID != "reviewer" || k.ExpiresAt == nil || k.RevokedAt != nil {
		t.Fatalf("key = %+v, %v", k, err)
	}
	if _, err := st.LookupAPIKey(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown key: %v", err)
	}
	if err := st.RevokeAPIKey(ctx, k.ID); err != nil {
		t.Fatal(err)
	}
	if k, _ = st.LookupAPIKey(ctx, "hash1"); k.RevokedAt == nil {
		t.Fatal("key should be revoked")
	}
	if err := st.RevokeAPIKey(ctx, 99999); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("revoking a missing key: %v", err)
	}
	if keys, err := st.ListAPIKeys(ctx, "reviewer"); err != nil || len(keys) != 1 {
		t.Fatalf("keys = %v, %v", keys, err)
	}
}

// ---- per-tenant conflict policy (F4-R7, F4-R8) ----

func TestParseConflictSettings(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		want    store.ConflictSettings
		wantErr bool
	}{
		{"no config", ``, store.ConflictSettings{Policy: "keep_first", HTTPStatus: 200}, false},
		{"empty object", `{}`, store.ConflictSettings{Policy: "keep_first", HTTPStatus: 200}, false},
		{"other keys only", `{"ingest_mode":"sync"}`, store.ConflictSettings{Policy: "keep_first", HTTPStatus: 200}, false},
		{"keep_first", `{"conflict":{"policy":"keep_first"}}`, store.ConflictSettings{Policy: "keep_first", HTTPStatus: 200}, false},
		{"keep_last defaults to 200", `{"conflict":{"policy":"keep_last"}}`, store.ConflictSettings{Policy: "keep_last", HTTPStatus: 200}, false},
		{"reject defaults to 409", `{"conflict":{"policy":"reject"}}`, store.ConflictSettings{Policy: "reject", HTTPStatus: 409}, false},
		{"reject with 200", `{"conflict":{"policy":"reject","http_status":200}}`, store.ConflictSettings{Policy: "reject", HTTPStatus: 200}, false},
		{"keep_first with 409", `{"conflict":{"policy":"keep_first","http_status":409}}`, store.ConflictSettings{Policy: "keep_first", HTTPStatus: 409}, false},
		{"unknown policy", `{"conflict":{"policy":"merge"}}`, store.ConflictSettings{}, true},
		{"missing policy", `{"conflict":{"http_status":409}}`, store.ConflictSettings{}, true},
		{"bad status", `{"conflict":{"policy":"reject","http_status":500}}`, store.ConflictSettings{}, true},
		{"not json", `{nope`, store.ConflictSettings{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.ParseConflictSettings([]byte(tt.config))
			if (err != nil) != tt.wantErr || (!tt.wantErr && got != tt.want) {
				t.Fatalf("got %+v, err %v; want %+v (err %v)", got, err, tt.want, tt.wantErr)
			}
		})
	}
	if _, err := store.NewConflictSettings("keep_last", 404); err == nil {
		t.Fatal("only 200 and 409 are allowed")
	}
}

// variant of sample 2 (same device and time) with different content.
func variant(t *testing.T, humidity, lat float64) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(sample(t, 2), &m); err != nil {
		t.Fatal(err)
	}
	m["Humidity"].(map[string]any)["Percentage"] = humidity
	m["Location"].(map[string]any)["Latitude"] = lat
	b, _ := json.Marshal(m)
	return b
}

func setPolicy(t *testing.T, st *store.PGStore, policy string, status int) {
	t.Helper()
	cs, err := store.NewConflictSettings(policy, status)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetConflictSettings(ctx, "reviewer", cs); err != nil {
		t.Fatal(err)
	}
}

func humidityAndLat(t *testing.T, st *store.PGStore) (hum, lat float64, locRows int) {
	t.Helper()
	err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT humidity FROM sensor_readings`).Scan(&hum); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM location_readings`).Scan(&locRows); err != nil {
			return err
		}
		if locRows == 1 {
			return tx.QueryRow(ctx, `SELECT lat FROM location_readings`).Scan(&lat)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return
}

func resolutions(t *testing.T, st *store.PGStore) []string {
	t.Helper()
	var out []string
	err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT resolution FROM reading_conflicts ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r string
			if err := rows.Scan(&r); err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func conflictFlagAction(res store.CommitResult) string {
	for _, f := range res.Flags {
		if f.Code == rules.CodeDuplicateConflict {
			return f.Action
		}
	}
	return ""
}

func TestConflictPolicyKeepFirstIsTheDefault_F4R7(t *testing.T) {
	st := newStore(t)
	process(t, st, "reviewer", sample(t, 2))
	out, _ := process(t, st, "reviewer", variant(t, 99.9, 38.5))
	if out.Status != store.StatusConflict || out.ConflictHTTPStatus != 200 || conflictFlagAction(out) != "kept" {
		t.Fatalf("%+v", out)
	}
	if hum, lat, rows := humidityAndLat(t, st); hum != 55.3 || lat != 37.7749 || rows != 1 {
		t.Fatalf("the first reading must be kept: %v %v %d", hum, lat, rows)
	}
	if r := resolutions(t, st); len(r) != 1 || r[0] != "kept_first" {
		t.Fatalf("resolutions %v", r)
	}
}

// keep_last: the new version replaces the stored reading, the old event stays archived and is named.
func TestConflictPolicyKeepLast_F4R7(t *testing.T) {
	st := newStore(t)
	setPolicy(t, st, "keep_last", 0)
	_, firstID := process(t, st, "reviewer", sample(t, 2))

	out, secondID := process(t, st, "reviewer", variant(t, 99.9, 38.5))
	if out.Status != store.StatusConflict || out.ConflictHTTPStatus != 200 || conflictFlagAction(out) != "replaced" || out.FirstRawEventID != firstID {
		t.Fatalf("%+v", out)
	}
	if hum, lat, rows := humidityAndLat(t, st); hum != 99.9 || lat != 38.5 || rows != 1 {
		t.Fatalf("the new reading must replace the stored one: %v %v %d", hum, lat, rows)
	}
	var owner string
	var devLat float64
	_ = st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT raw_event_id FROM sensor_readings`).Scan(&owner); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT last_lat FROM devices WHERE imei = '866088073468440'`).Scan(&devLat)
	})
	if owner != secondID || devLat != 38.5 {
		t.Fatalf("the reading now belongs to the new event and the device position follows: owner %s, last_lat %v", owner, devLat)
	}
	if _, err := st.GetEvent(ctx, "reviewer", firstID); err != nil {
		t.Fatalf("the replaced event must stay archived: %v", err)
	}
	if r := resolutions(t, st); len(r) != 1 || r[0] != "replaced" {
		t.Fatalf("resolutions %v", r)
	}

	// Last wins means last received: sending the original again puts it back.
	if out, _ = process(t, st, "reviewer", sample(t, 2)); out.Status != store.StatusConflict {
		t.Fatalf("%+v", out)
	}
	if hum, lat, _ := humidityAndLat(t, st); hum != 55.3 || lat != 37.7749 {
		t.Fatalf("%v %v", hum, lat)
	}

	// A new version without a valid location removes the old location row, as the new payload has none.
	if out, _ = process(t, st, "reviewer", variant(t, 70, 95)); out.Status != store.StatusConflict {
		t.Fatalf("%+v", out)
	}
	if hum, _, rows := humidityAndLat(t, st); hum != 70 || rows != 0 {
		t.Fatalf("humidity %v, location rows %d; the replaced reading has no location", hum, rows)
	}
}

func TestConflictPolicyReject_F4R7(t *testing.T) {
	st := newStore(t)
	setPolicy(t, st, "reject", 0)
	process(t, st, "reviewer", sample(t, 2))
	out, id := process(t, st, "reviewer", variant(t, 99.9, 38.5))
	if out.Status != store.StatusConflict || out.ConflictHTTPStatus != 409 || conflictFlagAction(out) != "rejected" {
		t.Fatalf("%+v", out)
	}
	if hum, lat, _ := humidityAndLat(t, st); hum != 55.3 || lat != 37.7749 {
		t.Fatalf("a rejected version writes nothing to the readings: %v %v", hum, lat)
	}
	if _, err := st.GetEvent(ctx, "reviewer", id); err != nil {
		t.Fatalf("the raw payload is archived anyway: %v", err)
	}
	if r := resolutions(t, st); len(r) != 1 || r[0] != "rejected" {
		t.Fatalf("resolutions %v", r)
	}

	// The status code is its own setting.
	setPolicy(t, st, "reject", 200)
	if out, _ = process(t, st, "reviewer", variant(t, 12.3, 38.5)); out.ConflictHTTPStatus != 200 {
		t.Fatalf("%+v", out)
	}
	setPolicy(t, st, "keep_first", 409)
	if out, _ = process(t, st, "reviewer", variant(t, 12.4, 38.5)); out.ConflictHTTPStatus != 409 {
		t.Fatalf("%+v", out)
	}
}

// An identical payload is a duplicate whatever the policy: the policy is only for different content.
func TestDuplicateIgnoresConflictPolicy(t *testing.T) {
	for _, policy := range []string{"keep_first", "keep_last", "reject"} {
		t.Run(policy, func(t *testing.T) {
			st := newStore(t)
			setPolicy(t, st, policy, 0)
			process(t, st, "reviewer", sample(t, 2))
			out, _ := process(t, st, "reviewer", sample(t, 2))
			if out.Status != store.StatusDuplicate || out.ConflictHTTPStatus != 0 {
				t.Fatalf("%+v", out)
			}
			if hum, _, _ := humidityAndLat(t, st); hum != 55.3 {
				t.Fatal("a duplicate changes nothing")
			}
		})
	}
}

// Policies belong to a tenant: another tenant keeps its own behavior.
func TestConflictPolicyIsPerTenant(t *testing.T) {
	st := newStore(t)
	setPolicy(t, st, "reject", 0) // reviewer only
	process(t, st, "acme-pharma", sample(t, 2))
	out, _ := process(t, st, "acme-pharma", variant(t, 99.9, 38.5))
	if out.ConflictHTTPStatus != 200 || conflictFlagAction(out) != "kept" {
		t.Fatalf("acme-pharma has no policy set, so it keeps the first: %+v", out)
	}
}

// A malformed stored config must never stop ingest: it falls back to the default.
func TestMalformedConflictConfigFallsBack(t *testing.T) {
	for _, bad := range []string{`{"conflict":{"policy":"merge"}}`, `{"conflict":{"policy":"reject","http_status":500}}`, `{"conflict":"yes"}`} {
		t.Run(bad, func(t *testing.T) {
			st := newStore(t)
			if _, err := st.Pool().Exec(ctx, `UPDATE tenants SET config = $1::jsonb WHERE id = 'reviewer'`, bad); err != nil {
				t.Fatal(err)
			}
			process(t, st, "reviewer", sample(t, 2))
			out, _ := process(t, st, "reviewer", variant(t, 99.9, 38.5))
			if out.Status != store.StatusConflict || out.ConflictHTTPStatus != 200 || conflictFlagAction(out) != "kept" {
				t.Fatalf("%+v", out)
			}
		})
	}
}

func TestSetConflictSettingsKeepsOtherConfig(t *testing.T) {
	st := newStore(t)
	if err := st.CreateTenant(ctx, "cfg", "cfg", map[string]any{"ingest_mode": "sync", "plan": "x"}); err != nil {
		t.Fatal(err)
	}
	cs, _ := store.NewConflictSettings("keep_last", 0)
	if err := st.SetConflictSettings(ctx, "cfg", cs); err != nil {
		t.Fatal(err)
	}
	raw, err := st.TenantConfig(ctx, "cfg")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if m["ingest_mode"] != "sync" || m["plan"] != "x" || m["conflict"].(map[string]any)["policy"] != "keep_last" {
		t.Fatalf("config = %s", raw)
	}
	if err := st.SetConflictSettings(ctx, "nobody", cs); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown tenant: %v", err)
	}
}

// ---- rulesets published at runtime ----

func TestRulesetsPublishAndActivate(t *testing.T) {
	st := newStore(t)
	if _, ok, err := st.ActiveRuleset(ctx); err != nil || ok {
		t.Fatalf("a new database has no active ruleset: %v %v", ok, err)
	}
	a, err := st.PublishRuleset(ctx, "v1", "version: v1\nrules: []\n", "alice")
	if err != nil || !a.Active || a.ID == 0 || a.CreatedBy != "alice" {
		t.Fatalf("%+v %v", a, err)
	}
	got, ok, err := st.ActiveRuleset(ctx)
	if err != nil || !ok || got.Version != "v1" || got.YAML != "version: v1\nrules: []\n" {
		t.Fatalf("%+v %v %v", got, ok, err)
	}

	// Publishing another makes it the only active one.
	if _, err := st.PublishRuleset(ctx, "v2", "version: v2\nrules: []\n", ""); err != nil {
		t.Fatal(err)
	}
	if got, _, _ = st.ActiveRuleset(ctx); got.Version != "v2" {
		t.Fatalf("active = %s", got.Version)
	}
	// A version is published once: a changed ruleset must bump it.
	if _, err := st.PublishRuleset(ctx, "v1", "version: v1\nrules: []\n", ""); !errors.Is(err, store.ErrVersionExists) {
		t.Fatalf("republishing a version: %v", err)
	}

	hist, err := st.RulesetHistory(ctx, 10)
	if err != nil || len(hist) != 2 || hist[0].Version != "v2" || !hist[0].Active || hist[1].Version != "v1" || hist[1].Active {
		t.Fatalf("history %+v %v", hist, err)
	}

	// Reset keeps the history but leaves nothing active.
	if cleared, err := st.ClearActiveRuleset(ctx); err != nil || !cleared {
		t.Fatalf("%v %v", cleared, err)
	}
	if _, ok, _ := st.ActiveRuleset(ctx); ok {
		t.Fatal("nothing should be active after a reset")
	}
	if cleared, _ := st.ClearActiveRuleset(ctx); cleared {
		t.Fatal("resetting twice is a no-op")
	}
	if hist, _ = st.RulesetHistory(ctx, 10); len(hist) != 2 {
		t.Fatal("history is kept")
	}
}

// The running service may read rulesets but never write them: only operators publish.
func TestAppRoleCannotWriteRulesets(t *testing.T) {
	st := newStore(t)
	if _, err := st.PublishRuleset(ctx, "v1", "version: v1\nrules: []\n", ""); err != nil {
		t.Fatal(err)
	}
	err := st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM rulesets`).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			t.Errorf("the app role can read rulesets: %d", n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	err = st.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE rulesets SET active = false`)
		return err
	})
	if err == nil {
		t.Fatal("the app role must not be able to change rulesets")
	}
}

// ---- replay support ----

func TestInRollbackTxWritesNothing(t *testing.T) {
	st := newStore(t)
	raw := sample(t, 2)
	id := store.NewID("evt_")
	if err := st.SaveRaw(ctx, store.RawEvent{ID: id, TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rs, _ := rules.Default()
	p := tive.New(rs, config.Default())
	d, _ := p.Parse(raw)
	res, err := p.Transform(d, rules.Scope{Tenant: "reviewer", Now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), Devices: dbResolver{}})
	if err != nil {
		t.Fatal(err)
	}

	var inside store.CommitResult
	err = st.InRollbackTx(ctx, "reviewer", time.Minute, func(commit func(context.Context, store.CommitInput) (store.CommitResult, error)) error {
		var err error
		inside, err = commit(ctx, store.CommitInput{RawEventID: id, TenantID: "reviewer", Result: res})
		if err != nil {
			return err
		}
		// Inside the transaction the write is visible to the next commit...
		again, err := commit(ctx, store.CommitInput{RawEventID: id, TenantID: "reviewer", Result: res, ReplayRunID: "dry-run"})
		if err != nil || again.Status != inside.Status {
			t.Errorf("a reprocess inside the transaction sees the earlier write: %+v %v", again, err)
		}
		return nil
	})
	if err != nil || inside.Status != "accepted" {
		t.Fatalf("%+v %v", inside, err)
	}
	// ...and afterwards nothing was written.
	for _, table := range []string{"sensor_readings", "location_readings", "event_outcomes", "event_outcome_history"} {
		if n := count(t, st, "reviewer", "SELECT count(*) FROM "+table); n != 0 {
			t.Errorf("%s has %d rows after a rolled-back run", table, n)
		}
	}
	// An error from fn is returned, not swallowed.
	boom := errors.New("boom")
	if err := st.InRollbackTx(ctx, "reviewer", time.Minute, func(func(context.Context, store.CommitInput) (store.CommitResult, error)) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("%v", err)
	}
}

func TestListTenantsAndReplayRuns(t *testing.T) {
	st := newStore(t)
	ids, err := st.ListTenants(ctx)
	if err != nil || strings.Join(ids, ",") != "acme-pharma,bioco,reviewer" {
		t.Fatalf("%v %v", ids, err)
	}
	now := time.Now()
	run := store.ReplayRun{RunID: "run_1", TenantID: "reviewer", RequestedBy: "bob", Params: map[string]any{"device": "X"}, StartedAt: now, FinishedAt: now,
		Selected: 5, Reprocessed: 4, Changed: 3, Errors: 1, Transitions: map[string]int{"rejected -> accepted": 3}}
	if err := st.RecordReplayRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	runs, err := st.ListReplayRuns(ctx, 5)
	if err != nil || len(runs) != 1 || runs[0].RequestedBy != "bob" || runs[0].Transitions["rejected -> accepted"] != 3 || runs[0].Params["device"] != "X" || runs[0].Errors != 1 {
		t.Fatalf("%+v %v", runs, err)
	}
	if err := st.RecordReplayRun(ctx, run); err == nil {
		t.Fatal("a run is recorded once per tenant")
	}
}
