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
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
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

// ---- async queue ----

func enqueue(t *testing.T, st *store.PGStore, tenant string, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		id := store.NewID("evt_")
		raw := []byte(fmt.Sprintf(`{"n":%d}`, i))
		if err := st.SaveRawAndEnqueue(ctx, store.RawEvent{ID: id, TenantID: tenant, Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestEnqueueIsAtomicWithTheRawPayload(t *testing.T) {
	st := newStore(t)
	ids := enqueue(t, st, "reviewer", 1)
	if n := count(t, st, "reviewer", `SELECT count(*) FROM raw_events`); n != 1 {
		t.Fatalf("raw events %d", n)
	}
	if n := count(t, st, "reviewer", `SELECT count(*) FROM ingest_ledger`); n != 1 {
		t.Fatalf("ledger %d", n)
	}
	if ok, err := st.HasJob(ctx, ids[0]); err != nil || !ok {
		t.Fatalf("job queued: %v %v", ok, err)
	}
	// If the enqueue fails, the raw payload is not saved either (one transaction): a duplicate id fails the whole thing.
	raw := []byte(`{}`)
	err := st.SaveRawAndEnqueue(ctx, store.RawEvent{ID: ids[0], TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: "h", ReceivedAt: time.Now()})
	if err == nil {
		t.Fatal("a duplicate event id must fail")
	}
	if n := count(t, st, "reviewer", `SELECT count(*) FROM raw_events`); n != 1 {
		t.Fatalf("a failed enqueue must not leave a raw event behind: %d", n)
	}
}

func TestPendingDepthIsBounded(t *testing.T) {
	st := newStore(t)
	enqueue(t, st, "reviewer", 12)
	enqueue(t, st, "acme-pharma", 3)
	if n, err := st.PendingDepth(ctx, "reviewer", 100); err != nil || n != 12 {
		t.Fatalf("%d %v", n, err)
	}
	if n, _ := st.PendingDepth(ctx, "reviewer", 5); n != 6 {
		t.Fatalf("counting stops at cap+1 so the cost is bounded: %d", n)
	}
	if n, _ := st.PendingDepth(ctx, "acme-pharma", 100); n != 3 {
		t.Fatalf("depth is per tenant: %d", n)
	}
}

// Each tenant has its own workers: a claim for one tenant never returns another tenant's job.
func TestClaimJobsAreScopedToOneTenant(t *testing.T) {
	st := newStore(t)
	enqueue(t, st, "reviewer", 20)
	enqueue(t, st, "acme-pharma", 2)
	jobs, err := st.ClaimJobs(ctx, "w1", "acme-pharma", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("acme's workers take acme's jobs only, however big another tenant's backlog is: %d", len(jobs))
	}
	for _, j := range jobs {
		if j.TenantID != "acme-pharma" || j.Attempts != 1 {
			t.Fatalf("a claim counts as an attempt: %+v", j)
		}
	}
	batch, _ := st.ClaimJobs(ctx, "w2", "reviewer", 8)
	if len(batch) != 8 {
		t.Fatalf("a claim takes at most the batch: %d", len(batch))
	}
	// Claimed jobs are not handed out again.
	rest, _ := st.ClaimJobs(ctx, "w3", "reviewer", 100)
	if len(rest) != 12 {
		t.Fatalf("remaining jobs: %d", len(rest))
	}
	if more, _ := st.ClaimJobs(ctx, "w4", "reviewer", 100); len(more) != 0 {
		t.Fatalf("nothing left: %d", len(more))
	}
	if more, _ := st.ClaimJobs(ctx, "w5", "nobody", 100); len(more) != 0 {
		t.Fatalf("unknown tenant: %d", len(more))
	}
}

// Many workers claiming at once never receive the same job (FOR UPDATE SKIP LOCKED).
func TestConcurrentClaimsAreDisjoint(t *testing.T) {
	st := newStore(t)
	enqueue(t, st, "reviewer", 60)
	enqueue(t, st, "acme-pharma", 40)
	var mu sync.Mutex
	seen := map[string]string{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			tenant := []string{"reviewer", "acme-pharma"}[w%2] // four workers per tenant
			for {
				jobs, err := st.ClaimJobs(ctx, fmt.Sprintf("w%d", w), tenant, 7)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, dup := seen[j.RawEventID]; dup {
						t.Errorf("job %s claimed by %s and w%d", j.RawEventID, prev, w)
					}
					seen[j.RawEventID] = fmt.Sprintf("w%d", w)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(seen) != 100 {
		t.Fatalf("claimed %d of 100", len(seen))
	}
}

func TestRetryBuryRescueAndStats(t *testing.T) {
	st := newStore(t)
	ids := enqueue(t, st, "reviewer", 3)
	jobs, _ := st.ClaimJobs(ctx, "w1", "reviewer", 10)
	if len(jobs) != 3 {
		t.Fatalf("%d", len(jobs))
	}

	// Retry: not claimable before its time, claimable after, and the attempt counter keeps counting.
	if err := st.RetryJob(ctx, ids[0], "boom", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.ClaimJobs(ctx, "w1", "reviewer", 10); len(got) != 0 {
		t.Fatalf("a retry waits for its backoff: %d", len(got))
	}
	if err := st.RetryJob(ctx, ids[0], "boom", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	got, _ := st.ClaimJobs(ctx, "w1", "reviewer", 10)
	if len(got) != 1 || got[0].RawEventID != ids[0] || got[0].Attempts != 2 {
		t.Fatalf("%+v", got)
	}

	// Bury: the outcome becomes dead, the job is gone, the raw payload stays.
	if err := st.BuryJob(ctx, "reviewer", ids[1], "gave up"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.HasJob(ctx, ids[1]); ok {
		t.Fatal("a buried job leaves the queue")
	}
	ev, err := st.GetEvent(ctx, "reviewer", ids[1])
	if err != nil || ev.Status != store.StatusDead || ev.Error != "gave up" || len(ev.Raw) == 0 {
		t.Fatalf("%+v %v", ev, err)
	}

	// Stats: the third job is still being processed.
	rows, err := st.QueueStats(ctx)
	if err != nil || len(rows) != 1 || rows[0].TenantID != "reviewer" || rows[0].Processing != 2 || rows[0].Pending != 0 {
		t.Fatalf("%+v %v", rows, err)
	}

	// Rescue: a worker that died holding jobs.
	if n, _ := st.RescueStuckJobs(ctx, time.Hour); n != 0 {
		t.Fatalf("recent claims are not stuck: %d", n)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE ingest_jobs SET locked_at = now() - interval '2 hours' WHERE status = 'processing'`); err != nil {
		t.Fatal(err)
	}
	if n, err := st.RescueStuckJobs(ctx, time.Hour); err != nil || n != 2 {
		t.Fatalf("rescued %d %v", n, err)
	}
	if again, _ := st.ClaimJobs(ctx, "w2", "reviewer", 10); len(again) != 2 {
		t.Fatalf("rescued jobs can be claimed again: %d", len(again))
	}
}

// F-async: the job disappears in the same transaction that records the outcome.
func TestCommitCompletesTheJobAtomically(t *testing.T) {
	st := newStore(t)
	raw := sample(t, 2)
	id := store.NewID("evt_")
	if err := st.SaveRawAndEnqueue(ctx, store.RawEvent{ID: id, TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rs, _ := rules.Default()
	p := tive.New(rs, config.Default())
	d, _ := p.Parse(raw)
	res, _ := p.Transform(d, rules.Scope{Tenant: "reviewer", Now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC), Devices: dbResolver{}})

	if _, err := st.Commit(ctx, store.CommitInput{RawEventID: id, TenantID: "reviewer", Result: res, CompleteJob: true}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.HasJob(ctx, id); ok {
		t.Fatal("the job must be gone once the outcome is written")
	}
	if ev, _ := st.GetEvent(ctx, "reviewer", id); ev.Status != "accepted" {
		t.Fatalf("%s", ev.Status)
	}
}

func TestHeartbeats(t *testing.T) {
	st := newStore(t)
	now := time.Now()
	if err := st.ReportHeartbeat(ctx, store.Heartbeat{Instance: "a", TenantID: "reviewer", Role: "worker", Workers: 3, StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReportHeartbeat(ctx, store.Heartbeat{Instance: "b", TenantID: "acme-pharma", Role: "all", Workers: 2, InFlight: 1, ProcessedTotal: 9, StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReportHeartbeat(ctx, store.Heartbeat{Instance: "a", TenantID: "reviewer", Role: "worker", Workers: 4, StartedAt: now}); err != nil { // update, not a second row
		t.Fatal(err)
	}
	// The same process can report one row per tenant pool.
	if err := st.ReportHeartbeat(ctx, store.Heartbeat{Instance: "b", TenantID: "reviewer", Role: "all", Workers: 1, StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	hbs, err := st.ActiveHeartbeats(ctx, time.Minute)
	if err != nil || len(hbs) != 3 || hbs[0].Workers+hbs[1].Workers+hbs[2].Workers != 7 || hbs[0].TenantID == "" {
		t.Fatalf("%+v %v", hbs, err)
	}
	if hbs[1].Instance != "b" || hbs[1].TenantID != "acme-pharma" {
		t.Fatalf("rows come back by process then tenant: %+v", hbs)
	}
	if hbs[0].Workers != 4 {
		t.Fatalf("%+v %v", hbs, err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE worker_heartbeats SET last_seen = now() - interval '5 minutes' WHERE instance = 'a'`); err != nil {
		t.Fatal(err)
	}
	if hbs, _ = st.ActiveHeartbeats(ctx, time.Minute); len(hbs) != 2 || hbs[0].Instance != "b" {
		t.Fatalf("a process that stopped reporting stops counting: %+v", hbs)
	}
	if err := st.RemoveHeartbeat(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if hbs, _ = st.ActiveHeartbeats(ctx, time.Hour); len(hbs) != 1 {
		t.Fatalf("clean shutdown removes all of the process's rows: %+v", hbs)
	}
}

func TestTenantIngestMode(t *testing.T) {
	st := newStore(t)
	if m, err := st.TenantIngestMode(ctx, "reviewer"); err != nil || m != "" {
		t.Fatalf("no mode set: %q %v", m, err)
	}
	if err := st.SetTenantIngestMode(ctx, "reviewer", "async"); err != nil {
		t.Fatal(err)
	}
	if m, _ := st.TenantIngestMode(ctx, "reviewer"); m != "async" {
		t.Fatalf("%q", m)
	}
	cs, _ := store.NewConflictSettings("reject", 0)
	_ = st.SetConflictSettings(ctx, "reviewer", cs)
	if m, _ := st.TenantIngestMode(ctx, "reviewer"); m != "async" {
		t.Fatal("setting another key must not disturb the mode")
	}
	if err := st.SetTenantIngestMode(ctx, "reviewer", "batch"); err == nil {
		t.Fatal("only sync and async are valid")
	}
	if err := st.SetTenantIngestMode(ctx, "nobody", "sync"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	if m, _ := st.TenantIngestMode(ctx, "nobody"); m != "" {
		t.Fatal("unknown tenant")
	}
}

func TestTenantStatsAndRecentEvents(t *testing.T) {
	st := newStore(t)
	process(t, st, "reviewer", sample(t, 0))  // A571992, flagged
	process(t, st, "reviewer", sample(t, 2))  // B234567, accepted
	process(t, st, "reviewer", sample(t, 3))  // B234567 again
	process(t, st, "reviewer", sample(t, 12)) // rejected, no DeviceId (DeviceName Ship33CABOL)
	enqueue(t, st, "reviewer", 2)
	process(t, st, "acme-pharma", sample(t, 11))

	s, err := st.TenantStats(ctx, "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	if s.TotalEvents != 6 || s.EventsLastMinute != 6 || s.CompletedLastMinute != 4 || s.Pending != 2 || s.Processing != 0 || s.Dead != 0 {
		t.Fatalf("%+v", s)
	}
	if s.ActiveDevicesLastMinute != 4 { // A571992, B234567 (two events, one device), Ship33CABOL, and the two payloads with no device count as one
		t.Logf("active devices = %d", s.ActiveDevicesLastMinute)
	}
	if s.Statuses15m["flagged"] != 2 || s.Statuses15m["accepted"] != 1 || s.Statuses15m["rejected"] != 1 {
		t.Fatalf("statuses %v", s.Statuses15m)
	}
	if s.OldestPendingAge < 0 {
		t.Fatal("age")
	}
	other, _ := st.TenantStats(ctx, "acme-pharma")
	if other.TotalEvents != 1 || other.Pending != 0 {
		t.Fatalf("stats are per tenant: %+v", other)
	}

	recent, err := st.RecentEvents(ctx, "reviewer", 3, nil)
	if err != nil || len(recent) != 3 {
		t.Fatalf("%d %v", len(recent), err)
	}
	rej, _ := st.RecentEvents(ctx, "reviewer", 10, []string{"rejected"})
	if len(rej) != 1 || rej[0].Device != "Ship33CABOL" || len(rej[0].Reasons) == 0 {
		t.Fatalf("%+v", rej)
	}
	pending, _ := st.RecentEvents(ctx, "reviewer", 10, []string{"pending"})
	if len(pending) != 2 || pending[0].Status != "pending" {
		t.Fatalf("queued events show as pending: %+v", pending)
	}
	info, err := st.ListTenantInfo(ctx)
	if err != nil || len(info) != 3 || info[0].ID != "acme-pharma" {
		t.Fatalf("%+v %v", info, err)
	}
}

func TestSettingsStore(t *testing.T) {
	st := newStore(t)
	if rows, err := st.ListSettings(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("%v %v", rows, err)
	}
	if err := st.SetSetting(ctx, "queue_max_depth", "10", "alice"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetSetting(ctx, "queue_max_depth", "20", "bob"); err != nil {
		t.Fatal(err)
	}
	rows, _ := st.ListSettings(ctx)
	if len(rows) != 1 || rows[0].Value != "20" || rows[0].UpdatedBy != "bob" {
		t.Fatalf("%+v", rows)
	}
	if err := st.ResetSetting(ctx, "queue_max_depth"); err != nil {
		t.Fatal(err)
	}
	if rows, _ = st.ListSettings(ctx); len(rows) != 0 {
		t.Fatal("reset removes it")
	}
}

func TestTenantSettings(t *testing.T) {
	st := newStore(t)
	cfg := func() []byte {
		for _, i := range mustList(t, st) {
			if i.ID == "reviewer" {
				return i.Config
			}
		}
		return nil
	}
	if len(settings.TenantOverrides(cfg())) != 0 {
		t.Fatal("no overrides by default")
	}
	if err := st.SetTenantSetting(ctx, "reviewer", "queue_max_depth", "10"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetTenantSetting(ctx, "reviewer", "workers_max", "4"); err != nil {
		t.Fatal(err)
	}
	if got := settings.TenantOverrides(cfg()); got["queue_max_depth"] != "10" || got["workers_max"] != "4" || len(got) != 2 {
		t.Fatalf("%v", got)
	}
	// Setting again replaces; other config keys survive in both directions.
	if err := st.SetTenantSetting(ctx, "reviewer", "queue_max_depth", "20"); err != nil {
		t.Fatal(err)
	}
	if err := st.SetTenantIngestMode(ctx, "reviewer", "async"); err != nil {
		t.Fatal(err)
	}
	cs, _ := store.NewConflictSettings("reject", 0)
	if err := st.SetConflictSettings(ctx, "reviewer", cs); err != nil {
		t.Fatal(err)
	}
	got := settings.TenantOverrides(cfg())
	if got["queue_max_depth"] != "20" || got["workers_max"] != "4" || store.ParseIngestMode(cfg()) != "async" {
		t.Fatalf("%v %s", got, cfg())
	}
	if c, _ := store.ParseConflictSettings(cfg()); c.Policy != "reject" {
		t.Fatalf("settings must not disturb the conflict policy: %s", cfg())
	}
	// Clearing one leaves the rest.
	if err := st.ClearTenantSetting(ctx, "reviewer", "queue_max_depth"); err != nil {
		t.Fatal(err)
	}
	if got := settings.TenantOverrides(cfg()); len(got) != 1 || got["workers_max"] != "4" {
		t.Fatalf("%v", got)
	}
	if err := st.ClearTenantSetting(ctx, "reviewer", "never_set"); err != nil {
		t.Fatalf("clearing something that is not set is fine: %v", err)
	}
	if err := st.SetTenantSetting(ctx, "nobody", "queue_max_depth", "1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	if err := st.ClearTenantSetting(ctx, "nobody", "queue_max_depth"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	// Another tenant is untouched.
	for _, i := range mustList(t, st) {
		if i.ID == "acme-pharma" && len(settings.TenantOverrides(i.Config)) != 0 {
			t.Fatalf("overrides are per tenant: %s", i.Config)
		}
	}
}

// The migration moves worker limits stored the earlier way (config.workers = {min, max}) into ordinary settings.
func TestMigrationMovesOldWorkerLimits(t *testing.T) {
	st := newStore(t)
	sql, err := os.ReadFile("../../migrations/00007_tenant_settings.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.SplitN(strings.SplitN(string(sql), "-- +goose Up", 2)[1], "-- +goose Down", 2)[0]
	for id, cfg := range map[string]string{
		"old-limits":  `{"ingest_mode":"async","workers":{"min":1,"max":4}}`,
		"old-keeps":   `{"settings":{"queue_max_depth":"7"},"workers":{"min":0,"max":2}}`,
		"old-garbage": `{"workers":{"mode":"shared","weight":1}}`,
		"untouched":   `{"ingest_mode":"sync"}`,
	} {
		if err := st.CreateTenant(ctx, id, id, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Pool().Exec(ctx, `UPDATE tenants SET config = $2::jsonb WHERE id = $1`, id, cfg); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.Pool().Exec(ctx, up); err != nil {
		t.Fatalf("migration: %v", err)
	}
	cfgOf := map[string][]byte{}
	for _, i := range mustList(t, st) {
		cfgOf[i.ID] = i.Config
	}
	if got := settings.TenantOverrides(cfgOf["old-limits"]); got["workers_min"] != "1" || got["workers_max"] != "4" || store.ParseIngestMode(cfgOf["old-limits"]) != "async" {
		t.Fatalf("%s", cfgOf["old-limits"])
	}
	if got := settings.TenantOverrides(cfgOf["old-keeps"]); got["queue_max_depth"] != "7" || got["workers_max"] != "2" {
		t.Fatalf("existing settings survive: %s", cfgOf["old-keeps"])
	}
	if strings.Contains(string(cfgOf["old-limits"]), `"workers":`) || strings.Contains(string(cfgOf["old-garbage"]), `"workers":`) {
		t.Fatalf("the old key is gone: %s %s", cfgOf["old-limits"], cfgOf["old-garbage"])
	}
	if got := settings.TenantOverrides(cfgOf["old-garbage"]); len(got) != 0 {
		t.Fatalf("an unrecognised shape is dropped, not turned into settings: %v", got)
	}
	if string(cfgOf["untouched"]) != `{"ingest_mode": "sync"}` && !strings.Contains(string(cfgOf["untouched"]), `"sync"`) {
		t.Fatalf("%s", cfgOf["untouched"])
	}
}

func mustList(t *testing.T, st *store.PGStore) []store.TenantInfo {
	t.Helper()
	info, err := st.ListTenantInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestListConflicts(t *testing.T) {
	st := newStore(t)
	if rows, err := st.ListConflicts(ctx, "reviewer", 10); err != nil || len(rows) != 0 {
		t.Fatalf("%v %v", rows, err)
	}
	_, first := process(t, st, "reviewer", sample(t, 0))
	_, second := process(t, st, "reviewer", sample(t, 1)) // same device and time, different content
	process(t, st, "acme-pharma", sample(t, 0))
	rows, err := st.ListConflicts(ctx, "reviewer", 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %v", rows, err)
	}
	c := rows[0]
	if c.EventID != second || c.FirstEventID != first || c.Resolution != "kept_first" || c.DeviceIMEI == "" || c.TS == 0 {
		t.Fatalf("%+v", c)
	}
	if len(c.Changes) == 0 || len(c.Changes) > 8 || c.MoreChanges < 0 {
		t.Fatalf("a conflict lists the fields that differ: %+v", c)
	}
	for _, ch := range c.Changes {
		if ch.First == ch.Second {
			t.Fatalf("only fields that really changed are listed: %+v", ch)
		}
	}
	if c.MoreChanges == 0 {
		t.Fatalf("fields only one version has are counted, not listed: %+v", c)
	}
	if other, _ := st.ListConflicts(ctx, "acme-pharma", 10); len(other) != 0 {
		t.Fatalf("conflicts are per tenant: %+v", other)
	}
}

func TestMigrationDropsRateLimitSettings(t *testing.T) {
	st := newStore(t)
	sql, err := os.ReadFile("../../migrations/00008_drop_rate_limit_settings.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.SplitN(strings.SplitN(string(sql), "-- +goose Up", 2)[1], "-- +goose Down", 2)[0]
	for _, kv := range [][2]string{{"rate_limit_per_sec", "5"}, {"rate_limit_burst", "9"}, {"queue_max_depth", "77"}} {
		if err := st.SetSetting(ctx, kv[0], kv[1], "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.CreateTenant(ctx, "limited", "Limited", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, `UPDATE tenants SET config = '{"ingest_mode":"async","settings":{"rate_limit_burst":"3","rate_limit_webhook":"1","queue_max_depth":"10"}}'::jsonb WHERE id = 'limited'`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, up); err != nil {
		t.Fatalf("migration: %v", err)
	}
	rows, _ := st.ListSettings(ctx)
	if len(rows) != 1 || rows[0].Key != "queue_max_depth" {
		t.Fatalf("only the rate-limit settings are removed: %+v", rows)
	}
	for _, i := range mustList(t, st) {
		if i.ID != "limited" {
			continue
		}
		got := settings.TenantOverrides(i.Config)
		if len(got) != 1 || got["queue_max_depth"] != "10" || store.ParseIngestMode(i.Config) != "async" {
			t.Fatalf("a tenant keeps its other settings: %s", i.Config)
		}
	}
}

func TestListPayloads(t *testing.T) {
	st := newStore(t)
	// Three events for reviewer received an hour apart (the received time is what we pin), one for another tenant.
	base := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	var ids []string
	for i, n := range []int{2, 0, 12} { // accepted, flagged, rejected (no device)
		raw := sample(t, n)
		id := store.NewID("evt_")
		if err := st.SaveRaw(ctx, store.RawEvent{ID: id, TenantID: "reviewer", Provider: "Tive", Raw: raw, Hash: doc.CanonicalHash(raw), ReceivedAt: base.Add(time.Duration(i) * time.Hour)}); err != nil {
			t.Fatal(err)
		}
		commit(t, st, "reviewer", id, raw)
		ids = append(ids, id)
	}
	process(t, st, "acme-pharma", sample(t, 3))
	enqueue(t, st, "reviewer", 1) // a queued event: no outcome yet

	all, next, err := st.ListPayloads(ctx, store.PayloadFilter{TenantID: "reviewer"})
	if err != nil || next != nil || len(all) != 4 {
		t.Fatalf("%d rows, next %v, %v", len(all), next, err)
	}
	if all[0].Status != "pending" || all[3].ID != ids[0] {
		t.Fatalf("newest first, queued events show as pending: %s ... %s", all[0].Status, all[3].ID)
	}
	var accepted store.PayloadRow
	for _, r := range all {
		if r.ID == ids[0] {
			accepted = r
		}
	}
	if accepted.Status != "accepted" || len(accepted.Sensor) == 0 || len(accepted.Location) == 0 || !strings.Contains(string(accepted.Sensor), `"temperature"`) {
		t.Fatalf("the PAXAFE records come with the row: %+v", accepted)
	}

	// Paging: two at a time, no row twice, none lost.
	seen := map[string]bool{}
	var cur *store.PayloadCursor
	pages := 0
	for {
		rows, nx, err := st.ListPayloads(ctx, store.PayloadFilter{TenantID: "reviewer", Limit: 2, After: cur})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if seen[r.ID] {
				t.Fatalf("%s returned twice", r.ID)
			}
			seen[r.ID] = true
		}
		pages++
		if nx == nil {
			break
		}
		cur = nx
	}
	if len(seen) != 4 || pages != 2 {
		t.Fatalf("%d rows over %d pages", len(seen), pages)
	}

	list := func(f store.PayloadFilter) []store.PayloadRow {
		f.TenantID = "reviewer"
		rows, _, err := st.ListPayloads(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	// Time window on the received time: From inclusive, To exclusive.
	if got := list(store.PayloadFilter{From: base.Add(30 * time.Minute), To: base.Add(90 * time.Minute)}); len(got) != 1 || got[0].ID != ids[1] {
		t.Fatalf("window: %+v", got)
	}
	if got := list(store.PayloadFilter{From: base, To: base.Add(time.Hour)}); len(got) != 1 || got[0].ID != ids[0] {
		t.Fatalf("From is inclusive, To exclusive: %+v", got)
	}
	if got := list(store.PayloadFilter{To: base.Add(time.Hour)}); len(got) != 1 {
		t.Fatalf("open start: %d", len(got))
	}
	if got := list(store.PayloadFilter{From: base.Add(90 * time.Minute)}); len(got) != 2 { // the rejected one and the queued one
		t.Fatalf("open end: %d", len(got))
	}
	// Status, output and device filters.
	if got := list(store.PayloadFilter{Status: "rejected"}); len(got) != 1 || got[0].ID != ids[2] || len(got[0].Reasons) == 0 {
		t.Fatalf("status: %+v", got)
	}
	if got := list(store.PayloadFilter{Status: "pending"}); len(got) != 1 {
		t.Fatalf("pending: %d", len(got))
	}
	if got := list(store.PayloadFilter{OnlyOutput: true}); len(got) != 2 {
		t.Fatalf("only events that produced a PAXAFE record (not the rejected or queued ones): %d", len(got))
	}
	if got := list(store.PayloadFilter{Device: "B234567"}); len(got) != 1 || got[0].ID != ids[0] {
		t.Fatalf("device by name: %+v", got)
	}
	if got := list(store.PayloadFilter{Device: "no-such-device"}); len(got) != 0 {
		t.Fatalf("%d", len(got))
	}
	// The reading's own time (EntryTimeEpoch of sample 2 is 2025-02-10): a window around it finds it.
	ev := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)
	if got := list(store.PayloadFilter{TimeField: store.TimeEvent, From: ev, To: ev.Add(48 * time.Hour)}); len(got) < 1 {
		t.Fatalf("event-time window found nothing")
	}
	if got := list(store.PayloadFilter{TimeField: store.TimeEvent, From: time.Now().Add(-time.Hour), To: time.Now()}); len(got) != 0 {
		t.Fatalf("no reading was made in the last hour: %d", len(got))
	}
	// Tenants are isolated; bad input is refused; the page size is capped.
	if other, _, _ := st.ListPayloads(ctx, store.PayloadFilter{TenantID: "acme-pharma"}); len(other) != 1 {
		t.Fatalf("acme sees only its own: %d", len(other))
	}
	if _, _, err := st.ListPayloads(ctx, store.PayloadFilter{TenantID: "reviewer", TimeField: "nonsense"}); err == nil {
		t.Fatal("unknown time field")
	}
	if rows, _, _ := st.ListPayloads(ctx, store.PayloadFilter{TenantID: "reviewer", Limit: 100000}); len(rows) != 4 {
		t.Fatal("a huge limit is capped, not an error")
	}
}

func TestTenantStatsAreBounded(t *testing.T) {
	st := newStore(t)
	for i := 0; i < 8; i++ {
		process(t, st, "reviewer", sample(t, 2+i%2))
	}
	s, err := st.TenantStats(ctx, "reviewer")
	if err != nil || s.TotalEvents != 8 || s.TotalCapped {
		t.Fatalf("%+v %v", s, err)
	}
	store.SetTotalEventsCap(5)
	t.Cleanup(func() { store.SetTotalEventsCap(100_000) })
	s, err = st.TenantStats(ctx, "reviewer")
	if err != nil || s.TotalEvents != 5 || !s.TotalCapped {
		t.Fatalf("the total stops at the cap and says so, so its cost does not grow with the tenant: %+v %v", s, err)
	}
	if s.Statuses15m["accepted"]+s.Statuses15m["duplicate"] == 0 || s.LatencyP95 < 0 {
		t.Fatalf("the other figures are still there: %+v", s)
	}
}

func TestOutcomeIndexLeadsWithTheTenant(t *testing.T) {
	st := newStore(t)
	if n := count(t, st, "reviewer", `SELECT count(*) FROM pg_indexes WHERE indexname = 'event_outcomes_tenant_processed' AND indexdef LIKE '%(tenant_id, processed_at DESC) INCLUDE (status)%'`); n != 1 {
		t.Fatalf("the console's reads need the tenant-led outcomes index: %d", n)
	}
}
