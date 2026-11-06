package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	if first.Status != "flagged" { // sample 0 carries IDENTITY_MISMATCH and TEMP_EXCURSION
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
