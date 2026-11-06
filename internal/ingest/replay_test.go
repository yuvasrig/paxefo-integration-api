package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

var ctx = context.Background()

// failOn makes the apply-mode commit of one event fail, to check that a replay carries on and leaves it alone.
type failOn struct {
	*store.PGStore
	mu  sync.Mutex
	ids map[string]bool
}

func (f *failOn) Commit(c context.Context, in store.CommitInput) (store.CommitResult, error) {
	f.mu.Lock()
	bad := f.ids[in.RawEventID]
	f.mu.Unlock()
	if bad && in.ReplayRunID != "" {
		return store.CommitResult{}, errors.New("injected failure")
	}
	return f.PGStore.Commit(c, in)
}

type env struct {
	t      *testing.T
	pg     *failOn
	svc    *ingest.Service
	holder *rules.Holder
	clock  time.Time
}

func newEnv(t *testing.T, tenants ...string) *env {
	t.Helper()
	pg, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{MaxConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tenants) == 0 {
		tenants = []string{"reviewer"}
	}
	for _, id := range tenants {
		if err := pg.CreateTenant(ctx, id, id, nil); err != nil {
			t.Fatal(err)
		}
	}
	rs, err := rules.Default()
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, pg: &failOn{PGStore: pg, ids: map[string]bool{}}, holder: rules.NewHolder(rs, rules.SourceDefault, time.Now()), clock: time.Date(2026, 11, 5, 8, 0, 0, 0, time.UTC)}
	e.svc = &ingest.Service{
		Store: e.pg, Provider: tive.NewDynamic(e.holder, config.Default()),
		Now: func() time.Time { return e.clock }, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	return e
}

func sample(t *testing.T, n int) []byte {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", n))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ingest sends a payload as the tenant at the given hour of the test clock and returns its event id.
func (e *env) ingest(tenant string, hour int, raw []byte) string {
	e.t.Helper()
	e.clock = time.Date(2026, 11, 5, hour, 0, 0, 0, time.UTC)
	resp, err := e.svc.Ingest(ctx, ingest.Request{TenantID: tenant, RequestID: "req", Body: raw})
	if err != nil {
		e.t.Fatal(err)
	}
	return resp.EventID
}

func (e *env) status(tenant, id string) (status string, attempts int, rulesVersion string) {
	e.t.Helper()
	ev, err := e.pg.GetEvent(ctx, tenant, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return ev.Status, ev.Attempts, ev.RulesVersion
}

func (e *env) flags(tenant, id string) string {
	e.t.Helper()
	ev, _ := e.pg.GetEvent(ctx, tenant, id)
	var codes []string
	for _, f := range ev.Flags {
		codes = append(codes, f.Code)
	}
	return strings.Join(codes, ",")
}

func (e *env) count(tenant, query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pg.WithTenantTx(ctx, tenant, func(tx pgx.Tx) error { return tx.QueryRow(ctx, query, args...).Scan(&n) }); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func hour(h int) time.Time { return time.Date(2026, 11, 5, h, 0, 0, 0, time.UTC) }

func (e *env) window(tenant string, from, to int) ingest.ReplayOptions {
	return ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: tenant, From: hour(from), To: hour(to)}}
}

// withHumidityRule makes the ruleset flag humidity above 50, as an operator's published fix would.
func (e *env) withHumidityRule(version string) {
	e.t.Helper()
	yaml := strings.Replace(string(rules.DefaultYAML()), "overrides: []", "", 1)
	yaml = strings.Replace(yaml, "version: "+e.holder.Current().Version, "version: "+version, 1) +
		"  - { id: HUMIDITY-HIGH, code: HUMIDITY_HIGH, field: Humidity.Percentage, when: { outside: [0, 50] }, action: flag, severity: warn }\n"
	rs, err := rules.Parse([]byte(yaml))
	if err != nil {
		e.t.Fatal(err)
	}
	e.holder.Set(rs, rules.SourceDatabase, time.Now())
}

// "Fix the rules, then replay": a dry run reports exactly what would change and writes nothing; applying it
// recomputes the events, archives their old outcomes and records the run.
func TestReplayAppliesAFixedRule(t *testing.T) {
	e := newEnv(t)
	a := e.ingest("reviewer", 9, sample(t, 2))   // humidity 55.3, accepted
	b := e.ingest("reviewer", 10, sample(t, 3))  // humidity 55, flagged (SENSOR_FAULT)
	c := e.ingest("reviewer", 11, sample(t, 11)) // humidity 54.8, accepted
	d := e.ingest("reviewer", 12, sample(t, 10)) // no humidity, accepted
	baseVersion := e.holder.Current().Version

	// Nothing changed yet: replaying is a no-op.
	rep, err := e.svc.ReplayWindow(ctx, e.window("reviewer", 0, 24))
	if err != nil || rep.Selected != 4 || rep.Changed != 0 || rep.Unchanged != 4 || rep.Applied {
		t.Fatalf("%+v %v", rep, err)
	}

	e.withHumidityRule("fix-1")
	rep, err = e.svc.ReplayWindow(ctx, e.window("reviewer", 0, 24))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Selected != 4 || rep.Reprocessed != 4 || rep.Changed != 3 || rep.Unchanged != 1 || rep.Errors != 0 || rep.Transitions["accepted -> flagged"] != 2 {
		t.Fatalf("dry run: %+v", rep)
	}
	// ...and it changed nothing.
	for _, id := range []string{a, b, c, d} {
		if s, attempts, v := e.status("reviewer", id); attempts != 1 || v != baseVersion || (id == a && s != "accepted") {
			t.Fatalf("dry run changed %s: %s attempts=%d rules=%s", id, s, attempts, v)
		}
	}
	if n := e.count("reviewer", `SELECT count(*) FROM event_outcome_history`); n != 0 {
		t.Fatalf("history rows after a dry run: %d", n)
	}
	if runs, _ := e.pg.ListReplayRuns(ctx, 10); len(runs) != 0 {
		t.Fatal("a dry run must not be recorded as an applied run")
	}

	// Apply.
	opts := e.window("reviewer", 0, 24)
	opts.Apply, opts.By, opts.RunID = true, "alice", "run_test1"
	rep, err = e.svc.ReplayWindow(ctx, opts)
	if err != nil || !rep.Applied || rep.Changed != 3 || rep.Errors != 0 {
		t.Fatalf("apply: %+v %v", rep, err)
	}
	if s, attempts, v := e.status("reviewer", a); s != "flagged" || attempts != 2 || v != "fix-1" || e.flags("reviewer", a) != "HUMIDITY_HIGH" {
		t.Fatalf("a: %s attempts=%d rules=%s flags=%s", s, attempts, v, e.flags("reviewer", a))
	}
	if s, _, _ := e.status("reviewer", d); s != "accepted" {
		t.Fatal("an event the new rule does not touch stays accepted")
	}
	if e.flags("reviewer", b) != "SENSOR_FAULT,HUMIDITY_HIGH" {
		t.Fatalf("b flags %s", e.flags("reviewer", b))
	}
	// The raw payloads and the readings are all still there, once.
	if n := e.count("reviewer", `SELECT count(*) FROM raw_events`); n != 4 {
		t.Fatalf("raw events %d", n)
	}
	if n := e.count("reviewer", `SELECT count(*) FROM sensor_readings`); n != 4 {
		t.Fatalf("sensor readings %d", n)
	}
	// Old outcomes are archived, and the run is on record.
	var oldStatus, oldRules, runID string
	err = e.pg.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, rules_version, replay_run_id FROM event_outcome_history WHERE raw_event_id = $1`, a).Scan(&oldStatus, &oldRules, &runID)
	})
	if err != nil || oldStatus != "accepted" || oldRules != baseVersion || runID != "run_test1" {
		t.Fatalf("history: %s %s %s %v", oldStatus, oldRules, runID, err)
	}
	runs, err := e.pg.ListReplayRuns(ctx, 10)
	if err != nil || len(runs) != 1 || runs[0].RunID != "run_test1" || runs[0].RequestedBy != "alice" || runs[0].Changed != 3 || runs[0].Transitions["accepted -> flagged"] != 2 {
		t.Fatalf("runs %+v %v", runs, err)
	}

	// Applying it again changes nothing: the replay is idempotent.
	opts.RunID = "run_test2"
	if rep, err = e.svc.ReplayWindow(ctx, opts); err != nil || rep.Changed != 0 || rep.Unchanged != 4 {
		t.Fatalf("second apply: %+v %v", rep, err)
	}
	if n := e.count("reviewer", `SELECT count(*) FROM sensor_readings`); n != 4 {
		t.Fatalf("sensor readings after a second apply: %d", n)
	}
}

// "Fix the conflicts": after a tenant switches from keep_first to keep_last, replaying the window applies the new policy,
// processing oldest first, and never lets an older event overwrite a later one.
func TestReplayAppliesAChangedConflictPolicy(t *testing.T) {
	e := newEnv(t)
	first := e.ingest("reviewer", 9, sample(t, 2))
	var m map[string]any
	_ = json.Unmarshal(sample(t, 2), &m)
	m["Humidity"].(map[string]any)["Percentage"] = 99.9
	changed, _ := json.Marshal(m)
	second := e.ingest("reviewer", 10, changed)

	humidity := func() float64 {
		var h float64
		_ = e.pg.WithTenantTx(ctx, "reviewer", func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT humidity FROM sensor_readings`).Scan(&h)
		})
		return h
	}
	if s, _, _ := e.status("reviewer", second); s != "conflict" || humidity() != 55.3 {
		t.Fatalf("under keep_first the first reading stays: %s %v", s, humidity())
	}

	cs, _ := store.NewConflictSettings("keep_last", 0)
	if err := e.pg.SetConflictSettings(ctx, "reviewer", cs); err != nil {
		t.Fatal(err)
	}

	// Dry run: the stored reading would change, and nothing is written.
	rep, err := e.svc.ReplayWindow(ctx, e.window("reviewer", 0, 24))
	if err != nil || rep.Selected != 2 || rep.Changed != 1 || rep.Unchanged != 1 {
		t.Fatalf("dry run: %+v %v", rep, err)
	}
	if humidity() != 55.3 {
		t.Fatal("a dry run must not change the readings")
	}

	// Apply: the newer version wins.
	opts := e.window("reviewer", 0, 24)
	opts.Apply = true
	if rep, err = e.svc.ReplayWindow(ctx, opts); err != nil || rep.Errors != 0 {
		t.Fatalf("%+v %v", rep, err)
	}
	if humidity() != 99.9 {
		t.Fatalf("keep_last: the later version must be stored, got %v", humidity())
	}
	if s, _, _ := e.status("reviewer", first); s != "accepted" {
		t.Fatalf("the first event is still accepted, got %s", s)
	}
	if n := e.count("reviewer", `SELECT count(*) FROM reading_conflicts WHERE resolution = 'replaced'`); n != 1 {
		t.Fatalf("replaced conflicts: %d", n)
	}

	// Replaying only the OLDER event must not put the old version back over the newer one.
	older := ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: "reviewer", From: hour(0), To: hour(10)}, Apply: true}
	if rep, err = e.svc.ReplayWindow(ctx, older); err != nil || rep.Selected != 1 || rep.Errors != 0 {
		t.Fatalf("%+v %v", rep, err)
	}
	if humidity() != 99.9 {
		t.Fatalf("an older event must never overwrite a later one, got %v", humidity())
	}
}

// Filters: time window (received or event time), device (IMEI or name), account, status; each narrows the selection.
func TestReplayFilters(t *testing.T) {
	e := newEnv(t)
	acct := func(raw []byte, id int) []byte {
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		m["AccountId"] = id
		b, _ := json.Marshal(m)
		return b
	}
	e.ingest("reviewer", 9, acct(sample(t, 2), 478))  // B234567, 1739302046000
	e.ingest("reviewer", 10, acct(sample(t, 3), 999)) // B234567, 1739305646000
	e.ingest("reviewer", 11, acct(sample(t, 5), 478)) // E567890, epoch in seconds 1739561246
	e.ingest("reviewer", 12, []byte(`{not json`))     // rejected, no event time
	e.ingest("reviewer", 13, sample(t, 12))           // rejected (unknown device)

	count := func(f store.ReplayFilter) int {
		f.TenantID = "reviewer"
		if f.From.IsZero() {
			f.From, f.To = hour(0), hour(23)
		}
		rep, err := e.svc.ReplayWindow(ctx, ingest.ReplayOptions{Filter: f})
		if err != nil {
			t.Fatal(err)
		}
		return rep.Selected
	}

	if got := count(store.ReplayFilter{}); got != 5 {
		t.Fatalf("all: %d", got)
	}
	// Received-time window: from inclusive, to exclusive.
	for name, tc := range map[string]struct {
		from, to int
		want     int
	}{"first two hours": {9, 11, 2}, "exactly one": {10, 11, 1}, "to is exclusive": {9, 10, 1}, "none": {14, 20, 0}} {
		if got := count(store.ReplayFilter{From: hour(tc.from), To: hour(tc.to)}); got != tc.want {
			t.Errorf("received window %s: %d, want %d", name, got, tc.want)
		}
	}
	// Event-time window: milliseconds, and seconds are understood; events with no readable epoch are not selected.
	ms := func(v int64) time.Time { return time.UnixMilli(v).UTC() }
	for name, tc := range map[string]struct {
		from, to int64
		want     int
	}{"the two B234567 readings": {1739302046000, 1739305646001, 2}, "just the second": {1739305646000, 1739305646001, 1}, "the seconds epoch": {1739561246000, 1739561247000, 1}, "nothing": {1000, 2000, 0}} {
		if got := count(store.ReplayFilter{TimeField: store.TimeEvent, From: ms(tc.from), To: ms(tc.to)}); got != tc.want {
			t.Errorf("event window %s: %d, want %d", name, got, tc.want)
		}
	}
	if got := count(store.ReplayFilter{TimeField: store.TimeEvent, From: ms(0), To: ms(4102444800000)}); got != 4 {
		t.Errorf("event time over all of history: %d (the non-JSON event has no event time; sample 12 has an epoch)", got)
	}
	// Device: by IMEI or by name.
	if got := count(store.ReplayFilter{Device: "866088073468440"}); got != 2 {
		t.Errorf("device by IMEI: %d", got)
	}
	if got := count(store.ReplayFilter{Device: "B234567"}); got != 2 {
		t.Errorf("device by name: %d", got)
	}
	if got := count(store.ReplayFilter{Device: "E567890"}); got != 1 {
		t.Errorf("another device: %d", got)
	}
	// Account.
	if got := count(store.ReplayFilter{Account: "478"}); got != 2 {
		t.Errorf("account 478: %d", got)
	}
	if got := count(store.ReplayFilter{Account: "999"}); got != 1 {
		t.Errorf("account 999: %d", got)
	}
	// Status, and combinations.
	if got := count(store.ReplayFilter{Statuses: []string{"rejected"}}); got != 2 {
		t.Errorf("status rejected: %d", got)
	}
	if got := count(store.ReplayFilter{Statuses: []string{"accepted", "flagged"}}); got != 3 {
		t.Errorf("status accepted/flagged: %d", got)
	}
	if got := count(store.ReplayFilter{Device: "B234567", Account: "999"}); got != 1 {
		t.Errorf("device and account: %d", got)
	}
	if got := count(store.ReplayFilter{Device: "B234567", Account: "999", Statuses: []string{"rejected"}}); got != 0 {
		t.Errorf("all filters together: %d", got)
	}
	// The limit caps the run.
	rep, _ := e.svc.ReplayWindow(ctx, ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: "reviewer", From: hour(0), To: hour(23)}, Limit: 2})
	if rep.Selected != 2 {
		t.Errorf("limit: %d", rep.Selected)
	}
	if _, err := e.pg.GetEvent(ctx, "reviewer", "evt_none"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("sanity")
	}
	if _, _, _, err := e.pg.ListReplayEvents(ctx, store.ReplayFilter{TenantID: "reviewer", TimeField: "bogus", From: hour(0), To: hour(1)}, store.ReplayCursor{}, 10); err == nil {
		t.Fatal("an unknown time field must be an error")
	}
}

// A rejected event is the classic "fix and replay": once the missing context exists, replaying it works.
func TestReplayRecoversARejectedEvent(t *testing.T) {
	e := newEnv(t)
	byName := []byte(`{"DeviceName":"A571992","EntryTimeEpoch":1739215700000,"Temperature":{"Celsius":10}}`) // a different time than sample 0, so no conflict
	rejected := e.ingest("reviewer", 9, byName)
	if s, _, _ := e.status("reviewer", rejected); s != "rejected" {
		t.Fatalf("%s", s)
	}
	e.ingest("reviewer", 10, sample(t, 0)) // registers A571992

	opts := ingest.ReplayOptions{Filter: store.ReplayFilter{TenantID: "reviewer", From: hour(0), To: hour(24), Statuses: []string{"rejected"}}, Apply: true}
	rep, err := e.svc.ReplayWindow(ctx, opts)
	if err != nil || rep.Selected != 1 || rep.Changed != 1 || rep.Transitions["rejected -> flagged"] != 1 {
		t.Fatalf("%+v %v", rep, err)
	}
	if s, _, _ := e.status("reviewer", rejected); s != "flagged" || e.flags("reviewer", rejected) != "DEVICE_RESOLVED_BY_NAME" {
		t.Fatalf("%s %s", s, e.flags("reviewer", rejected))
	}
}

// Every tenant is replayed on its own: the data of one never leaks into another's report or results.
func TestReplayAllTenantsIsIsolated(t *testing.T) {
	e := newEnv(t, "acme-pharma", "bioco", "reviewer")
	e.ingest("acme-pharma", 9, sample(t, 2))
	e.ingest("acme-pharma", 10, sample(t, 3))
	e.ingest("bioco", 11, sample(t, 11))
	e.withHumidityRule("fix-1")

	opts := ingest.ReplayOptions{Filter: store.ReplayFilter{From: hour(0), To: hour(24)}, Apply: true, By: "ops"}
	reports, err := e.svc.ReplayAllTenants(ctx, opts)
	if err != nil || len(reports) != 3 {
		t.Fatalf("%+v %v", reports, err)
	}
	by := map[string]ingest.ReplayReport{}
	for _, r := range reports {
		by[r.Tenant] = r
	}
	if by["acme-pharma"].Selected != 2 || by["bioco"].Selected != 1 || by["reviewer"].Selected != 0 {
		t.Fatalf("selected: %+v", by)
	}
	if by["acme-pharma"].RunID != by["bioco"].RunID {
		t.Fatal("one run id for the whole invocation")
	}
	if n := e.count("acme-pharma", `SELECT count(*) FROM event_outcome_history`); n != 2 {
		t.Fatalf("acme history rows: %d", n)
	}
	if n := e.count("bioco", `SELECT count(*) FROM event_outcome_history`); n != 1 {
		t.Fatalf("bioco history rows: %d", n)
	}
	if runs, _ := e.pg.ListReplayRuns(ctx, 10); len(runs) != 3 {
		t.Fatalf("one audit row per tenant, even one with no matching events: %d", len(runs))
	}
}

// One event that cannot be reprocessed is reported and left exactly as it was; the others carry on.
func TestReplayErrorsLeaveOutcomesAlone(t *testing.T) {
	e := newEnv(t)
	a := e.ingest("reviewer", 9, sample(t, 2))
	b := e.ingest("reviewer", 10, sample(t, 3))
	e.withHumidityRule("fix-1")
	e.pg.mu.Lock()
	e.pg.ids[a] = true
	e.pg.mu.Unlock()

	opts := e.window("reviewer", 0, 24)
	opts.Apply = true
	rep, err := e.svc.ReplayWindow(ctx, opts)
	if err != nil || rep.Errors != 1 || rep.Reprocessed != 1 || len(rep.Failures) != 1 || !strings.Contains(rep.Failures[0], a) {
		t.Fatalf("%+v %v", rep, err)
	}
	if s, attempts, v := e.status("reviewer", a); s != "accepted" || attempts != 1 || v == "fix-1" {
		t.Fatalf("the failed event keeps its previous outcome: %s %d %s", s, attempts, v)
	}
	if _, _, v := e.status("reviewer", b); v != "fix-1" {
		t.Fatalf("the other event was reprocessed: %s", v)
	}
	if n := e.count("reviewer", `SELECT count(*) FROM sensor_readings`); n != 2 {
		t.Fatalf("no reading may be lost by a failed replay: %d", n)
	}
}

func TestReplayNeedsATenantAndSurvivesCancellation(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.ReplayWindow(ctx, ingest.ReplayOptions{}); err == nil {
		t.Fatal("a tenant is required")
	}
	e.ingest("reviewer", 9, sample(t, 2))
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.svc.ReplayWindow(cctx, e.window("reviewer", 0, 24)); err == nil {
		t.Fatal("a cancelled replay must return an error")
	}
}
