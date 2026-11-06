package settings

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

func base() Values {
	return Values{QueueMaxDepth: 5000, WorkersMin: 2, WorkersMax: 8, JobsPerWorker: 50, LagSLOSeconds: 10, MaxAttempts: 5}
}

func TestEveryDefHasAGetter(t *testing.T) {
	v := base()
	for _, d := range Defs() {
		if v.Get(d.Key) == "" {
			t.Errorf("%s has no getter", d.Key)
		}
		if _, ok := Lookup(d.Key); !ok || d.Title == "" || d.Help == "" {
			t.Errorf("%s needs a title and help", d.Key)
		}
	}
	if v.Get("lag_slo_seconds") != "10" || v.Get("queue_max_depth") != "5000" || v.Get("nope") != "" {
		t.Fatal("getter values")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		key, val string
		wantErr  string
	}{
		{"queue_max_depth", "250", ""},
		{"max_attempts", "1", ""},
		{"workers_min", "0", ""},
		{"workers_max", "16", ""},
		{"lag_slo_seconds", "0", "between"},
		{"lag_slo_seconds", "fast", "number"},
		{"max_attempts", "2.5", "whole"},
		{"rate_limit_per_sec", "100", "unknown"}, // rate limiting was removed: Tive cannot be throttled
		{"queue_max_depth", "-1", "between"},
		{"workers_max", "5000", "between"},
		{"max_attempts", "0", "between"},
		{"nope", "1", "unknown"},
		{"workers_min", "9", "cannot be more than workers_max"}, // 9 > 8
		{"workers_max", "1", "cannot be more than workers_max"}, // 1 < min 2
	}
	for _, tt := range tests {
		err := Validate(base(), tt.key, tt.val)
		if (err == nil) != (tt.wantErr == "") || (err != nil && !strings.Contains(err.Error(), tt.wantErr)) {
			t.Errorf("%s=%s: err %v, want %q", tt.key, tt.val, err, tt.wantErr)
		}
	}
}

func TestApplyIgnoresBadOverrides(t *testing.T) {
	rows := []store.SettingRow{
		{Key: "lag_slo_seconds", Value: "250"},
		{Key: "queue_max_depth", Value: "garbage"},
		{Key: "unknown", Value: "1"},
		{Key: "workers_min", Value: "99"},
		{Key: "workers_max", Value: "12"},
	}
	v, errs := Apply(base(), rows)
	if v.LagSLOSeconds != 250 || v.QueueMaxDepth != 5000 || v.WorkersMax != 12 || v.WorkersMin != 2 {
		t.Fatalf("values %+v", v)
	}
	if len(errs) != 3 {
		t.Fatalf("errors %v", errs)
	}
}

type fakeSource struct {
	rows    []store.SettingRow
	tenants []store.TenantInfo
	err     error
}

func (f *fakeSource) ListSettings(context.Context) ([]store.SettingRow, error) { return f.rows, f.err }
func (f *fakeSource) ListTenantInfo(context.Context) ([]store.TenantInfo, error) {
	return f.tenants, f.err
}

func TestSyncer(t *testing.T) {
	src := &fakeSource{}
	h := NewHolder(base())
	s := &Syncer{Source: src, Holder: h, Base: base(), Interval: 10 * time.Millisecond}
	if err := s.Refresh(context.Background()); err != nil || h.Current() != base() {
		t.Fatalf("no overrides: %v", err)
	}
	src.rows = []store.SettingRow{{Key: "queue_max_depth", Value: "10"}}
	_ = s.Refresh(context.Background())
	if h.Current().QueueMaxDepth != 10 {
		t.Fatal("override not applied")
	}
	src.err = errors.New("db down")
	if err := s.Refresh(context.Background()); err == nil || h.Current().QueueMaxDepth != 10 {
		t.Fatal("an error must leave the settings alone")
	}
	src.err, src.rows = nil, nil // reset
	_ = s.Refresh(context.Background())
	if h.Current().QueueMaxDepth != 5000 {
		t.Fatal("removing the override restores the default")
	}

	// Run picks changes up on its own and stops with its context.
	src.rows = []store.SettingRow{{Key: "max_attempts", Value: "9"}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	for i := 0; i < 200 && h.Current().MaxAttempts != 9; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if h.Current().MaxAttempts != 9 {
		t.Fatal("Run did not apply the change")
	}
}

func tenant(id, config string) store.TenantInfo {
	return store.TenantInfo{ID: id, Status: "active", Config: []byte(config)}
}

func TestTenantOverrides(t *testing.T) {
	got := TenantOverrides([]byte(`{"ingest_mode":"async","settings":{"queue_max_depth":"10","workers_max":4,"junk":true}}`))
	if len(got) != 2 || got["queue_max_depth"] != "10" || got["workers_max"] != "4" {
		t.Fatalf("strings and numbers are read, other kinds ignored: %v", got)
	}
	for _, raw := range []string{``, `null`, `[]`, `{}`, `{"settings":"x"}`, `not json`} {
		if got := TenantOverrides([]byte(raw)); len(got) != 0 {
			t.Errorf("%q: %v", raw, got)
		}
	}
}

func TestResolveTenantValueWinsOverGlobal(t *testing.T) {
	g := base()
	v, errs := Resolve(g, map[string]string{"queue_max_depth": "10", "workers_max": "3", "workers_min": "1"})
	if len(errs) != 0 || v.QueueMaxDepth != 10 || v.WorkersMax != 3 || v.WorkersMin != 1 {
		t.Fatalf("%+v %v", v, errs)
	}
	if v.LagSLOSeconds != g.LagSLOSeconds || v.MaxAttempts != g.MaxAttempts {
		t.Fatalf("anything the tenant does not set is the global value: %+v", v)
	}
	if g != base() {
		t.Fatal("the global values must not change")
	}
	if same, _ := Resolve(g, nil); same != g {
		t.Fatal("no overrides: exactly the global values")
	}
}

func TestResolveSkipsBadOverrides(t *testing.T) {
	g := base()
	v, errs := Resolve(g, map[string]string{
		"queue_max_depth": "garbage", "nonsense": "1", "workers_total_max": "9", "max_attempts": "7",
	})
	if v.QueueMaxDepth != g.QueueMaxDepth || v.MaxAttempts != 7 || v.WorkersTotalMax != g.WorkersTotalMax {
		t.Fatalf("only the valid override applies: %+v", v)
	}
	if len(errs) != 3 {
		t.Fatalf("each bad one is reported, including a global-only setting: %v", errs)
	}
	// min above max: the tenant falls back to the global worker limits instead of getting a broken pool.
	v, errs = Resolve(g, map[string]string{"workers_min": "9", "workers_max": "3"})
	if v.WorkersMin != g.WorkersMin || v.WorkersMax != g.WorkersMax || len(errs) != 1 {
		t.Fatalf("%+v %v", v, errs)
	}
}

func TestGlobalOnlyAndTenantDefs(t *testing.T) {
	if !GlobalOnly("workers_total_max") || GlobalOnly("queue_max_depth") {
		t.Fatal("only the process-wide cap is global only")
	}
	for _, d := range TenantDefs() {
		if d.Key == "workers_total_max" {
			t.Fatal("a tenant cannot set the process-wide cap")
		}
	}
	if len(TenantDefs()) != len(Defs())-1 {
		t.Fatalf("%d of %d", len(TenantDefs()), len(Defs()))
	}
}

func TestHolderServesPerTenantValues(t *testing.T) {
	g := base()
	h := NewHolder(g)
	if h.For("acme") != g {
		t.Fatal("a tenant without overrides gets the global values")
	}
	own, _ := Resolve(g, map[string]string{"queue_max_depth": "5"})
	h.SetAll(g, map[string]Values{"acme": own})
	if h.For("acme").QueueMaxDepth != 5 || h.For("bioco") != g || h.Current() != g {
		t.Fatal("a tenant's own value wins; others and the global values are untouched")
	}
	h.Set(g) // Set replaces everything
	if h.For("acme") != g {
		t.Fatal("Set clears tenant overrides")
	}
}

func TestSyncerAppliesTenantOverridesAndTheyBeatChangedGlobals(t *testing.T) {
	src := &fakeSource{tenants: []store.TenantInfo{
		tenant("acme", `{"settings":{"queue_max_depth":"10"}}`),
		tenant("bioco", `{}`),
		tenant("broken", `{"settings":{"queue_max_depth":"banana","max_attempts":"2"}}`),
	}}
	h := NewHolder(base())
	s := &Syncer{Source: src, Holder: h, Base: base()}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.For("acme").QueueMaxDepth != 10 || h.For("bioco") != base() {
		t.Fatalf("acme overrides, bioco inherits: %+v", h.For("acme"))
	}
	if b := h.For("broken"); b.QueueMaxDepth != 5000 || b.MaxAttempts != 2 {
		t.Fatalf("a bad override is skipped, a good one in the same tenant still applies: %+v", b)
	}
	// The global value changes: tenants that override it keep theirs, the others follow.
	src.rows = []store.SettingRow{{Key: "queue_max_depth", Value: "999"}, {Key: "max_attempts", Value: "8"}}
	_ = s.Refresh(context.Background())
	if h.For("acme").QueueMaxDepth != 10 || h.For("acme").MaxAttempts != 8 || h.For("bioco").QueueMaxDepth != 999 {
		t.Fatalf("per tenant beats global, the rest follows global: %+v %+v", h.For("acme"), h.For("bioco"))
	}
	// A tenant's override is removed: it follows the global value again.
	src.tenants[0] = tenant("acme", `{}`)
	_ = s.Refresh(context.Background())
	if h.For("acme").QueueMaxDepth != 999 {
		t.Fatalf("%+v", h.For("acme"))
	}
	src.err = errors.New("db down")
	if err := s.Refresh(context.Background()); err == nil || h.For("bioco").QueueMaxDepth != 999 {
		t.Fatal("an error leaves everything as it was")
	}
}
