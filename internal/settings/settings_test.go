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
	return Values{RateLimitPerSec: 100, RateLimitBurst: 200, QueueMaxDepth: 5000, WorkersMin: 2, WorkersMax: 8, JobsPerWorker: 50, LagSLOSeconds: 10, MaxAttempts: 5}
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
	if v.Get("rate_limit_per_sec") != "100" || v.Get("queue_max_depth") != "5000" || v.Get("nope") != "" {
		t.Fatal("getter values")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		key, val string
		wantErr  string
	}{
		{"rate_limit_per_sec", "250.5", ""},
		{"rate_limit_burst", "1", ""},
		{"workers_min", "0", ""},
		{"workers_max", "16", ""},
		{"rate_limit_per_sec", "0", "between"},
		{"rate_limit_per_sec", "fast", "number"},
		{"rate_limit_burst", "2.5", "whole"},
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
		{Key: "rate_limit_per_sec", Value: "250"},
		{Key: "queue_max_depth", Value: "garbage"},
		{Key: "unknown", Value: "1"},
		{Key: "workers_min", Value: "99"},
		{Key: "workers_max", Value: "12"},
	}
	v, errs := Apply(base(), rows)
	if v.RateLimitPerSec != 250 || v.QueueMaxDepth != 5000 || v.WorkersMax != 12 || v.WorkersMin != 2 {
		t.Fatalf("values %+v", v)
	}
	if len(errs) != 3 {
		t.Fatalf("errors %v", errs)
	}
}

type fakeSource struct {
	rows []store.SettingRow
	err  error
}

func (f *fakeSource) ListSettings(context.Context) ([]store.SettingRow, error) { return f.rows, f.err }

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
