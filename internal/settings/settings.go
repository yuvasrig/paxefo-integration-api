// Package settings holds the service's runtime-tunable settings: the few values an operator can change from the
// admin console (or `api settings set`) while the service runs. Environment variables give the defaults; a value
// stored in the database overrides its default, and every running instance picks the change up within the refresh
// interval, with no restart. Anything not listed here (ports, the database URL, the log level) needs a restart.
package settings

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// Values is the set of runtime settings in force.
type Values struct {
	RateLimitPerSec    float64 // requests per second per tenant
	RateLimitBurst     int     // burst size per tenant
	QueueMaxDepth      int     // jobs a tenant may have waiting before ingest answers 503
	WorkersMin         int     // fewest workers an instance keeps running
	WorkersMax         int     // most workers an instance runs
	WorkersTotalMax    int     // most workers one process runs across all tenants' pools (0 = no cap)
	JobsPerWorker      int     // backlog one worker is expected to keep up with (sizes the pool)
	LagSLOSeconds      int     // a job waiting longer than this is "behind"
	MaxAttempts        int     // tries before a job is given up as dead
	AlertRejectPercent int     // alert when this share of a tenant's recent events is rejected (0 = off)
}

// Def describes one setting for the console and for validation.
type Def struct {
	Key   string
	Title string
	Help  string
	Float bool // false: whole number
	Min   float64
	Max   float64
}

var defs = []Def{
	{"rate_limit_per_sec", "Rate limit (requests/s per tenant)", "Requests per second each tenant may send before it gets 429.", true, 0.1, 100000},
	{"rate_limit_burst", "Rate limit burst", "How many requests a tenant may send at once before the per-second limit applies.", false, 1, 1000000},
	{"queue_max_depth", "Queue limit per tenant", "Waiting jobs a tenant may have before ingest answers 503 with Retry-After (backpressure).", false, 1, 100000000},
	{"workers_min", "Workers per tenant: minimum", "Fewest workers each async tenant's pool keeps running per instance. A tenant can set its own on the Tenants page.", false, 0, 1000},
	{"workers_max", "Workers per tenant: maximum", "Most workers each tenant's pool runs per instance. Keep tenants x workers x instances within the database connection budget.", false, 1, 1000},
	{"workers_total_max", "Workers: hard cap per process", "Most workers one process runs across ALL tenants' pools, whatever the tenants' own limits add up to. 0 means no cap. Protects the database connection budget.", false, 0, 10000},
	{"jobs_per_worker", "Jobs per worker", "Backlog one worker is expected to handle; a tenant's pool grows when its backlog exceeds workers x this.", false, 1, 1000000},
	{"lag_slo_seconds", "Lag target (seconds)", "A job waiting longer than this counts as behind, and the pool grows by one.", false, 1, 86400},
	{"max_attempts", "Attempts before a job is dead", "How many times a failing job is tried before its outcome becomes 'dead' (replayable).", false, 1, 100},
	{"alert_reject_percent", "Alert: reject rate (%)", "Raise an alert when at least this percentage of a tenant's events in the last 15 minutes were rejected (needs 10 or more events). 0 turns the rule off.", false, 0, 100},
}

// Defs lists the runtime settings in display order.
func Defs() []Def { return append([]Def(nil), defs...) }

// Lookup returns the definition for a key.
func Lookup(key string) (Def, bool) {
	for _, d := range defs {
		if d.Key == key {
			return d, true
		}
	}
	return Def{}, false
}

// Get returns a setting's current value as text.
func (v Values) Get(key string) string {
	switch key {
	case "rate_limit_per_sec":
		return strconv.FormatFloat(v.RateLimitPerSec, 'f', -1, 64)
	case "rate_limit_burst":
		return strconv.Itoa(v.RateLimitBurst)
	case "queue_max_depth":
		return strconv.Itoa(v.QueueMaxDepth)
	case "workers_min":
		return strconv.Itoa(v.WorkersMin)
	case "workers_max":
		return strconv.Itoa(v.WorkersMax)
	case "workers_total_max":
		return strconv.Itoa(v.WorkersTotalMax)
	case "jobs_per_worker":
		return strconv.Itoa(v.JobsPerWorker)
	case "lag_slo_seconds":
		return strconv.Itoa(v.LagSLOSeconds)
	case "max_attempts":
		return strconv.Itoa(v.MaxAttempts)
	case "alert_reject_percent":
		return strconv.Itoa(v.AlertRejectPercent)
	}
	return ""
}

// parse validates raw for the key and returns the number.
func parse(key, raw string) (float64, error) {
	d, ok := Lookup(key)
	if !ok {
		return 0, fmt.Errorf("unknown setting %q", key)
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number (got %q)", key, raw)
	}
	if !d.Float && f != float64(int64(f)) {
		return 0, fmt.Errorf("%s must be a whole number (got %q)", key, raw)
	}
	if f < d.Min || f > d.Max {
		return 0, fmt.Errorf("%s must be between %v and %v (got %v)", key, d.Min, d.Max, f)
	}
	return f, nil
}

// Validate checks a value for a setting, including the rule that the minimum workers cannot exceed the maximum
// given the other values in force.
func Validate(current Values, key, raw string) error {
	if _, err := parse(key, raw); err != nil {
		return err
	}
	next, err := current.With(key, raw)
	if err != nil {
		return err
	}
	if next.WorkersMin > next.WorkersMax {
		return fmt.Errorf("workers_min (%d) cannot be more than workers_max (%d)", next.WorkersMin, next.WorkersMax)
	}
	return nil
}

// With returns v with one setting changed.
func (v Values) With(key, raw string) (Values, error) {
	f, err := parse(key, raw)
	if err != nil {
		return v, err
	}
	switch key {
	case "rate_limit_per_sec":
		v.RateLimitPerSec = f
	case "rate_limit_burst":
		v.RateLimitBurst = int(f)
	case "queue_max_depth":
		v.QueueMaxDepth = int(f)
	case "workers_min":
		v.WorkersMin = int(f)
	case "workers_max":
		v.WorkersMax = int(f)
	case "workers_total_max":
		v.WorkersTotalMax = int(f)
	case "jobs_per_worker":
		v.JobsPerWorker = int(f)
	case "lag_slo_seconds":
		v.LagSLOSeconds = int(f)
	case "max_attempts":
		v.MaxAttempts = int(f)
	case "alert_reject_percent":
		v.AlertRejectPercent = int(f)
	}
	return v, nil
}

// Apply layers stored overrides on the defaults. An invalid override is skipped and returned as an error, never
// applied: a bad row must not change how the service behaves.
func Apply(base Values, overrides []store.SettingRow) (Values, []error) {
	v := base
	var errs []error
	for _, o := range overrides {
		next, err := v.With(o.Key, o.Value)
		if err == nil && next.WorkersMin > next.WorkersMax {
			err = fmt.Errorf("workers_min cannot be more than workers_max")
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("setting %s=%q ignored: %w", o.Key, o.Value, err))
			continue
		}
		v = next
	}
	return v, errs
}

// Holder is the settings in force; readers call Current() on every use so a change takes effect at once.
type Holder struct{ v atomic.Pointer[Values] }

// NewHolder starts with v.
func NewHolder(v Values) *Holder { h := &Holder{}; h.Set(v); return h }

// Set replaces the settings in force.
func (h *Holder) Set(v Values) { h.v.Store(&v) }

// Current returns the settings in force.
func (h *Holder) Current() Values { return *h.v.Load() }

// Source lists the stored overrides (implemented by *store.PGStore).
type Source interface {
	ListSettings(ctx context.Context) ([]store.SettingRow, error)
}

// Syncer keeps a Holder current with the stored overrides.
type Syncer struct {
	Source   Source
	Holder   *Holder
	Base     Values // the environment defaults
	Interval time.Duration
	Log      *slog.Logger
}

// Refresh reads the overrides once. A database error leaves the settings as they are.
func (s *Syncer) Refresh(ctx context.Context) error {
	rows, err := s.Source.ListSettings(ctx)
	if err != nil {
		return err
	}
	v, errs := Apply(s.Base, rows)
	for _, e := range errs {
		if s.Log != nil {
			s.Log.Warn("runtime setting ignored", "error", e)
		}
	}
	if v != s.Holder.Current() {
		s.Holder.Set(v)
		if s.Log != nil {
			s.Log.Info("runtime settings changed")
		}
	}
	return nil
}

// Run refreshes every Interval until ctx ends (0 disables it).
func (s *Syncer) Run(ctx context.Context) {
	if s.Interval <= 0 {
		return
	}
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Refresh(ctx); err != nil && ctx.Err() == nil && s.Log != nil {
				s.Log.Error("could not read runtime settings; keeping the current ones", "error", err)
			}
		}
	}
}
