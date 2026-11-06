// Package rulesync keeps the ruleset in force up to date while the service runs, so rules can be changed on the
// fly: an operator publishes a ruleset (`api rules set`), and every running instance validates it and swaps it in
// within the refresh interval, with no restart.
//
// Safety rules: a ruleset is validated before it replaces the current one, and one that fails to load (or a
// database that cannot be reached) leaves the working ruleset in place. When no ruleset is published, the service
// uses its base ruleset (RULES_PATH or the embedded default).
package rulesync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// Source provides the ruleset published at runtime (implemented by *store.PGStore).
type Source interface {
	ActiveRuleset(ctx context.Context) (row store.RulesetRow, ok bool, err error)
}

// Observer records what the reloader did (implemented by observability.Metrics). It may be nil.
type Observer interface {
	RulesReload(result string)          // "changed", "invalid" or "error"
	RulesActive(version, source string) // the ruleset now in force
}

// Reloader swaps rulesets into a Holder.
type Reloader struct {
	Source     Source
	Holder     *rules.Holder
	Base       *rules.Ruleset // used when no ruleset is published
	BaseSource string         // rules.SourceDefault or rules.SourceFile
	Interval   time.Duration  // how often Run checks; 0 disables Run
	Log        *slog.Logger
	Now        func() time.Time
	Observer   Observer
}

// Refresh checks for a published ruleset once. A non-nil error means the current ruleset was left in place
// (the database could not be read, or the published ruleset is invalid). At startup treat the error as fatal;
// while running, Run logs it and tries again.
func (r *Reloader) Refresh(ctx context.Context) error {
	row, published, err := r.Source.ActiveRuleset(ctx)
	if err != nil {
		r.result("error")
		return fmt.Errorf("rulesync: read published ruleset: %w", err)
	}
	cur := r.Holder.Loaded()

	if !published {
		if cur.Source == rules.SourceDatabase { // the published ruleset was reset: back to the base
			r.set(r.Base, r.BaseSource, "ruleset reset to the base ruleset")
			r.result("changed")
		}
		return nil
	}
	if cur.Source == rules.SourceDatabase && cur.Ruleset.Version == row.Version {
		return nil // already in force
	}
	rs, err := rules.Parse([]byte(row.YAML))
	if err != nil {
		r.result("invalid")
		return fmt.Errorf("rulesync: published ruleset %q is invalid and was not applied: %w", row.Version, err)
	}
	r.set(rs, rules.SourceDatabase, "ruleset changed")
	r.result("changed")
	return nil
}

func (r *Reloader) set(rs *rules.Ruleset, source, msg string) {
	r.Holder.Set(rs, source, r.now())
	r.log().Info(msg, "rules_version", rs.Version, "source", source)
	if r.Observer != nil {
		r.Observer.RulesActive(rs.Version, source)
	}
}

func (r *Reloader) result(res string) {
	if r.Observer != nil {
		r.Observer.RulesReload(res)
	}
}

// Run checks every Interval until ctx ends. Errors are logged and the working ruleset keeps serving.
func (r *Reloader) Run(ctx context.Context) {
	if r.Interval <= 0 {
		return
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Refresh(ctx); err != nil && ctx.Err() == nil {
				r.log().Error("ruleset not updated; the current ruleset stays in force", "rules_version", r.Holder.Current().Version, "error", err)
			}
		}
	}
}

func (r *Reloader) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Reloader) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}
