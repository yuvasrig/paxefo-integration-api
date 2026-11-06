package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// Replay reprocesses stored events, an operator action for "fix the rules or the conflict policy, then apply it to
// what was already received". It re-runs each event's stored raw payload through the current ruleset and the
// tenant's current conflict policy, for a time window and optionally one device, one account and some statuses.
//
// How it stays safe:
//   - The raw payload is never touched. Only derived data is recomputed.
//   - Dry run is the default: the whole run happens inside a transaction that is rolled back, so the report is exact
//     (each event sees the effect of the ones before it) and nothing changes.
//   - Applying archives each event's previous outcome (event_outcome_history) and records the run (replay_runs).
//   - Events are processed oldest received first, so under a "keep_last" conflict policy the last received wins; and
//     a replay never lets an older event overwrite a reading a later event produced.
//   - A failure for one event leaves that event's outcome as it was and the run carries on.

const (
	replayPage       = 200
	maxReportChanges = 50
	maxReportErrors  = 20
	// dryRunTimeout bounds one tenant's dry run, which is a single long transaction.
	dryRunTimeout = 30 * time.Minute
)

// ReplayOptions describes one replay for one tenant.
type ReplayOptions struct {
	Filter store.ReplayFilter // Filter.TenantID is the tenant
	Apply  bool               // false (the default) is a dry run
	RunID  string             // identifies the run in the history and audit tables
	By     string             // who ran it
	Limit  int                // stop after this many events (0 = no limit)
}

// EventChange describes one event whose result differs after the replay.
type EventChange struct {
	EventID       string   `json:"event_id"`
	From          string   `json:"from"`
	To            string   `json:"to"`
	FlagsFrom     []string `json:"flags_from"`
	FlagsTo       []string `json:"flags_to"`
	OutputChanged bool     `json:"output_changed"`
}

// ReplayReport is what a replay did (or, for a dry run, would do) for one tenant.
type ReplayReport struct {
	Tenant      string         `json:"tenant"`
	Applied     bool           `json:"applied"`
	RunID       string         `json:"run_id"`
	Selected    int            `json:"selected"`    // events matching the filter
	Reprocessed int            `json:"reprocessed"` // computed without error
	Unchanged   int            `json:"unchanged"`
	Changed     int            `json:"changed"` // status, flags or PAXAFE output differ from before
	Queued      int            `json:"queued"`  // still waiting for a worker: left alone
	Errors      int            `json:"errors"`
	Transitions map[string]int `json:"transitions"` // "conflict -> accepted": 3
	Changes     []EventChange  `json:"changes"`     // the first few
	Failures    []string       `json:"failures"`    // the first few
}

// ReplayWindow reprocesses the events of one tenant that match opts.Filter.
func (s *Service) ReplayWindow(ctx context.Context, opts ReplayOptions) (ReplayReport, error) {
	tenant := opts.Filter.TenantID
	if tenant == "" {
		return ReplayReport{}, fmt.Errorf("ingest: replay needs a tenant")
	}
	if opts.RunID == "" {
		opts.RunID = store.NewID("run_")
	}
	rep := ReplayReport{Tenant: tenant, Applied: opts.Apply, RunID: opts.RunID, Transitions: map[string]int{}, Changes: []EventChange{}, Failures: []string{}}
	started := s.Now()

	work := func(commit func(context.Context, store.CommitInput) (store.CommitResult, error)) error {
		var cursor store.ReplayCursor
		for {
			events, next, done, err := s.Store.ListReplayEvents(ctx, opts.Filter, cursor, replayPage)
			if err != nil {
				return err
			}
			for _, ev := range events {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if opts.Limit > 0 && rep.Selected >= opts.Limit {
					return nil
				}
				rep.Selected++
				s.reprocess(ctx, ev, opts, commit, &rep)
			}
			if done {
				return nil
			}
			cursor = next
		}
	}

	var err error
	if opts.Apply {
		err = work(nil) // each event commits on its own, so one failure does not undo the rest
	} else {
		err = s.Store.InRollbackTx(ctx, tenant, dryRunTimeout, work)
	}
	if err != nil {
		return rep, fmt.Errorf("ingest: replay for tenant %s: %w", tenant, err)
	}

	if opts.Apply {
		params := map[string]any{
			"time_field": opts.Filter.TimeField, "from": opts.Filter.From, "to": opts.Filter.To,
			"device": opts.Filter.Device, "account": opts.Filter.Account, "statuses": opts.Filter.Statuses, "limit": opts.Limit,
		}
		run := store.ReplayRun{
			RunID: opts.RunID, TenantID: tenant, RequestedBy: opts.By, Params: params, StartedAt: started, FinishedAt: s.Now(),
			Selected: rep.Selected, Reprocessed: rep.Reprocessed, Changed: rep.Changed, Errors: rep.Errors, Transitions: rep.Transitions,
		}
		if err := s.Store.RecordReplayRun(ctx, run); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// ReplayAllTenants runs ReplayWindow for every tenant, each isolated from the others. The filter's TenantID is ignored.
func (s *Service) ReplayAllTenants(ctx context.Context, opts ReplayOptions) ([]ReplayReport, error) {
	tenants, err := s.Store.ListTenants(ctx)
	if err != nil {
		return nil, err
	}
	if opts.RunID == "" {
		opts.RunID = store.NewID("run_")
	}
	var reports []ReplayReport
	for _, t := range tenants {
		o := opts
		o.Filter.TenantID = t
		rep, err := s.ReplayWindow(ctx, o)
		reports = append(reports, rep)
		if err != nil {
			return reports, err
		}
	}
	return reports, nil
}

func (s *Service) reprocess(ctx context.Context, ev store.ReplayEvent, opts ReplayOptions,
	commit func(context.Context, store.CommitInput) (store.CommitResult, error), rep *ReplayReport) {

	// An event with no outcome that still has a queued job belongs to its worker; replaying it too would race.
	if ev.Status == "pending" {
		if queued, err := s.Store.HasJob(ctx, ev.ID); err == nil && queued {
			rep.Queued++
			return
		}
	}
	runID := opts.RunID
	if !opts.Apply {
		runID = "dry-run"
	}
	resp := s.process(ctx, run{
		eventID: ev.ID, requestID: "replay-" + opts.RunID, tenant: opts.Filter.TenantID, body: ev.Raw,
		replayRunID: runID, commit: commit, dryRun: !opts.Apply, quiet: true,
	})
	if resp.Status == store.StatusFailed {
		rep.Errors++
		if len(rep.Failures) < maxReportErrors {
			rep.Failures = append(rep.Failures, fmt.Sprintf("%s: %v", ev.ID, resp.cause))
		}
		return
	}
	rep.Reprocessed++

	newOut, _ := json.Marshal(map[string]any{"sensor": resp.Sensor, "location": resp.Location})
	outputChanged := !sameJSON(ev.Outputs, newOut)
	oldCodes, newCodes := flagCodes(ev), codesOf(resp)
	if ev.Status == resp.Status && !outputChanged && reflect.DeepEqual(oldCodes, newCodes) {
		rep.Unchanged++
		return
	}
	rep.Changed++
	if ev.Status != resp.Status {
		rep.Transitions[ev.Status+" -> "+resp.Status]++
	}
	if len(rep.Changes) < maxReportChanges {
		rep.Changes = append(rep.Changes, EventChange{
			EventID: ev.ID, From: ev.Status, To: resp.Status, FlagsFrom: oldCodes, FlagsTo: newCodes, OutputChanged: outputChanged,
		})
	}
}

// flagSig is a flag's code, plus its action when that is what distinguishes it: a conflict whose resolution went from
// kept to replaced is a change even though the code is the same.
func flagSig(code, action string) string {
	if code == rules.CodeDuplicateConflict && action != "" {
		return code + "(" + action + ")"
	}
	return code
}

func flagCodes(ev store.ReplayEvent) []string {
	out := make([]string, 0, len(ev.Flags))
	for _, f := range ev.Flags {
		out = append(out, flagSig(f.Code, f.Action))
	}
	sort.Strings(out)
	return out
}

func codesOf(r Response) []string {
	out := make([]string, 0, len(r.Flags))
	for _, f := range r.Flags {
		out = append(out, flagSig(f.Code, f.Action))
	}
	sort.Strings(out)
	return out
}

// sameJSON compares two JSON documents by value (key order and number formatting do not matter).
func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return strings.TrimSpace(string(a)) == strings.TrimSpace(string(b))
	}
	return reflect.DeepEqual(x, y)
}
