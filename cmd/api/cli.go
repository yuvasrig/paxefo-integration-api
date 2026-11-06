package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/rulesync"
	"github.com/yuvasrig/paxefo-integration-api/internal/seed"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

const adminUsage = `usage:
  api serve                                            run the server (default)
  api seed                                             create demo tenants and one key each
  api keys create --tenant T [--name N] [--expires YYYY-MM-DD]
  api keys list [--tenant T]
  api keys revoke ID
  api tenants create --id ID [--name NAME]
  api rules default                                    print the default ruleset (a starting point to edit)
  api rules validate --file F                          check a ruleset file without publishing it
  api rules set --file F [--by NAME]                   publish a ruleset; running instances adopt it within the
                                                       refresh interval (RULES_REFRESH_INTERVAL, default 10s), no restart
  api rules show                                       the published ruleset (or the default in use)
  api rules history [--limit N]                        rulesets published so far
  api rules reset                                      stop using the published ruleset; back to the default
  api replay (--tenant T | --all-tenants) --from TIME --to TIME [--time received|event]
             [--device IMEI_OR_NAME] [--account N] [--status s1,s2] [--limit N] [--apply] [--by NAME]
                                                       reprocess stored payloads with the rules and conflict policy in force now,
                                                       for a time window (TIME: 2026-11-05 or RFC 3339; --to is exclusive).
                                                       Without --apply it is a dry run: it reports what would change and writes nothing.
  api replay runs [--limit N]                          applied replays so far (audit)
  api tenants show --tenant T                          print the tenant's config
  api tenants set-conflict --tenant T --policy keep_first|keep_last|reject [--http-status 200|409]
                                                       what to do when the same device and time arrive with different content
`

// runAdmin runs the operator commands against the database (keys, tenants, seed).
func runAdmin(ctx context.Context, st *store.PGStore, cmd string, args []string, out io.Writer) error {
	switch cmd {
	case "seed":
		return seed.Run(ctx, st, out)
	case "keys":
		return runKeys(ctx, st, args, out)
	case "tenants":
		return runTenants(ctx, st, args, out)
	case "rules":
		return runRules(ctx, st, args, out)
	case "replay":
		return runReplay(ctx, st, args, out)
	}
	return fmt.Errorf("unknown command %q\n%s", cmd, adminUsage)
}

func newFlags(name string, out io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)
	return fs
}

func runKeys(ctx context.Context, st *store.PGStore, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("keys: missing subcommand\n%s", adminUsage)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		fs := newFlags("keys create", out)
		tenant := fs.String("tenant", "", "tenant id (required)")
		name := fs.String("name", "key", "label for the key")
		expires := fs.String("expires", "", "expiry date YYYY-MM-DD (end of day UTC) or RFC 3339; empty = never")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *tenant == "" {
			return errors.New("keys create: --tenant is required")
		}
		if ok, err := st.TenantExists(ctx, *tenant); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("keys create: tenant %q does not exist", *tenant)
		}
		exp, err := parseExpiry(*expires)
		if err != nil {
			return err
		}
		key, err := auth.NewKey()
		if err != nil {
			return err
		}
		if err := st.CreateAPIKey(ctx, *tenant, *name, key.Prefix, key.Hash, exp); err != nil {
			return err
		}
		fmt.Fprintf(out, "created key for tenant %s (expires: %s)\n%s\nThis is the only time the key is shown; store it now.\n", *tenant, expiryText(exp), key.Plaintext)
		return nil
	case "list":
		fs := newFlags("keys list", out)
		tenant := fs.String("tenant", "", "only this tenant")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		keys, err := st.ListAPIKeys(ctx, *tenant)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tPREFIX\tTENANT\tNAME\tCREATED\tEXPIRES\tREVOKED")
		for _, k := range keys {
			revoked := "-"
			if k.RevokedAt != nil {
				revoked = k.RevokedAt.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(tw, "%d\t%s…\t%s\t%s\t%s\t%s\t%s\n", k.ID, k.Prefix, k.TenantID, k.Name, k.CreatedAt.UTC().Format(time.RFC3339), expiryText(k.ExpiresAt), revoked)
		}
		return tw.Flush()
	case "revoke":
		if len(rest) != 1 {
			return errors.New("keys revoke: expected one key id")
		}
		id, err := strconv.ParseInt(rest[0], 10, 64)
		if err != nil {
			return fmt.Errorf("keys revoke: bad id %q", rest[0])
		}
		if err := st.RevokeAPIKey(ctx, id); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("keys revoke: no key with id %d", id)
			}
			return err
		}
		fmt.Fprintf(out, "revoked key %d\n", id)
		return nil
	}
	return fmt.Errorf("keys: unknown subcommand %q\n%s", sub, adminUsage)
}

func runTenants(ctx context.Context, st *store.PGStore, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("tenants: missing subcommand\n%s", adminUsage)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		fs := newFlags("tenants create", out)
		id := fs.String("id", "", "tenant id (required)")
		name := fs.String("name", "", "display name (default: the id)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *id == "" {
			return errors.New("tenants create: --id is required")
		}
		if *name == "" {
			*name = *id
		}
		if err := st.CreateTenant(ctx, *id, *name, nil); err != nil {
			return err
		}
		fmt.Fprintf(out, "tenant %s ready\n", *id)
		return nil
	case "show":
		fs := newFlags("tenants show", out)
		tenant := fs.String("tenant", "", "tenant id (required)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *tenant == "" {
			return errors.New("tenants show: --tenant is required")
		}
		cfg, err := st.TenantConfig(ctx, *tenant)
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("tenants show: tenant %q does not exist", *tenant)
		}
		if err != nil {
			return err
		}
		cs, perr := store.ParseConflictSettings(cfg)
		if perr != nil {
			fmt.Fprintf(out, "warning: stored conflict config is invalid (%v); ingest uses the default\n", perr)
			cs = store.DefaultConflictSettings()
		}
		fmt.Fprintf(out, "config: %s\nconflict policy: %s (HTTP %d)\n", cfg, cs.Policy, cs.HTTPStatus)
		return nil
	case "set-conflict":
		fs := newFlags("tenants set-conflict", out)
		tenant := fs.String("tenant", "", "tenant id (required)")
		policy := fs.String("policy", "", "keep_first, keep_last or reject (required)")
		status := fs.Int("http-status", 0, "HTTP status of a conflict response, 200 or 409 (default: 409 for reject, else 200)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *tenant == "" || *policy == "" {
			return errors.New("tenants set-conflict: --tenant and --policy are required")
		}
		cs, err := store.NewConflictSettings(*policy, *status)
		if err != nil {
			return fmt.Errorf("tenants set-conflict: %w", err)
		}
		if err := st.SetConflictSettings(ctx, *tenant, cs); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("tenants set-conflict: tenant %q does not exist", *tenant)
			}
			return err
		}
		fmt.Fprintf(out, "tenant %s: conflict policy %s, HTTP %d\n", *tenant, cs.Policy, cs.HTTPStatus)
		return nil
	}
	return fmt.Errorf("tenants: unknown subcommand %q\n%s", sub, adminUsage)
}

func parseExpiry(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		t = t.Add(24*time.Hour - time.Second) // valid through the whole named day
		return &t, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("--expires must be YYYY-MM-DD or RFC 3339 (got %q)", s)
	}
	return &t, nil
}

func expiryText(t *time.Time) string {
	if t == nil {
		return "never"
	}
	return t.UTC().Format(time.RFC3339)
}

// runRulesOffline runs the rules commands that need no database: `rules default` and `rules validate`.
// handled is false when the command needs the database.
func runRulesOffline(args []string, out io.Writer) (handled bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "default":
		_, err := out.Write(rules.DefaultYAML())
		return true, err
	case "validate":
		fs := newFlags("rules validate", out)
		file := fs.String("file", "", "ruleset file (required)")
		if err := fs.Parse(args[1:]); err != nil {
			return true, err
		}
		rs, err := readRuleset(*file)
		if err != nil {
			return true, err
		}
		fmt.Fprintf(out, "ok: ruleset %q is valid (%d rules, %d overrides)\n", rs.Version, len(rs.Rules), len(rs.Overrides))
		return true, nil
	}
	return false, nil
}

func readRuleset(path string) (*rules.Ruleset, error) {
	if path == "" {
		return nil, errors.New("--file is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	rs, err := rules.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid ruleset: %w", path, err)
	}
	return rs, nil
}

// runRules manages the ruleset that running instances use. Rules are configurable on the fly: `set` publishes a
// validated ruleset and every running instance swaps it in within its refresh interval.
func runRules(ctx context.Context, st *store.PGStore, args []string, out io.Writer) error {
	if handled, err := runRulesOffline(args, out); handled {
		return err
	}
	if len(args) == 0 {
		return fmt.Errorf("rules: missing subcommand\n%s", adminUsage)
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "set":
		fs := newFlags("rules set", out)
		file := fs.String("file", "", "ruleset file (required)")
		by := fs.String("by", os.Getenv("USER"), "who is publishing it (recorded in the history)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		rs, err := readRuleset(*file)
		if err != nil {
			return fmt.Errorf("rules set: %w", err)
		}
		data, err := os.ReadFile(*file)
		if err != nil {
			return fmt.Errorf("rules set: %w", err)
		}
		row, err := st.PublishRuleset(ctx, rs.Version, string(data), *by)
		if errors.Is(err, store.ErrVersionExists) {
			return fmt.Errorf("rules set: version %q was already published; change `version:` in the file (it is recorded on every event)", rs.Version)
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "published ruleset %q (%d rules) and made it active.\nRunning instances adopt it within their refresh interval (default 10s); no restart needed.\n", row.Version, len(rs.Rules))
		return nil
	case "show":
		row, ok, err := st.ActiveRuleset(ctx)
		if err != nil {
			return err
		}
		if !ok {
			def, _ := rules.Default()
			fmt.Fprintf(out, "no ruleset is published: instances use RULES_PATH if set, else the embedded default (version %s).\n", def.Version)
			return nil
		}
		fmt.Fprintf(out, "# published ruleset %q by %q at %s\n%s", row.Version, row.CreatedBy, row.CreatedAt.UTC().Format(time.RFC3339), row.YAML)
		return nil
	case "history":
		fs := newFlags("rules history", out)
		limit := fs.Int("limit", 20, "how many to list")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		rows, err := st.RulesetHistory(ctx, *limit)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tVERSION\tBY\tPUBLISHED\tACTIVE")
		for _, r := range rows {
			active := ""
			if r.Active {
				active = "yes"
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", r.ID, r.Version, r.CreatedBy, r.CreatedAt.UTC().Format(time.RFC3339), active)
		}
		return tw.Flush()
	case "reset":
		cleared, err := st.ClearActiveRuleset(ctx)
		if err != nil {
			return err
		}
		if !cleared {
			fmt.Fprintln(out, "no ruleset was published; nothing to reset.")
			return nil
		}
		fmt.Fprintln(out, "the published ruleset is no longer active (history kept). Running instances go back to RULES_PATH or the embedded default within their refresh interval.")
		return nil
	}
	return fmt.Errorf("rules: unknown subcommand %q\n%s", sub, adminUsage)
}

var replayStatuses = map[string]bool{"accepted": true, "flagged": true, "duplicate": true, "conflict": true, "rejected": true, "failed": true, "pending": true}

// parseTime accepts 2026-11-05 (midnight UTC) or an RFC 3339 time.
func parseTime(name, v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, fmt.Errorf("--%s is required", name)
	}
	if t, err := time.Parse("2006-01-02", v); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("--%s must be YYYY-MM-DD or RFC 3339 (got %q)", name, v)
	}
	return t.UTC(), nil
}

// runReplay reprocesses stored payloads with the rules and conflict policy in force now. It is how an operator
// "fixes the rules or the conflict policy, then applies the fix to what was already received".
func runReplay(ctx context.Context, st *store.PGStore, args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "runs" {
		fs := newFlags("replay runs", out)
		limit := fs.Int("limit", 20, "how many to list")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		runs, err := st.ListReplayRuns(ctx, *limit)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "RUN\tTENANT\tBY\tFINISHED\tSELECTED\tCHANGED\tERRORS")
		for _, r := range runs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%d\n", r.RunID, r.TenantID, r.RequestedBy, r.FinishedAt.UTC().Format(time.RFC3339), r.Selected, r.Changed, r.Errors)
		}
		return tw.Flush()
	}

	fs := newFlags("replay", out)
	tenant := fs.String("tenant", "", "replay this tenant's events")
	all := fs.Bool("all-tenants", false, "replay every tenant (each isolated from the others)")
	from := fs.String("from", "", "start of the window, inclusive (required)")
	to := fs.String("to", "", "end of the window, exclusive (required)")
	timeField := fs.String("time", store.TimeReceived, "what the window measures: received (when we received it) or event (EntryTimeEpoch)")
	device := fs.String("device", "", "only this device: its IMEI (DeviceId) or DeviceName")
	account := fs.String("account", "", "only this AccountId")
	statuses := fs.String("status", "", "only events whose current status is one of these (comma separated): accepted, flagged, duplicate, conflict, rejected, failed, pending")
	limit := fs.Int("limit", 0, "stop after this many events per tenant (0 = no limit)")
	apply := fs.Bool("apply", false, "write the results; without it this is a dry run that changes nothing")
	by := fs.String("by", os.Getenv("USER"), "who is running it (recorded in the audit)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	switch {
	case (*tenant == "") == !*all:
		return errors.New("replay: give exactly one of --tenant or --all-tenants")
	case *timeField != store.TimeReceived && *timeField != store.TimeEvent:
		return fmt.Errorf("replay: --time must be %s or %s", store.TimeReceived, store.TimeEvent)
	}
	fromT, err := parseTime("from", *from)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	toT, err := parseTime("to", *to)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	if !toT.After(fromT) {
		return errors.New("replay: --to must be after --from")
	}
	var statusList []string
	for _, s := range strings.Split(*statuses, ",") {
		if s = strings.TrimSpace(s); s != "" {
			if !replayStatuses[s] {
				return fmt.Errorf("replay: unknown --status %q", s)
			}
			statusList = append(statusList, s)
		}
	}
	if *tenant != "" {
		if ok, err := st.TenantExists(ctx, *tenant); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("replay: tenant %q does not exist", *tenant)
		}
	}

	// The ruleset in force: the one published with `api rules set`, else RULES_PATH, else the embedded default.
	base, err := loadRules(os.Getenv("RULES_PATH"))
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	baseSource := rules.SourceDefault
	if os.Getenv("RULES_PATH") != "" {
		baseSource = rules.SourceFile
	}
	holder := rules.NewHolder(base, baseSource, time.Now())
	if err := (&rulesync.Reloader{Source: st, Holder: holder, Base: base, BaseSource: baseSource, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Refresh(ctx); err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	loaded := holder.Loaded()

	svc := &ingest.Service{Store: st, Provider: tive.NewDynamic(holder, config.Default()), Now: time.Now, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	opts := ingest.ReplayOptions{
		Filter: store.ReplayFilter{TimeField: *timeField, From: fromT, To: toT, Device: *device, Account: *account, Statuses: statusList},
		Apply:  *apply, By: *by, Limit: *limit, RunID: store.NewID("run_"),
	}

	mode := "DRY RUN: nothing was changed. Add --apply to write."
	if *apply {
		mode = "APPLIED (run " + opts.RunID + ")"
	}
	fmt.Fprintf(out, "replay on the %s time window [%s, %s) with ruleset %s (%s)\n%s\n", *timeField, fromT.Format(time.RFC3339), toT.Format(time.RFC3339), loaded.Ruleset.Version, loaded.Source, mode)

	var reports []ingest.ReplayReport
	if *all {
		reports, err = svc.ReplayAllTenants(ctx, opts)
	} else {
		opts.Filter.TenantID = *tenant
		var rep ingest.ReplayReport
		rep, err = svc.ReplayWindow(ctx, opts)
		reports = []ingest.ReplayReport{rep}
	}
	errorsTotal := 0
	for _, rep := range reports {
		printReplay(out, rep)
		errorsTotal += rep.Errors
	}
	if err != nil {
		return err
	}
	if errorsTotal > 0 {
		return fmt.Errorf("replay: %d events could not be reprocessed (their previous results are unchanged)", errorsTotal)
	}
	return nil
}

func printReplay(out io.Writer, r ingest.ReplayReport) {
	fmt.Fprintf(out, "\ntenant %s\n  selected %d · reprocessed %d · changed %d · unchanged %d · errors %d\n", r.Tenant, r.Selected, r.Reprocessed, r.Changed, r.Unchanged, r.Errors)
	keys := make([]string, 0, len(r.Transitions))
	for k := range r.Transitions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(out, "  %-28s %d\n", k, r.Transitions[k])
	}
	for i, c := range r.Changes {
		if i == 0 {
			fmt.Fprintf(out, "  changes (first %d):\n", len(r.Changes))
		}
		extra := ""
		if c.OutputChanged {
			extra = "  (PAXAFE output changed)"
		}
		fmt.Fprintf(out, "    %s  %s -> %s  flags [%s] -> [%s]%s\n", c.EventID, c.From, c.To, strings.Join(c.FlagsFrom, ","), strings.Join(c.FlagsTo, ","), extra)
	}
	for i, f := range r.Failures {
		if i == 0 {
			fmt.Fprintln(out, "  errors (previous results unchanged):")
		}
		fmt.Fprintf(out, "    %s\n", f)
	}
}
