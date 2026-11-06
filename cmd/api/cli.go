package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
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
