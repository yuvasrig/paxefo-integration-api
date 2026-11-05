package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/seed"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

const adminUsage = `usage:
  api serve                                            run the server (default)
  api seed                                             create demo tenants, account mappings and one key each
  api keys create --tenant T [--name N] [--expires YYYY-MM-DD]
  api keys list [--tenant T]
  api keys revoke ID
  api tenants create --id ID [--name NAME]
  api tenants map-source --tenant T --account N [--provider Tive]
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
	case "map-source":
		fs := newFlags("tenants map-source", out)
		tenant := fs.String("tenant", "", "tenant id (required)")
		account := fs.Int64("account", 0, "provider account id, for Tive the AccountId (required)")
		provider := fs.String("provider", "Tive", "provider name")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *tenant == "" || *account == 0 {
			return errors.New("tenants map-source: --tenant and --account are required")
		}
		if ok, err := st.TenantExists(ctx, *tenant); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("tenants map-source: tenant %q does not exist", *tenant)
		}
		if err := st.MapSource(ctx, *tenant, *provider, *account); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s account %d now maps to tenant %s\n", *provider, *account, *tenant)
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
