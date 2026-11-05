// Package seed creates the demo tenants, their provider-account mappings and one API key each (M2).
package seed

import (
	"context"
	"fmt"
	"io"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

type tenantSpec struct {
	id, name string
	config   map[string]any
}

// Tenants in the demo. Only reviewer's config is used before M8a; the others record the designed worker allocation.
var tenants = []tenantSpec{
	{"common-pool", "Common pool (ops)", nil},
	{"reviewer", "Reviewer", map[string]any{"ingest_mode": "sync"}},
	{"acme-pharma", "Acme Pharma", map[string]any{"ingest_mode": "async", "workers": map[string]any{"mode": "shared", "weight": 1}}},
	{"bioco", "BioCo", map[string]any{"ingest_mode": "async", "workers": map[string]any{"mode": "dedicated", "min": 2, "weight": 3}}},
}

// Tive AccountId -> tenant. 999 (sample 9) is deliberately unmapped so it lands in common-pool.
var accounts = map[int64]string{478: "reviewer", 1001: "acme-pharma", 1002: "bioco"}

// Run is idempotent: tenants and mappings are upserted, and a tenant that already has a seed key gets no new one.
// Plaintext keys are written to w only when created.
func Run(ctx context.Context, st *store.PGStore, w io.Writer) error {
	for _, t := range tenants {
		if err := st.CreateTenant(ctx, t.id, t.name, t.config); err != nil {
			return err
		}
	}
	for acct, tenant := range accounts {
		if err := st.MapSource(ctx, tenant, "Tive", acct); err != nil {
			return err
		}
	}

	fmt.Fprintf(w, "%-14s %s\n", "TENANT", "API KEY")
	for _, t := range tenants {
		name := "seed-" + t.id
		keys, err := st.ListAPIKeys(ctx, t.id)
		if err != nil {
			return err
		}
		exists := false
		for _, k := range keys {
			exists = exists || (k.Name == name && k.RevokedAt == nil)
		}
		if exists {
			fmt.Fprintf(w, "%-14s (already seeded; plaintext is shown only at creation)\n", t.id)
			continue
		}
		key, err := auth.NewKey()
		if err != nil {
			return err
		}
		if err := st.CreateAPIKey(ctx, t.id, name, key.Prefix, key.Hash, nil); err != nil {
			return err
		}
		fmt.Fprintf(w, "%-14s %s\n", t.id, key.Plaintext)
	}
	return nil
}
