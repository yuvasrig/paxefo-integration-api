package seed_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/seed"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

func TestSeed(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var first bytes.Buffer
	if err := seed.Run(ctx, st, &first); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"reviewer", "acme-pharma", "bioco"} {
		if ok, _ := st.TenantExists(ctx, tenant); !ok {
			t.Fatalf("tenant %s missing", tenant)
		}
	}
	if ok, _ := st.TenantExists(ctx, "common-pool"); ok {
		t.Fatal("there is no common-pool: the tenant comes from the API key only")
	}

	// One key per tenant, printed once and valid.
	var plaintexts []string
	for _, line := range strings.Split(first.String(), "\n") {
		for _, f := range strings.Fields(line) {
			if auth.ValidFormat(f) {
				plaintexts = append(plaintexts, f)
			}
		}
	}
	if len(plaintexts) != 3 {
		t.Fatalf("expected 3 keys printed, got %d:\n%s", len(plaintexts), first.String())
	}
	for _, p := range plaintexts {
		if _, err := st.LookupAPIKey(ctx, auth.HashKey(p)); err != nil {
			t.Fatalf("printed key is not stored: %v", err)
		}
	}

	// Re-running creates no new keys and never reprints a secret.
	var second bytes.Buffer
	if err := seed.Run(ctx, st, &second); err != nil {
		t.Fatal(err)
	}
	for _, p := range plaintexts {
		if strings.Contains(second.String(), p) {
			t.Fatal("a rerun must not print existing keys")
		}
	}
	keys, _ := st.ListAPIKeys(ctx, "")
	if len(keys) != 3 {
		t.Fatalf("keys after rerun = %d, want 3", len(keys))
	}
}
