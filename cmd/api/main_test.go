package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

func testConfig(t *testing.T, dsn string, extra map[string]string) config.Server {
	t.Helper()
	env := map[string]string{"DATABASE_URL": dsn, "RATE_LIMIT_PER_SEC": "1000", "RATE_LIMIT_BURST": "1000"}
	for k, v := range extra {
		env[k] = v
	}
	cfg, err := config.FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func openTestStore(t *testing.T) (*store.PGStore, string) {
	t.Helper()
	dsn := pgtest.NewDSN(t)
	st, err := store.Open(context.Background(), dsn, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st, dsn
}

func run(t *testing.T, st *store.PGStore, cmd string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runAdmin(context.Background(), st, cmd, args, &out)
	return out.String(), err
}

func TestCLIKeysAndTenants(t *testing.T) {
	st, _ := openTestStore(t)

	if out, err := run(t, st, "tenants", "create", "--id", "reviewer", "--name", "Reviewer"); err != nil || !strings.Contains(out, "reviewer") {
		t.Fatalf("tenants create: %q %v", out, err)
	}
	if _, err := run(t, st, "tenants", "create"); err == nil {
		t.Fatal("tenants create needs --id")
	}
	if out, err := run(t, st, "tenants", "map-source", "--tenant", "reviewer", "--account", "478"); err != nil || !strings.Contains(out, "478") {
		t.Fatalf("map-source: %q %v", out, err)
	}
	if tn, ok, _ := st.TenantForAccount(context.Background(), "Tive", 478); !ok || tn != "reviewer" {
		t.Fatalf("mapping not stored: %q %v", tn, ok)
	}
	if _, err := run(t, st, "tenants", "map-source", "--tenant", "ghost", "--account", "1"); err == nil {
		t.Fatal("mapping to a missing tenant must fail with a clear error")
	}

	// keys create prints the plaintext once, and the reviewer key expires at the end of the named day.
	out, err := run(t, st, "keys", "create", "--tenant", "reviewer", "--name", "paxafe-reviewer", "--expires", "2026-12-15")
	if err != nil {
		t.Fatal(err)
	}
	var plaintext string
	for _, f := range strings.Fields(out) {
		if auth.ValidFormat(f) {
			plaintext = f
		}
	}
	if plaintext == "" || !strings.Contains(out, "2026-12-15T23:59:59Z") {
		t.Fatalf("output: %s", out)
	}
	k, err := st.LookupAPIKey(context.Background(), auth.HashKey(plaintext))
	if err != nil || k.TenantID != "reviewer" || k.Name != "paxafe-reviewer" || k.ExpiresAt == nil || k.ExpiresAt.UTC().Format(time.RFC3339) != "2026-12-15T23:59:59Z" {
		t.Fatalf("stored key = %+v, %v", k, err)
	}

	list, err := run(t, st, "keys", "list", "--tenant", "reviewer")
	if err != nil || !strings.Contains(list, "paxafe-reviewer") || strings.Contains(list, plaintext) {
		t.Fatalf("list must show the key without its secret:\n%s %v", list, err)
	}

	if _, err := run(t, st, "keys", "create", "--tenant", "ghost"); err == nil {
		t.Fatal("create for a missing tenant must fail")
	}
	if _, err := run(t, st, "keys", "create"); err == nil {
		t.Fatal("create needs --tenant")
	}
	if _, err := run(t, st, "keys", "create", "--tenant", "reviewer", "--expires", "tomorrow"); err == nil {
		t.Fatal("bad expiry must fail")
	}

	if _, err := run(t, st, "keys", "revoke", "abc"); err == nil {
		t.Fatal("bad id must fail")
	}
	if _, err := run(t, st, "keys", "revoke", "9999"); err == nil {
		t.Fatal("unknown id must fail")
	}
	if out, err := run(t, st, "keys", "revoke", "1"); err != nil || !strings.Contains(out, "revoked") {
		t.Fatalf("revoke: %q %v", out, err)
	}
	if k, _ := st.LookupAPIKey(context.Background(), auth.HashKey(plaintext)); k.RevokedAt == nil {
		t.Fatal("key should be revoked")
	}
	if _, err := run(t, st, "bogus"); err == nil {
		t.Fatal("unknown command must fail")
	}
}

// F13-R1, R3, R4, R5, R7: the server migrates, serves probes and the webhook, and shuts down cleanly.
func TestServeLifecycle(t *testing.T) {
	st, dsn := openTestStore(t)
	if _, err := run(t, st, "seed"); err != nil {
		t.Fatal(err)
	}
	k, _ := auth.NewKey()
	if err := st.CreateAPIKey(context.Background(), "reviewer", "t", k.Prefix, k.Hash, nil); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, ln, testConfig(t, dsn, nil), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()

	base := "http://" + ln.Addr().String()
	waitFor(t, base+"/healthz")
	for path, want := range map[string]string{"/readyz": `"ingest_mode":"sync"`, "/metrics": "go_goroutines", "/docs": "<redoc"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(b), want) {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
		}
	}

	sample, err := os.ReadFile("../../testdata/samples/02.json")
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", base+"/api/v1/webhook/tive", bytes.NewReader(sample))
	req.Header.Set("X-API-Key", k.Plaintext)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"status":"accepted"`) {
		t.Fatalf("webhook: %d %s", resp.StatusCode, b)
	}

	cancel() // what SIGTERM does
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error: %v", err)
		}
	case <-time.After(shutdownTimeout + 2*time.Second):
		t.Fatal("server did not stop")
	}
	if _, err := http.Get(base + "/healthz"); err == nil {
		t.Fatal("server should be stopped")
	}
}

func waitFor(t *testing.T, url string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s never came up", url)
}

// F13-R2: an invalid ruleset stops the process before it serves.
func TestServeRejectsInvalidRules_F13R2(t *testing.T) {
	_, dsn := openTestStore(t)
	path := t.TempDir() + "/rules.yaml"
	bad := "version: x\nrules:\n  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: fix, strategy: scale, params: { factor: 2 }, severity: warn }\n"
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	err := serve(context.Background(), ln, testConfig(t, dsn, map[string]string{"RULES_PATH": path}), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("err = %v, want a protected-field error", err)
	}

	err = serve(context.Background(), ln, testConfig(t, dsn, map[string]string{"RULES_PATH": "/nonexistent.yaml"}), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("missing rules file must fail")
	}
}

func TestRealMainUsageAndConfigErrors(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := realMain([]string{"help"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "keys create") {
		t.Fatalf("help: %d %q", code, out.String())
	}
	t.Setenv("DATABASE_URL", "")
	errOut.Reset()
	if code := realMain([]string{"seed"}, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "DATABASE_URL") {
		t.Fatalf("no database: %d %q", code, errOut.String())
	}
}
