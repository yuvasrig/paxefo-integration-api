package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
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
	if _, err := run(t, st, "tenants", "map-source", "--tenant", "reviewer", "--account", "478"); err == nil {
		t.Fatal("there is no map-source: the tenant comes from the API key only")
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

// Fix the rules, then replay what was already received (the CLI path an operator uses).
func TestCLIReplay(t *testing.T) {
	st, _ := openTestStore(t)
	bg := context.Background()
	for _, id := range []string{"acme-pharma", "reviewer"} {
		if err := st.CreateTenant(bg, id, id, nil); err != nil {
			t.Fatal(err)
		}
	}
	rs, _ := rules.Default()
	var clock = time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	svc := &ingest.Service{Store: st, Provider: tive.New(rs, config.Default()), Now: func() time.Time { return clock }}
	send := func(tenant string, n int, at time.Time) {
		clock = at
		raw, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", n))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Ingest(bg, ingest.Request{TenantID: tenant, RequestID: "r", Body: raw}); err != nil {
			t.Fatal(err)
		}
	}
	send("reviewer", 2, clock)                      // humidity 55.3
	send("reviewer", 10, clock.Add(time.Hour))      // no humidity
	send("acme-pharma", 11, clock.Add(2*time.Hour)) // humidity 54.8

	// Validation.
	base := []string{"replay", "--from", "2026-11-05", "--to", "2026-11-06"}
	for name, args := range map[string][]string{
		"no tenant or all":    base,
		"both tenant and all": append([]string{"--tenant", "reviewer", "--all-tenants"}, base[1:]...),
		"no from":             {"--tenant", "reviewer", "--to", "2026-11-06"},
		"no to":               {"--tenant", "reviewer", "--from", "2026-11-05"},
		"to not after from":   {"--tenant", "reviewer", "--from", "2026-11-06", "--to", "2026-11-05"},
		"bad time":            {"--tenant", "reviewer", "--from", "yesterday", "--to", "2026-11-06"},
		"bad time field":      {"--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06", "--time", "never"},
		"bad status":          {"--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06", "--status", "weird"},
		"unknown tenant":      {"--tenant", "ghost", "--from", "2026-11-05", "--to", "2026-11-06"},
	} {
		if _, err := run(t, st, "replay", args...); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}

	// Publish a fix (a new rule), as an operator would.
	var out bytes.Buffer
	out.Write(rules.DefaultYAML())
	fixed := strings.Replace(strings.Replace(out.String(), "version: 2026-11-06.1", "version: fix-1", 1), "overrides: []", "", 1) +
		"  - { id: HUMIDITY-HIGH, code: HUMIDITY_HIGH, field: Humidity.Percentage, when: { outside: [0, 50] }, action: flag, severity: warn }\n"
	f := t.TempDir() + "/fix.yaml"
	if err := os.WriteFile(f, []byte(fixed), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, st, "rules", "set", "--file", f); err != nil {
		t.Fatal(err)
	}

	// Dry run is the default: it uses the published rules, reports the change, and writes nothing.
	got, err := run(t, st, "replay", "--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06")
	if err != nil || !strings.Contains(got, "DRY RUN") || !strings.Contains(got, "with ruleset fix-1 (database)") ||
		!strings.Contains(got, "selected 2 · reprocessed 2 · changed 1 · unchanged 1 · errors 0") || !strings.Contains(got, "accepted -> flagged") {
		t.Fatalf("dry run: %v\n%s", err, got)
	}
	if runs, _ := st.ListReplayRuns(bg, 10); len(runs) != 0 {
		t.Fatal("a dry run is not an applied run")
	}

	// Filters narrow it: this device has no events in the window.
	got, _ = run(t, st, "replay", "--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06", "--device", "NOPE")
	if !strings.Contains(got, "selected 0") {
		t.Fatalf("%s", got)
	}

	// --apply writes it and records the run.
	got, err = run(t, st, "replay", "--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06", "--apply", "--by", "alice")
	if err != nil || !strings.Contains(got, "APPLIED (run run_") || !strings.Contains(got, "changed 1") {
		t.Fatalf("apply: %v\n%s", err, got)
	}
	if got, _ = run(t, st, "replay", "runs"); !strings.Contains(got, "alice") || !strings.Contains(got, "reviewer") {
		t.Fatalf("runs: %s", got)
	}
	// Now nothing is left to change.
	if got, _ = run(t, st, "replay", "--tenant", "reviewer", "--from", "2026-11-05", "--to", "2026-11-06"); !strings.Contains(got, "changed 0 · unchanged 2") {
		t.Fatalf("after apply: %s", got)
	}

	// Every tenant, each isolated.
	got, err = run(t, st, "replay", "--all-tenants", "--from", "2026-11-05", "--to", "2026-11-06")
	if err != nil || !strings.Contains(got, "tenant acme-pharma") || !strings.Contains(got, "tenant reviewer") || !strings.Contains(got, "selected 1 · reprocessed 1 · changed 1") {
		t.Fatalf("all tenants: %v\n%s", err, got)
	}
	// Event time, and a status filter.
	got, _ = run(t, st, "replay", "--tenant", "reviewer", "--from", "2025-02-01", "--to", "2025-03-01", "--time", "event", "--status", "flagged")
	if !strings.Contains(got, "selected 1 ") {
		t.Fatalf("event-time window: %s", got)
	}
}

// Rules are configurable on the fly: publish, inspect, change and reset them from the CLI.
func TestCLIRules(t *testing.T) {
	st, _ := openTestStore(t)
	dir := t.TempDir()
	write := func(name, content string) string {
		p := dir + "/" + name
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// default: the starting point, which is itself a valid ruleset.
	out, err := run(t, st, "rules", "default")
	if err != nil || !strings.Contains(out, "DEFAULT RULESET") || !strings.Contains(out, "id: TS-FUTURE") {
		t.Fatalf("default: %q %v", out, err)
	}
	mine := write("mine.yaml", strings.Replace(strings.Replace(out, "version: 2026-11-06.1", "version: mine-1", 1), "overrides: []", "", 1)+
		"  - { id: HUMIDITY-HIGH, code: HUMIDITY_HIGH, field: Humidity.Percentage, when: { outside: [0, 50] }, action: flag, severity: warn }\n")

	// validate: no database, no publish.
	if out, err = run(t, st, "rules", "validate", "--file", mine); err != nil || !strings.Contains(out, `ruleset "mine-1" is valid (16 rules`) {
		t.Fatalf("validate: %q %v", out, err)
	}
	bad := write("bad.yaml", "version: bad\nrules:\n  - { id: A, code: X, field: Temperature.Celsius, when: { lt: 0 }, action: fix, strategy: scale, params: { factor: 2 }, severity: warn }\n")
	if _, err = run(t, st, "rules", "validate", "--file", bad); err == nil || !strings.Contains(err.Error(), "protected") {
		t.Fatalf("an unsafe ruleset must fail validation: %v", err)
	}
	if _, err = run(t, st, "rules", "validate"); err == nil {
		t.Fatal("--file is required")
	}
	if _, err = run(t, st, "rules", "validate", "--file", dir+"/missing.yaml"); err == nil {
		t.Fatal("a missing file must fail")
	}

	// show before anything is published.
	if out, _ = run(t, st, "rules", "show"); !strings.Contains(out, "no ruleset is published") {
		t.Fatalf("show: %q", out)
	}

	// set: validated, then active.
	if _, err = run(t, st, "rules", "set", "--file", bad); err == nil {
		t.Fatal("an invalid ruleset must not be published")
	}
	if _, ok, _ := st.ActiveRuleset(context.Background()); ok {
		t.Fatal("a failed set must publish nothing")
	}
	if out, err = run(t, st, "rules", "set", "--file", mine, "--by", "alice"); err != nil || !strings.Contains(out, `published ruleset "mine-1"`) || !strings.Contains(out, "no restart") {
		t.Fatalf("set: %q %v", out, err)
	}
	if row, ok, _ := st.ActiveRuleset(context.Background()); !ok || row.Version != "mine-1" || row.CreatedBy != "alice" {
		t.Fatalf("active %+v", row)
	}
	if _, err = run(t, st, "rules", "set", "--file", mine); err == nil || !strings.Contains(err.Error(), "already published") {
		t.Fatalf("the same version twice must ask for a bump: %v", err)
	}

	if out, _ = run(t, st, "rules", "show"); !strings.Contains(out, `published ruleset "mine-1" by "alice"`) || !strings.Contains(out, "HUMIDITY-HIGH") {
		t.Fatalf("show: %q", out)
	}
	mine2 := write("mine2.yaml", strings.Replace(mine2Source(t, mine), "version: mine-1", "version: mine-2", 1))
	if _, err = run(t, st, "rules", "set", "--file", mine2); err != nil {
		t.Fatal(err)
	}
	out, _ = run(t, st, "rules", "history")
	if !strings.Contains(out, "mine-2") || !strings.Contains(out, "mine-1") || strings.Count(out, "yes") != 1 {
		t.Fatalf("history: %q", out)
	}

	// reset.
	if out, err = run(t, st, "rules", "reset"); err != nil || !strings.Contains(out, "no longer active") {
		t.Fatalf("reset: %q %v", out, err)
	}
	if out, _ = run(t, st, "rules", "reset"); !strings.Contains(out, "nothing to reset") {
		t.Fatalf("second reset: %q", out)
	}
	if _, err = run(t, st, "rules", "bogus"); err == nil {
		t.Fatal("unknown subcommand")
	}
}

func mine2Source(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// `rules default` and `rules validate` work with no database and no configuration at all.
func TestRulesOfflineCommandsNeedNoDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	var out, errOut bytes.Buffer
	if code := realMain([]string{"rules", "default"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "DEFAULT RULESET") {
		t.Fatalf("default: %d %q", code, errOut.String())
	}
	f := t.TempDir() + "/r.yaml"
	_ = os.WriteFile(f, out.Bytes(), 0o600)
	out.Reset()
	if code := realMain([]string{"rules", "validate", "--file", f}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "is valid") {
		t.Fatalf("validate: %d %q", code, errOut.String())
	}
	errOut.Reset()
	if code := realMain([]string{"rules", "show"}, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "DATABASE_URL") {
		t.Fatalf("show needs the database: %d %q", code, errOut.String())
	}
}

func TestCLIConflictPolicy(t *testing.T) {
	st, _ := openTestStore(t)
	if _, err := run(t, st, "tenants", "create", "--id", "reviewer"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, st, "tenants", "show", "--tenant", "reviewer")
	if err != nil || !strings.Contains(out, "conflict policy: keep_first (HTTP 200)") {
		t.Fatalf("default: %q %v", out, err)
	}
	out, err = run(t, st, "tenants", "set-conflict", "--tenant", "reviewer", "--policy", "reject")
	if err != nil || !strings.Contains(out, "reject, HTTP 409") {
		t.Fatalf("reject: %q %v", out, err)
	}
	out, _ = run(t, st, "tenants", "show", "--tenant", "reviewer")
	if !strings.Contains(out, "conflict policy: reject (HTTP 409)") || !strings.Contains(out, `"policy"`) {
		t.Fatalf("show: %q", out)
	}
	if out, err = run(t, st, "tenants", "set-conflict", "--tenant", "reviewer", "--policy", "keep_last", "--http-status", "409"); err != nil || !strings.Contains(out, "keep_last, HTTP 409") {
		t.Fatalf("%q %v", out, err)
	}
	for name, args := range map[string][]string{
		"no tenant":      {"set-conflict", "--policy", "reject"},
		"no policy":      {"set-conflict", "--tenant", "reviewer"},
		"bad policy":     {"set-conflict", "--tenant", "reviewer", "--policy", "merge"},
		"bad status":     {"set-conflict", "--tenant", "reviewer", "--policy", "reject", "--http-status", "500"},
		"unknown tenant": {"set-conflict", "--tenant", "ghost", "--policy", "reject"},
		"show unknown":   {"show", "--tenant", "ghost"},
		"show no tenant": {"show"},
	} {
		if _, err := run(t, st, "tenants", args...); err == nil {
			t.Errorf("%s: expected an error", name)
		}
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
	req, _ := http.NewRequest("POST", base+"/api/webhook/tive", bytes.NewReader(sample))
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
	if resp, err := http.Get(base + "/healthz"); err == nil {
		_ = resp.Body.Close()
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

// Rules published while the server runs are adopted without a restart (served by the real wiring, not a test double).
func TestServeAdoptsPublishedRulesWithoutRestart(t *testing.T) {
	st, dsn := openTestStore(t)
	if _, err := run(t, st, "seed"); err != nil {
		t.Fatal(err)
	}
	k, _ := auth.NewKey()
	if err := st.CreateAPIKey(context.Background(), "reviewer", "t", k.Prefix, k.Hash, nil); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, ln, testConfig(t, dsn, map[string]string{"RULES_REFRESH_INTERVAL": "50ms"}), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	}()
	base := "http://" + ln.Addr().String()
	waitFor(t, base+"/healthz")

	rulesJSON := func() (version, source string) {
		req, _ := http.NewRequest("GET", base+"/api/v1/rules", nil)
		req.Header.Set("X-API-Key", k.Plaintext)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var v struct{ Version, Source string }
		_ = json.NewDecoder(resp.Body).Decode(&v)
		return v.Version, v.Source
	}
	if v, src := rulesJSON(); v != "2026-11-06.1" || src != "default" {
		t.Fatalf("at start: %s/%s", v, src)
	}

	if _, err := run(t, st, "rules", "set", "--file", writeRules(t, "live-1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, src := rulesJSON(); v == "live-1" && src == "database" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the running server never adopted the published ruleset")
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func writeRules(t *testing.T, version string) string {
	t.Helper()
	var out bytes.Buffer
	if err := func() error { _, err := out.Write(rules.DefaultYAML()); return err }(); err != nil {
		t.Fatal(err)
	}
	p := t.TempDir() + "/live.yaml"
	body := strings.Replace(out.String(), "version: 2026-11-06.1", "version: "+version, 1)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// F13-R2: a broken ruleset published in the database stops startup instead of serving with unknown rules.
func TestServeRefusesABrokenPublishedRuleset_F13R2(t *testing.T) {
	st, dsn := openTestStore(t)
	if _, err := st.Pool().Exec(context.Background(), `INSERT INTO rulesets (version, yaml, active) VALUES ('broken', 'version: [', true)`); err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	err := serve(context.Background(), ln, testConfig(t, dsn, nil), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("err = %v, want a ruleset error naming the version", err)
	}
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

func TestTenantsSetWorkers(t *testing.T) {
	st, _ := openTestStore(t)
	out, err := run(t, st, "tenants", "create", "--id", "pool-co")
	if err != nil {
		t.Fatal(out, err)
	}
	if out, err = run(t, st, "tenants", "set-workers", "--tenant", "pool-co", "--min", "1", "--max", "5"); err != nil || !strings.Contains(out, "workers 1 to 5") {
		t.Fatalf("%q %v", out, err)
	}
	if out, err = run(t, st, "tenants", "set-workers", "--tenant", "pool-co", "--clear"); err != nil || !strings.Contains(out, "cleared") {
		t.Fatalf("%q %v", out, err)
	}
	for name, args := range map[string][]string{
		"no tenant":  {"set-workers", "--max", "3"},
		"no limits":  {"set-workers", "--tenant", "pool-co"},
		"min > max":  {"set-workers", "--tenant", "pool-co", "--min", "5", "--max", "2"},
		"no tenant2": {"set-workers", "--tenant", "ghost", "--max", "2"},
	} {
		if _, err := run(t, st, "tenants", args...); err == nil {
			t.Errorf("%s should fail", name)
		}
	}
}

func TestRestartConfigNeverShowsSecrets(t *testing.T) {
	cfg := config.Server{AlertWebhookURL: "https://hooks.example.com/services/T000/B000/SECRET", AdminToken: "tok", DatabaseURL: "postgres://u:pw@h/db"}
	for _, it := range restartConfig(cfg) {
		if strings.Contains(it.Value+it.Note, "SECRET") || strings.Contains(it.Value, "pw@") || strings.Contains(it.Value, "tok") {
			t.Fatalf("%s leaks a secret: %q", it.Name, it.Value)
		}
	}
}
