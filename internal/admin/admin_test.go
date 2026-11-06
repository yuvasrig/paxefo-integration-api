package admin_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/admin"
	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/rulesync"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/testutil/pgtest"
)

var ctx = context.Background()

const token = "s3cret-admin-token"

type env struct {
	ing    *ingest.Service
	t      *testing.T
	st     *store.PGStore
	srv    *httptest.Server
	client *http.Client
	live   *settings.Holder
	rules  *rules.Holder
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st, err := store.Open(ctx, pgtest.NewDSN(t), store.Options{MaxConns: 10})
	must(t, err)
	t.Cleanup(st.Close)
	must(t, st.Migrate(ctx))

	base, _ := rules.Default()
	holder := rules.NewHolder(base, rules.SourceDefault, time.Now())
	reloader := &rulesync.Reloader{Source: st, Holder: holder, Base: base, BaseSource: rules.SourceDefault, Log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	defaults := settings.Values{QueueMaxDepth: 5000, WorkersMin: 2, WorkersMax: 8, JobsPerWorker: 50, LagSLOSeconds: 10, MaxAttempts: 5}
	live := settings.NewHolder(defaults)
	syncer := &settings.Syncer{Source: st, Holder: live, Base: defaults}

	ing := &ingest.Service{
		Store: st, Provider: tive.NewDynamic(holder, config.Default()),
		Now: func() time.Time { return time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC) }, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	c := &admin.Console{
		Replayer: ing, Store: st, Token: token, Settings: live, Defaults: defaults, Rules: holder,
		ReloadRules: reloader.Refresh, ReloadSettings: syncer.Refresh, DefaultMode: "sync",
		Restart:       []admin.ConfigItem{{Name: "PORT", Value: "8080", Note: "listen"}},
		PublicBaseURL: "https://api.example.com", LogsURL: "https://logs.example.com/x", DashboardURL: "https://grafana.example.com",
		RefreshSeconds: 10, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	mux := http.NewServeMux()
	mux.Handle("/admin/", c.Handler())
	mux.Handle("/admin", c.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &env{ing: ing, t: t, st: st, srv: srv, client: client, live: live, rules: holder}
}

func (e *env) do(method, path string, form url.Values, hdr ...string) (int, string, http.Header) {
	e.t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, e.srv.URL+path, body)
	must(e.t, err)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.client.Do(req)
	must(e.t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func (e *env) signIn() {
	e.t.Helper()
	if code, _, _ := e.do("POST", "/admin/login", url.Values{"token": {token}}); code != 303 {
		e.t.Fatalf("sign-in: %d", code)
	}
}

func (e *env) get(path string) string {
	e.t.Helper()
	code, body, _ := e.do("GET", path, nil)
	if code != 200 {
		e.t.Fatalf("GET %s: %d %s", path, code, body)
	}
	return body
}

func (e *env) post(path string, form url.Values) (int, string, http.Header) {
	e.t.Helper()
	return e.do("POST", path, form)
}

func has(t *testing.T, body string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(body, p) {
			t.Fatalf("page does not contain %q:\n%s", p, body)
		}
	}
}

// ---- access ----

func TestDisabledWithoutToken(t *testing.T) {
	c := &admin.Console{}
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/admin/", nil))
	if rec.Code != 404 {
		t.Fatalf("an unconfigured console must not exist: %d", rec.Code)
	}
}

func TestSignInIsRequired(t *testing.T) {
	e := newEnv(t)
	for _, p := range []string{"/admin/", "/admin/tenants", "/admin/rules", "/admin/config", "/admin/monitoring", "/admin/monitoring/live"} {
		code, _, hdr := e.do("GET", p, nil)
		if code != 302 || hdr.Get("Location") != "/admin/login" {
			t.Fatalf("%s without a session: %d %q", p, code, hdr.Get("Location"))
		}
	}
	if code, _, _ := e.do("POST", "/admin/tenants", url.Values{"id": {"sneaky"}}); code != 401 {
		t.Fatalf("a post without a session: %d", code)
	}
	if ok, _ := e.st.TenantExists(ctx, "sneaky"); ok {
		t.Fatal("nothing may be created without signing in")
	}
	if code, _, _ := e.do("POST", "/admin/login", url.Values{"token": {"wrong"}}); code != 401 {
		t.Fatalf("wrong token: %d", code)
	}
	if code, _, _ := e.do("GET", "/admin/", nil); code != 302 {
		t.Fatal("a failed sign-in must not leave a session")
	}
}

func TestSignInSetsASafeCookie(t *testing.T) {
	e := newEnv(t)
	code, _, hdr := e.do("POST", "/admin/login", url.Values{"token": {token}})
	if code != 303 {
		t.Fatalf("%d", code)
	}
	ck := hdr.Get("Set-Cookie")
	has(t, ck, "paxafe_admin=", "HttpOnly", "SameSite=Strict", "Path=/admin")
	if strings.Contains(ck, token) {
		t.Fatal("the cookie must not contain the token")
	}
	if !strings.Contains(e.get("/admin/"), "Dashboard") {
		t.Fatal("dashboard")
	}
	if code, _, _ := e.post("/admin/logout", nil); code != 303 {
		t.Fatalf("logout %d", code)
	}
	if code, _, _ := e.do("GET", "/admin/", nil); code != 302 {
		t.Fatal("logged out")
	}
}

func TestScriptsCanUseTheTokenHeader(t *testing.T) {
	e := newEnv(t)
	if code, _, _ := e.do("GET", "/admin/tenants", nil, "X-Admin-Token", token); code != 200 {
		t.Fatalf("%d", code)
	}
	if code, _, _ := e.do("GET", "/admin/tenants", nil, "X-Admin-Token", "nope"); code != 302 {
		t.Fatalf("%d", code)
	}
}

func TestCrossSitePostIsRefused(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	code, _, _ := e.do("POST", "/admin/tenants", url.Values{"id": {"evil"}}, "Origin", "https://evil.example")
	if code != 403 {
		t.Fatalf("%d", code)
	}
	if ok, _ := e.st.TenantExists(ctx, "evil"); ok {
		t.Fatal("a cross-site post created a tenant")
	}
	if code, _, _ := e.do("POST", "/admin/tenants", url.Values{"id": {"fine-co"}}, "Origin", e.srv.URL); code != 200 {
		t.Fatalf("same-origin post: %d", code)
	}
}

func TestSignInGuessingIsLimited(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 10; i++ {
		e.do("POST", "/admin/login", url.Values{"token": {"guess"}})
	}
	if code, _, _ := e.do("POST", "/admin/login", url.Values{"token": {token}}); code != 429 {
		t.Fatalf("even the right token waits after ten misses: %d", code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	e := newEnv(t)
	_, _, hdr := e.do("GET", "/admin/login", nil)
	if hdr.Get("Cache-Control") != "no-store" || hdr.Get("X-Frame-Options") != "DENY" || !strings.Contains(hdr.Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatalf("%v", hdr)
	}
}

// ---- tenants ----

var keyRe = regexp.MustCompile(`X-API-Key: (\S+)`)

func TestCreateTenantIssuesAKeyAndShowsSetup(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	code, body, _ := e.post("/admin/tenants", url.Values{"id": {"newco"}, "name": {"New Co"}, "mode": {"async"}})
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	has(t, body, "Tenant &#34;newco&#34; created", "https://api.example.com/api/webhook/tive", "curl -i -X POST")
	m := keyRe.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("no key shown:\n%s", body)
	}
	key := m[1]
	// The key works, for exactly this tenant.
	a := &auth.Authenticator{Keys: e.st, Now: time.Now}
	p, err := a.Authenticate(ctx, key)
	if err != nil || p.TenantID != "newco" {
		t.Fatalf("%+v %v", p, err)
	}
	// It is shown once: the tenants page afterwards has only the prefix.
	if page := e.get("/admin/tenants"); strings.Contains(page, key) {
		t.Fatal("the plaintext key must never be shown again")
	}
	if mode, _ := e.st.TenantIngestMode(ctx, "newco"); mode != "async" {
		t.Fatalf("mode %q", mode)
	}
}

func TestCreateTenantValidation(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	for _, id := range []string{"", "A", "Bad Id", "-lead", "x", strings.Repeat("a", 41), "has/slash", "<b>"} {
		_, body, _ := e.post("/admin/tenants", url.Values{"id": {id}})
		has(t, body, "A tenant id is 2-40 characters")
	}
	e.post("/admin/tenants", url.Values{"id": {"dup-co"}})
	_, body, _ := e.post("/admin/tenants", url.Values{"id": {"dup-co"}})
	has(t, body, "already exists")
}

func TestTenantNameIsEscaped(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	e.post("/admin/tenants", url.Values{"id": {"xss-co"}, "name": {`<script>alert(1)</script>`}})
	page := e.get("/admin/tenants")
	if strings.Contains(page, "<script>alert(1)") || !strings.Contains(page, "&lt;script&gt;alert(1)") {
		t.Fatal("tenant names must be escaped")
	}
}

func TestTenantSettingsAndKeyLifecycle(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	e.post("/admin/tenants", url.Values{"id": {"cfg-co"}})
	code, _, hdr := e.post("/admin/tenants/cfg-co/settings", url.Values{"mode": {"async"}, "policy": {"keep_last"}, "http_status": {"200"}})
	if code != 303 || !strings.Contains(hdr.Get("Location"), "done=") {
		t.Fatalf("%d %q", code, hdr.Get("Location"))
	}
	info, _ := e.st.ListTenantInfo(ctx)
	var cfg store.TenantInfo
	for _, i := range info {
		if i.ID == "cfg-co" {
			cfg = i
		}
	}
	cs, _ := store.ParseConflictSettings(cfg.Config)
	if cs.Policy != "keep_last" || cs.HTTPStatus != 200 {
		t.Fatalf("%+v", cs)
	}
	if mode, _ := e.st.TenantIngestMode(ctx, "cfg-co"); mode != "async" {
		t.Fatalf("%q", mode)
	}
	_, body, _ := e.post("/admin/tenants/cfg-co/settings", url.Values{"mode": {"turbo"}, "policy": {"keep_last"}})
	has(t, body, "Ingest mode must be sync or async")
	_, body, _ = e.post("/admin/tenants/cfg-co/settings", url.Values{"mode": {"sync"}, "policy": {"nonsense"}})
	has(t, body, "conflict policy must be")
	if code, _, _ := e.post("/admin/tenants/ghost/settings", url.Values{"mode": {"sync"}, "policy": {"keep_first"}}); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}

	// A second key, then revoke the first.
	_, body, _ = e.post("/admin/tenants/cfg-co/keys", url.Values{"name": {"second"}})
	second := keyRe.FindStringSubmatch(body)[1]
	keys, _ := e.st.ListAPIKeys(ctx, "cfg-co")
	if len(keys) != 2 {
		t.Fatalf("%d keys", len(keys))
	}
	var first store.APIKey
	for _, k := range keys {
		if k.Name == "console" {
			first = k
		}
	}
	code, _, _ = e.post("/admin/tenants/cfg-co/keys/"+itoa(first.ID)+"/revoke", nil)
	if code != 303 {
		t.Fatalf("revoke %d", code)
	}
	a := &auth.Authenticator{Keys: e.st, Now: time.Now}
	if _, err := a.Authenticate(ctx, second); err != nil {
		t.Fatalf("the other key keeps working: %v", err)
	}
}

func itoa(n int64) string { return fmtInt(n) }

func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// ---- rules ----

func TestRulesValidatePublishAndReset(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	page := e.get("/admin/rules")
	has(t, page, "2026-11-06.1", "default", "textarea")

	// Garbage is reported and changes nothing.
	_, body, _ := e.post("/admin/rules/validate", url.Values{"yaml": {"version: [oops"}})
	has(t, body, "Not valid, nothing changed")
	_, body, _ = e.post("/admin/rules/publish", url.Values{"yaml": {"version: [oops"}})
	has(t, body, "Not valid, nothing published")
	if e.rules.Current().Version != "2026-11-06.1" {
		t.Fatal("an invalid ruleset must never replace the working one")
	}

	// A valid new version: validate first, then publish; it is live at once on this instance.
	yaml := strings.Replace(string(rules.DefaultYAML()), "2026-11-06.1", "console-test.1", 1)
	_, body, _ = e.post("/admin/rules/validate", url.Values{"yaml": {yaml}})
	has(t, body, "Valid. Version &#34;console-test.1&#34;")
	code, _, hdr := e.post("/admin/rules/publish", url.Values{"yaml": {yaml}})
	if code != 303 {
		t.Fatalf("%d", code)
	}
	if e.rules.Current().Version != "console-test.1" {
		t.Fatalf("published rules should be live at once, got %s", e.rules.Current().Version)
	}
	has(t, e.get(hdr.Get("Location")), "Published console-test.1", "admin-console")

	// The same version again is refused (every event records its version).
	_, body, _ = e.post("/admin/rules/publish", url.Values{"yaml": {yaml}})
	has(t, body, "was published before")

	// History can be loaded back into the editor.
	hist, _ := e.st.RulesetHistory(ctx, 5)
	has(t, e.get("/admin/rules?from="+fmtInt(hist[0].ID)), "console-test.1")

	// Reset returns to the built-in rules.
	if code, _, _ := e.post("/admin/rules/reset", nil); code != 303 {
		t.Fatalf("%d", code)
	}
	if e.rules.Current().Version != "2026-11-06.1" {
		t.Fatalf("after reset: %s", e.rules.Current().Version)
	}
}

// ---- config ----

func TestConfigPageShowsEverythingAndEditsLiveSettings(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	page := e.get("/admin/config")
	has(t, page, "Live settings", "Needs a restart", "Queue limit per tenant", "5000", "PORT", "8080")

	code, _, hdr := e.post("/admin/config/set", url.Values{"key": {"queue_max_depth"}, "value": {"123"}})
	if code != 303 {
		t.Fatalf("%d", code)
	}
	if e.live.Current().QueueMaxDepth != 123 {
		t.Fatal("a saved setting must be live immediately")
	}
	has(t, e.get(hdr.Get("Location")), "Saved queue_max_depth = 123", "changed by admin-console")

	for _, bad := range []url.Values{
		{"key": {"queue_max_depth"}, "value": {"banana"}},
		{"key": {"queue_max_depth"}, "value": {"-5"}},
		{"key": {"nonsense"}, "value": {"1"}},
		{"key": {"workers_min"}, "value": {"99"}}, // above workers_max (8)
	} {
		_, body, _ := e.post("/admin/config/set", bad)
		has(t, body, "Not saved")
	}
	if e.live.Current().WorkersMin != 2 || e.live.Current().QueueMaxDepth != 123 {
		t.Fatalf("a rejected edit must change nothing: %+v", e.live.Current())
	}

	if code, _, _ := e.post("/admin/config/reset", url.Values{"key": {"queue_max_depth"}}); code != 303 {
		t.Fatal("reset")
	}
	if e.live.Current().QueueMaxDepth != 5000 {
		t.Fatalf("reset returns to the default: %d", e.live.Current().QueueMaxDepth)
	}
	if code, _, _ := e.post("/admin/config/reset", url.Values{"key": {"nonsense"}}); code != 404 {
		t.Fatal("unknown key")
	}
}

// ---- dashboard and monitoring ----

func TestDashboardAndMonitoringShowTheLoad(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	must(t, e.st.CreateTenant(ctx, "busy", "Busy", nil))
	must(t, e.st.SetTenantIngestMode(ctx, "busy", "async"))
	for i := 0; i < 3; i++ {
		raw := []byte(`{"DeviceId":"863257063350583","n":` + fmtInt(int64(i)) + `}`)
		must(t, e.st.SaveRawAndEnqueue(ctx, store.RawEvent{ID: store.NewID("evt_"), TenantID: "busy", Provider: "Tive", Raw: raw, Hash: "h" + fmtInt(int64(i)), ReceivedAt: time.Now()}))
	}

	// Jobs are waiting and no process has reported in: the prediction says so.
	dash := e.get("/admin/")
	has(t, dash, "busy", "async", "Stalled", "No workers are running")
	has(t, dash, `class="alert critical"`, "busy has 3 jobs waiting and no worker is running for it")

	// Workers report in and the picture changes.
	must(t, e.st.ReportHeartbeat(ctx, store.Heartbeat{Instance: "host-1", TenantID: "busy", Role: "all", Workers: 3, InFlight: 1, StartedAt: time.Now()}))
	live := e.get("/admin/monitoring/live")
	has(t, live, "host-1", "busy", "pending")
	if strings.Contains(live, "No workers are running") {
		t.Fatal("workers are reporting")
	}

	mon := e.get("/admin/monitoring")
	has(t, mon, `data-live-url="/admin/monitoring/live"`, "/metrics", "https://logs.example.com/x", "https://grafana.example.com")

	if strings.Contains(live, `class="alert critical"`) {
		t.Fatal("workers are reporting, so the no-workers alert must clear")
	}

	// Nothing waiting: up to date.
	_, _ = e.st.Pool().Exec(ctx, `DELETE FROM ingest_jobs`)
	has(t, e.get("/admin/monitoring/live"), "Up to date")
}

func TestAssetsAreServed(t *testing.T) {
	e := newEnv(t)
	for p, ct := range map[string]string{"/admin/admin.css": "text/css", "/admin/admin.js": "text/javascript"} {
		code, body, hdr := e.do("GET", p, nil)
		if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), ct) || len(body) < 50 {
			t.Fatalf("%s: %d %s", p, code, hdr.Get("Content-Type"))
		}
	}
}

func TestPerTenantSettingsOverrideGlobalOnes(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	e.post("/admin/tenants", url.Values{"id": {"pool-co"}})
	e.post("/admin/tenants", url.Values{"id": {"plain-co"}})

	page := e.get("/admin/config?tenant=pool-co")
	has(t, page, "Settings for <strong>pool-co</strong>", "Queue limit per tenant", "5000", `<span class="muted">global</span>`)
	if strings.Contains(page, "Needs a restart") || strings.Contains(page, "Workers: hard cap") {
		t.Fatal("process-wide settings are global only")
	}

	// A tenant's own value applies to that tenant only.
	if code, _, hdr := e.post("/admin/config/set", url.Values{"tenant": {"pool-co"}, "key": {"queue_max_depth"}, "value": {"10"}}); code != 303 || !strings.Contains(hdr.Get("Location"), "tenant=pool-co") {
		t.Fatalf("%d %q", code, hdr.Get("Location"))
	}
	if e.live.For("pool-co").QueueMaxDepth != 10 || e.live.For("plain-co").QueueMaxDepth != 5000 || e.live.Current().QueueMaxDepth != 5000 {
		t.Fatalf("pool-co %d, plain-co %d, global %d", e.live.For("pool-co").QueueMaxDepth, e.live.For("plain-co").QueueMaxDepth, e.live.Current().QueueMaxDepth)
	}
	has(t, e.get("/admin/config?tenant=pool-co"), `<span class="tag flagged">this tenant</span>`, "Use global")
	has(t, e.get("/admin/tenants"), "<code>queue_max_depth=10</code>", "edit settings for pool-co")

	// Changing the global value moves the tenants that follow it, and not the one that overrides it.
	e.post("/admin/config/set", url.Values{"key": {"queue_max_depth"}, "value": {"777"}})
	if e.live.For("pool-co").QueueMaxDepth != 10 || e.live.For("plain-co").QueueMaxDepth != 777 {
		t.Fatalf("per tenant beats global: %d %d", e.live.For("pool-co").QueueMaxDepth, e.live.For("plain-co").QueueMaxDepth)
	}

	// Validation is against the tenant's own values: it cannot put its minimum above its maximum.
	e.post("/admin/config/set", url.Values{"tenant": {"pool-co"}, "key": {"workers_max"}, "value": {"3"}})
	_, body, _ := e.post("/admin/config/set", url.Values{"tenant": {"pool-co"}, "key": {"workers_min"}, "value": {"5"}})
	has(t, body, "Not saved", "workers_min (5) cannot be more than workers_max (3)")
	for _, bad := range []url.Values{
		{"tenant": {"pool-co"}, "key": {"queue_max_depth"}, "value": {"banana"}},
		{"tenant": {"pool-co"}, "key": {"workers_total_max"}, "value": {"4"}},
		{"tenant": {"pool-co"}, "key": {"nonsense"}, "value": {"1"}},
	} {
		_, body, _ := e.post("/admin/config/set", bad)
		has(t, body, "Not saved")
	}
	if e.live.For("pool-co").WorkersMin != 2 || e.live.For("pool-co").WorkersMax != 3 {
		t.Fatalf("a refused change must change nothing: %+v", e.live.For("pool-co"))
	}
	if code, _, _ := e.post("/admin/config/set", url.Values{"tenant": {"ghost"}, "key": {"queue_max_depth"}, "value": {"1"}}); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}
	if code, _, _ := e.do("GET", "/admin/config?tenant=ghost", nil); code != 404 {
		t.Fatalf("unknown tenant page: %d", code)
	}

	// "Use global" removes the override.
	if code, _, _ := e.post("/admin/config/reset", url.Values{"tenant": {"pool-co"}, "key": {"queue_max_depth"}}); code != 303 {
		t.Fatalf("%d", code)
	}
	if e.live.For("pool-co").QueueMaxDepth != 777 {
		t.Fatalf("back to the global value: %d", e.live.For("pool-co").QueueMaxDepth)
	}
}

// The console's content-security policy forbids inline handlers, so confirmations must be data attributes that
// admin.js handles; an inline onclick would silently never ask.
func TestNoInlineHandlersAndConfirmsAreWired(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	must(t, e.st.CreateTenant(ctx, "confirm-co", "Confirm Co", nil)) // the replay form needs a tenant
	for _, p := range []string{"/admin/", "/admin/tenants", "/admin/conflicts", "/admin/rules", "/admin/config", "/admin/monitoring"} {
		page := e.get(p)
		for _, bad := range []string{" onclick=", " onsubmit=", " onchange=", " onload="} {
			if strings.Contains(page, bad) {
				t.Fatalf("%s has an inline handler %q that the CSP blocks", p, bad)
			}
		}
	}
	has(t, e.get("/admin/conflicts"), "data-confirm=", "data-autosubmit")
	has(t, e.get("/admin/rules"), `data-confirm="Go back to the built-in default rules?"`)
	_, js, _ := e.do("GET", "/admin/admin.js", nil)
	has(t, js, "data-confirm", "data-autosubmit")
}

func (e *env) send(tenant string, n int) {
	e.t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("../../testdata/samples/%02d.json", n))
	must(e.t, err)
	if _, err := e.ing.Ingest(ctx, ingest.Request{TenantID: tenant, RequestID: "r", Body: b}); err != nil {
		e.t.Fatal(err)
	}
}

func TestConflictViewerAndReplay(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	must(t, e.st.CreateTenant(ctx, "clash-co", "Clash Co", nil))
	has(t, e.get("/admin/conflicts?tenant=clash-co"), "No conflicts for this tenant")

	e.send("clash-co", 0)
	e.send("clash-co", 1) // same device and time, different content: a conflict, first version kept
	page := e.get("/admin/conflicts?tenant=clash-co")
	has(t, page, "kept_first", "Temperature.Celsius", "&rarr;", "Fix and replay")
	must(t, e.st.CreateTenant(ctx, "other-co", "Other Co", nil))
	if strings.Contains(e.get("/admin/conflicts?tenant=other-co"), "kept_first") {
		t.Fatal("another tenant must not show this tenant's conflicts")
	}
	if code, _, _ := e.do("GET", "/admin/conflicts?tenant=ghost", nil); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}

	// Change the policy, preview (nothing written), then apply.
	cs, _ := store.NewConflictSettings("keep_last", 0)
	must(t, e.st.SetConflictSettings(ctx, "clash-co", cs))
	_, body, _ := e.post("/admin/conflicts/replay", url.Values{"tenant": {"clash-co"}, "hours": {"1000"}, "mode": {"preview"}})
	has(t, body, "Preview: nothing was written", "conflicting events selected")
	if rows, _ := e.st.ListConflicts(ctx, "clash-co", 10); len(rows) != 1 || rows[0].Resolution != "kept_first" {
		t.Fatalf("a preview changes nothing: %+v", rows)
	}
	_, body, _ = e.post("/admin/conflicts/replay", url.Values{"tenant": {"clash-co"}, "hours": {"1000"}, "mode": {"apply"}})
	has(t, body, "Applied")
	if rows, _ := e.st.ListConflicts(ctx, "clash-co", 10); len(rows) != 1 || rows[0].Resolution != "replaced" {
		t.Fatalf("keep_last replaced the reading: %+v", rows)
	}
	if code, _, _ := e.post("/admin/conflicts/replay", url.Values{"tenant": {"ghost"}, "mode": {"apply"}}); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}
}

func TestAlertsShowDeadEventsAndFeedTheNotifier(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	must(t, e.st.CreateTenant(ctx, "dead-co", "Dead Co", nil))
	id := store.NewID("evt_")
	raw := []byte(`{}`)
	must(t, e.st.SaveRawAndEnqueue(ctx, store.RawEvent{ID: id, TenantID: "dead-co", Provider: "Tive", Raw: raw, Hash: "h", ReceivedAt: time.Now()}))
	must(t, e.st.BuryJob(ctx, "dead-co", id, "boom"))
	has(t, e.get("/admin/monitoring/live"), `class="alert warning"`, "Dead events", "dead-co has 1 events that failed every attempt")
}

// ---- friendly rules page ----

func TestRulesPageReadsInPlainEnglish(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	page := e.get("/admin/rules")
	has(t, page,
		"When Humidity (%) is outside 0 to 100, clear the value and flag it OUT_OF_RANGE (warning).",
		"When Temperature (°C) is exactly one of -32,768, 327.67, 32,767, clear the value and flag it SENSOR_FAULT (warning).",
		"reject the whole reading (TIMESTAMP_FUTURE)", "within 10 seconds", "Add a rule", "Advanced: edit the rules as YAML")
}

func (e *env) rule(id string) *rules.Rule { return e.rules.Current().Find(id) }

var versionRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}\.\d+$`)

func TestSwitchARuleOffAndOnKeepsItsAction(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	code, _, hdr := e.post("/admin/rules/toggle", url.Values{"id": {"TEMP-SENTINEL"}, "off": {"1"}})
	if code != 303 {
		t.Fatalf("%d", code)
	}
	r := e.rule("TEMP-SENTINEL")
	if !r.Disabled || r.Action != "nullify" {
		t.Fatalf("off, with its action kept: %+v", r)
	}
	if v := e.rules.Current().Version; !versionRe.MatchString(v) {
		t.Fatalf("the console picks the version: %q", v)
	}
	page := e.get(hdr.Get("Location"))
	has(t, page, "Switched TEMP-SENTINEL off.", "Published as version", "1 switched off", `class="switch is-off"`)
	e.post("/admin/rules/toggle", url.Values{"id": {"TEMP-SENTINEL"}, "off": {"0"}})
	if e.rule("TEMP-SENTINEL").Disabled {
		t.Fatal("switched back on")
	}
	hist, _ := e.st.RulesetHistory(ctx, 10)
	if len(hist) != 2 || hist[0].Version == hist[1].Version {
		t.Fatalf("every change is a new version: %+v", hist)
	}
}

func TestEditARulesThresholdsFromTheForm(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	before := e.rules.Current().Version
	code, _, _ := e.post("/admin/rules/edit", url.Values{"id": {"HUMIDITY-RANGE"}, "out_min": {"5"}, "out_max": {"95"}, "action": {"nullify"}, "code": {"HUMIDITY_BAD"}, "severity": {"error"}, "section": {"sensor"}})
	if code != 303 {
		t.Fatalf("%d", code)
	}
	if got := e.rule("HUMIDITY-RANGE").Sentence(); got != "When Humidity (%) is outside 5 to 95, clear the value and flag it HUMIDITY_BAD (error)." {
		t.Fatal(got)
	}
	if e.rules.Current().Version == before {
		t.Fatal("a change gets a new version")
	}
	// Bad input is explained and nothing is published.
	version := e.rules.Current().Version
	for name, form := range map[string]url.Values{
		"not a number": {"id": {"HUMIDITY-RANGE"}, "out_min": {"x"}, "out_max": {"95"}},
		"half a range": {"id": {"HUMIDITY-RANGE"}, "out_min": {"5"}},
		"bad list":     {"id": {"TEMP-SENTINEL"}, "in": {"1, two"}},
		"unknown rule": {"id": {"NOPE"}, "code": {"X"}},
		"empty code":   {"id": {"HUMIDITY-RANGE"}, "code": {""}, "severity": {"warn"}, "action": {"flag"}},
		"bad severity": {"id": {"HUMIDITY-RANGE"}, "severity": {"loud"}, "code": {"X"}, "action": {"flag"}},
	} {
		_, body, _ := e.post("/admin/rules/edit", form)
		if !strings.Contains(body, "Nothing was changed") {
			t.Errorf("%s: %s", name, body)
		}
	}
	if e.rules.Current().Version != version {
		t.Fatal("a refused change publishes nothing")
	}
}

func TestGuardrailsStillApplyToTheForms(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	version := e.rules.Current().Version
	_, body, _ := e.post("/admin/rules/edit", url.Values{"id": {"TS-FUTURE"}, "action": {"nullify"}, "severity": {"warn"}, "code": {"TIMESTAMP_FUTURE"}})
	has(t, body, "Nothing was changed", "protected field EntryTimeEpoch cannot be nullified")
	_, body, _ = e.post("/admin/rules/add", url.Values{"field": {"EntryTimeEpoch"}, "check": {"below"}, "v1": {"5"}, "action": {"drop_section"}, "section": {"sensor"}, "code": {"X"}})
	has(t, body, "Nothing was changed", "protected field EntryTimeEpoch cannot use drop_section")
	if e.rules.Current().Version != version {
		t.Fatal("nothing published")
	}
}

func TestAddAndDeleteARuleFromDropdowns(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	n := len(e.rules.Current().Rules)
	code, _, _ := e.post("/admin/rules/add", url.Values{"field": {"Battery.Percentage"}, "check": {"below"}, "v1": {"10"}, "action": {"flag"}, "code": {"low battery"}, "severity": {"warn"}, "id": {"battery low"}})
	if code != 303 || len(e.rules.Current().Rules) != n+1 {
		t.Fatalf("%d, %d rules", code, len(e.rules.Current().Rules))
	}
	if got := e.rule("BATTERY-LOW").Sentence(); got != "When Battery (%) is below 10, keep the value and flag it LOW_BATTERY (warning)." {
		t.Fatal(got)
	}
	// Each kind of check builds a valid rule; an unnamed rule gets a name.
	for name, f := range map[string]url.Values{
		"outside": {"field": {"Humidity.Percentage"}, "check": {"outside"}, "v1": {"10"}, "v2": {"90"}, "action": {"reject"}, "code": {"HUM"}},
		"one of":  {"field": {"Light.Lux"}, "check": {"oneof"}, "list": {"-1, 65535"}, "action": {"nullify"}, "code": {"LUX_FAULT"}},
		"missing": {"field": {"Battery.Percentage"}, "check": {"missing"}, "action": {"flag"}, "code": {"NO_BATTERY"}, "severity": {"info"}},
		"numtext": {"field": {"Light.Lux"}, "check": {"numtext"}, "action": {"flag"}, "code": {"LUX_TEXT"}},
		"future":  {"field": {"EntryTimeEpoch"}, "check": {"future"}, "v1": {"30"}, "action": {"reject"}, "code": {"TOO_NEW"}},
		"differs": {"field": {"EntityName"}, "check": {"differs"}, "other": {"DeviceName"}, "action": {"flag"}, "code": {"NAME_DIFFERS"}},
		"drop":    {"field": {"Location.Accuracy.Meters"}, "check": {"below"}, "v1": {"0"}, "action": {"drop_section"}, "section": {"location"}, "code": {"BAD_ACC"}},
		"custom":  {"field_custom": {"Shipment.Carrier"}, "check": {"missing"}, "action": {"flag"}, "code": {"NO_CARRIER"}},
	} {
		if code, body, _ := e.post("/admin/rules/add", f); code != 303 {
			t.Errorf("%s: %d %s", name, code, body)
		}
	}
	if e.rule("CUSTOM-1") == nil || e.rule("CUSTOM-2") == nil {
		t.Fatal("unnamed rules get CUSTOM-n names")
	}
	for name, f := range map[string]url.Values{
		"no field":     {"check": {"missing"}, "action": {"flag"}},
		"no check":     {"field": {"Light.Lux"}, "action": {"flag"}},
		"no action":    {"field": {"Light.Lux"}, "check": {"missing"}},
		"below no num": {"field": {"Light.Lux"}, "check": {"below"}, "action": {"flag"}},
		"outside half": {"field": {"Light.Lux"}, "check": {"outside"}, "v1": {"1"}, "action": {"flag"}},
		"bad list":     {"field": {"Light.Lux"}, "check": {"oneof"}, "list": {"a"}, "action": {"flag"}},
		"future zero":  {"field": {"EntryTimeEpoch"}, "check": {"future"}, "v1": {"0"}, "action": {"reject"}},
		"differs none": {"field": {"EntityName"}, "check": {"differs"}, "action": {"flag"}},
		"duplicate":    {"field": {"Battery.Percentage"}, "check": {"missing"}, "action": {"flag"}, "id": {"BATTERY-LOW"}, "code": {"X"}},
	} {
		_, body, _ := e.post("/admin/rules/add", f)
		if !strings.Contains(body, "not added") && !strings.Contains(body, "Nothing was changed") {
			t.Errorf("%s should be refused: %s", name, body)
		}
	}
	e.post("/admin/rules/delete", url.Values{"id": {"BATTERY-LOW"}})
	if e.rule("BATTERY-LOW") != nil {
		t.Fatal("deleted")
	}
	_, body, _ := e.post("/admin/rules/delete", url.Values{"id": {"BATTERY-LOW"}})
	has(t, body, "Nothing was changed")
}

func TestMoveARule(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	first, second := e.rules.Current().Rules[0].ID, e.rules.Current().Rules[1].ID
	e.post("/admin/rules/move", url.Values{"id": {second}, "dir": {"up"}})
	if e.rules.Current().Rules[0].ID != second || e.rules.Current().Rules[1].ID != first {
		t.Fatal("moved up")
	}
	e.post("/admin/rules/move", url.Values{"id": {second}, "dir": {"down"}})
	if e.rules.Current().Rules[0].ID != first {
		t.Fatal("moved down")
	}
}

func TestPerTenantRuleOverrides(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	must(t, e.st.CreateTenant(ctx, "acme-co", "Acme", nil))
	page := e.get("/admin/rules?tenant=acme-co")
	has(t, page, "Rules for acme-co", `<span class="muted">same as global</span>`)
	if strings.Contains(page, "Add a rule") || strings.Contains(page, "Delete this rule") {
		t.Fatal("a tenant page changes only that tenant: no add or delete")
	}
	code, _, hdr := e.post("/admin/rules/override", url.Values{"tenant": {"acme-co"}, "id": {"HUMIDITY-RANGE"}, "action": {"ignore"}, "severity": {""}})
	if code != 303 || !strings.Contains(hdr.Get("Location"), "tenant=acme-co") {
		t.Fatalf("%d %q", code, hdr.Get("Location"))
	}
	if e.rules.Current().TenantOverride("acme-co", "HUMIDITY-RANGE").Action != "ignore" || e.rules.Current().TenantOverride("other", "HUMIDITY-RANGE").Action != "" {
		t.Fatal("the override is for acme-co only")
	}
	has(t, e.get(hdr.Get("Location")), `<span class="tag flagged">this tenant</span>`, "do nothing", "Saved HUMIDITY-RANGE for acme-co")
	if e.rule("HUMIDITY-RANGE").Action != "nullify" {
		t.Fatal("the global rule is unchanged")
	}
	// Severity alone, then back to the global rule.
	e.post("/admin/rules/override", url.Values{"tenant": {"acme-co"}, "id": {"HUMIDITY-RANGE"}, "severity": {"error"}})
	if ov := e.rules.Current().TenantOverride("acme-co", "HUMIDITY-RANGE"); ov.Action != "" || ov.Severity != "error" {
		t.Fatalf("%+v", ov)
	}
	e.post("/admin/rules/override", url.Values{"tenant": {"acme-co"}, "id": {"HUMIDITY-RANGE"}})
	if e.rules.Current().TenantOverride("acme-co", "HUMIDITY-RANGE").Severity != "" || len(e.rules.Current().Overrides) != 0 {
		t.Fatal("same as global removes the override")
	}
	if code, _, _ := e.post("/admin/rules/override", url.Values{"tenant": {"ghost"}, "id": {"HUMIDITY-RANGE"}, "action": {"flag"}}); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}
	if code, _, _ := e.do("GET", "/admin/rules?tenant=ghost", nil); code != 404 {
		t.Fatalf("unknown tenant page: %d", code)
	}
	_, body, _ := e.post("/admin/rules/override", url.Values{"tenant": {"acme-co"}, "id": {"NOPE"}, "action": {"flag"}})
	has(t, body, "Nothing was changed")
}

func TestVersionsNeverCollide(t *testing.T) {
	e := newEnv(t)
	e.signIn()
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		e.post("/admin/rules/toggle", url.Values{"id": {"LUX-NEGATIVE"}, "off": {map[bool]string{true: "1", false: "0"}[i%2 == 0]}})
		v := e.rules.Current().Version
		if seen[v] || !versionRe.MatchString(v) {
			t.Fatalf("version %q repeats or is malformed", v)
		}
		seen[v] = true
	}
}
