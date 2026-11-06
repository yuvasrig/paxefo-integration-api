// Package admin is the operator console, served at /admin by the api role. It is server-rendered HTML (no framework,
// no external scripts) over the same store the CLI uses, so everything it shows or changes is also available from
// `api` commands. It is protected by ADMIN_TOKEN and not mounted at all when that is empty.
//
// What it offers: tenants (add one, issue its key, show the webhook URL and a ready-to-paste Tive setup), per-tenant
// live figures, the rules page (view, validate, publish, history, reset), the config page (every setting, live vs
// restart-required, edit the live ones) and a monitoring page (rates, latency, workers, backlog prediction, links to
// logs and dashboards).
package admin

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

//go:embed web/*
var webFS embed.FS

// Store is what the console needs from the database (implemented by *store.PGStore).
type Store interface {
	ListTenantInfo(ctx context.Context) ([]store.TenantInfo, error)
	CreateTenant(ctx context.Context, id, name string, config map[string]any) error
	TenantExists(ctx context.Context, id string) (bool, error)
	CreateAPIKey(ctx context.Context, tenantID, name, prefix, hash string, expiresAt *time.Time) error
	ListAPIKeys(ctx context.Context, tenantID string) ([]store.APIKey, error)
	RevokeAPIKey(ctx context.Context, id int64) error
	SetTenantSetting(ctx context.Context, tenantID, key, value string) error
	ClearTenantSetting(ctx context.Context, tenantID, key string) error
	SetTenantIngestMode(ctx context.Context, tenantID, mode string) error
	SetConflictSettings(ctx context.Context, tenantID string, cs store.ConflictSettings) error
	TenantStats(ctx context.Context, tenantID string) (store.TenantStats, error)
	RecentEvents(ctx context.Context, tenantID string, limit int, statuses []string) ([]store.RecentEvent, error)
	ActiveHeartbeats(ctx context.Context, within time.Duration) ([]store.Heartbeat, error)
	ListConflicts(ctx context.Context, tenantID string, limit int) ([]store.ConflictRow, error)
	ListSettings(ctx context.Context) ([]store.SettingRow, error)
	SetSetting(ctx context.Context, key, value, by string) error
	ResetSetting(ctx context.Context, key string) error
	PublishRuleset(ctx context.Context, version, yaml, by string) (store.RulesetRow, error)
	ActiveRuleset(ctx context.Context) (store.RulesetRow, bool, error)
	ClearActiveRuleset(ctx context.Context) (bool, error)
	RulesetHistory(ctx context.Context, limit int) ([]store.RulesetRow, error)
}

// Replayer reprocesses stored events (implemented by *ingest.Service): the console uses it for the conflict viewer.
type Replayer interface {
	ReplayWindow(ctx context.Context, opts ingest.ReplayOptions) (ingest.ReplayReport, error)
}

// ConfigItem is a setting that needs a restart to change; the console only shows it.
type ConfigItem struct{ Name, Value, Note string }

// Console is the admin site. Build one and mount Handler() at /admin.
type Console struct {
	Store          Store
	Token          string // ADMIN_TOKEN; the console answers 404 to everything when empty
	Settings       *settings.Holder
	Defaults       settings.Values // the environment defaults, shown beside the live values
	Rules          *rules.Holder
	ReloadRules    func(ctx context.Context) error // apply a published ruleset now instead of at the next poll
	ReloadSettings func(ctx context.Context) error
	Replayer       Replayer     // nil hides the replay buttons
	DefaultMode    string       // INGEST_MODE
	Restart        []ConfigItem // restart-required configuration, shown read-only
	PublicBaseURL  string       // used in webhook URLs; the request's own address when empty
	LogsURL        string
	DashboardURL   string
	RefreshSeconds int // how long a change takes to reach every instance (shown in messages)
	Now            func() time.Time
	Log            *slog.Logger

	once  sync.Once
	tmpl  *template.Template
	fails failures
}

const (
	cookieName = "paxafe_admin"
	// WorkerLiveWithin is how recently a process must have reported to count as running.
	workerLiveWithin = 30 * time.Second
	sessionAge       = 12 * time.Hour
)

func (c *Console) init() {
	c.once.Do(func() {
		c.tmpl = template.Must(template.New("").Funcs(funcs).ParseFS(webFS, "web/*.html"))
		if c.Now == nil {
			c.Now = time.Now
		}
		if c.Log == nil {
			c.Log = slog.Default()
		}
	})
}

// Handler returns the console's routes (all under /admin).
func (c *Console) Handler() http.Handler {
	c.init()
	mux := http.NewServeMux()
	if c.Token == "" {
		return http.NotFoundHandler()
	}
	mux.HandleFunc("GET /admin/login", c.loginPage)
	mux.HandleFunc("POST /admin/login", c.login)
	mux.HandleFunc("GET /admin/admin.css", c.asset("web/admin.css", "text/css; charset=utf-8"))
	mux.HandleFunc("GET /admin/admin.js", c.asset("web/admin.js", "text/javascript; charset=utf-8"))

	page := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, c.protected(h)) }
	page("POST /admin/logout", c.logout)
	page("GET /admin/{$}", c.dashboard)
	page("GET /admin/tenants", c.tenants)
	page("POST /admin/tenants", c.tenantCreate)
	page("POST /admin/tenants/{id}/keys", c.keyCreate)
	page("POST /admin/tenants/{id}/keys/{key}/revoke", c.keyRevoke)
	page("POST /admin/tenants/{id}/settings", c.tenantSettings)
	page("GET /admin/conflicts", c.conflictsPage)
	page("POST /admin/conflicts/replay", c.conflictsReplay)
	page("GET /admin/rules", c.rulesPage)
	page("POST /admin/rules/validate", c.rulesValidate)
	page("POST /admin/rules/publish", c.rulesPublish)
	page("POST /admin/rules/reset", c.rulesReset)
	page("GET /admin/config", c.configPage)
	page("POST /admin/config/set", c.configSet)
	page("POST /admin/config/reset", c.configReset)
	page("GET /admin/monitoring", c.monitoring)
	page("GET /admin/monitoring/live", c.monitoringLive)
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/admin/", http.StatusFound) })
	return noStore(mux)
}

func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Cache-Control", "no-store")
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'none'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}

func (c *Console) asset(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := webFS.ReadFile(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write(b)
	}
}

// ---- authentication ----

// session is the cookie value: a hash of the token, so the token itself is never stored in the browser.
func (c *Console) session() string {
	sum := sha256.Sum256([]byte("paxafe-admin-session:" + c.Token))
	return hex.EncodeToString(sum[:])
}

func (c *Console) authenticated(r *http.Request) bool {
	if ck, err := r.Cookie(cookieName); err == nil && subtle.ConstantTimeCompare([]byte(ck.Value), []byte(c.session())) == 1 {
		return true
	}
	// Scripts may send the token directly.
	if tok := r.Header.Get("X-Admin-Token"); tok != "" {
		return subtle.ConstantTimeCompare(hash(tok), hash(c.Token)) == 1
	}
	return false
}

func hash(s string) []byte { h := sha256.Sum256([]byte(s)); return h[:] }

// protected requires a valid session, and for state-changing requests a same-origin Origin header (with the
// SameSite=Strict cookie this stops cross-site form posts).
func (c *Console) protected(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.authenticated(r) {
			if r.Method == http.MethodGet {
				http.Redirect(w, r, "/admin/login", http.StatusFound)
				return
			}
			http.Error(w, "sign in at /admin/login", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && !sameOrigin(r) {
			http.Error(w, "cross-site request refused", http.StatusForbidden)
			return
		}
		next(w, r)
	})
}

func sameOrigin(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" { // not a browser form post (curl, tests); the cookie is SameSite=Strict anyway
		return true
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

func (c *Console) loginPage(w http.ResponseWriter, r *http.Request) {
	c.render(w, r, "login", view{Title: "Sign in", Bare: true})
}

func (c *Console) login(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "cross-site request refused", http.StatusForbidden)
		return
	}
	if !c.fails.allow(c.Now()) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		c.render(w, r, "login", view{Title: "Sign in", Bare: true, Error: "Too many attempts. Wait a minute."})
		return
	}
	if subtle.ConstantTimeCompare(hash(r.FormValue("token")), hash(c.Token)) != 1 {
		c.fails.record(c.Now())
		c.Log.Warn("admin sign-in failed", "remote", r.RemoteAddr)
		w.WriteHeader(http.StatusUnauthorized)
		c.render(w, r, "login", view{Title: "Sign in", Bare: true, Error: "That token is not right."})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: cookieName, Value: c.session(), Path: "/admin", MaxAge: int(sessionAge.Seconds()),
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
	})
	http.Redirect(w, r, "/admin/", http.StatusSeeOther)
}

func (c *Console) logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/admin", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
	http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
}

// failures limits sign-in guessing: ten wrong tokens within a minute lock sign-in for the rest of that minute.
type failures struct {
	mu sync.Mutex
	at []time.Time
}

func (f *failures) prune(now time.Time) {
	keep := f.at[:0]
	for _, t := range f.at {
		if now.Sub(t) < time.Minute {
			keep = append(keep, t)
		}
	}
	f.at = keep
}

func (f *failures) allow(now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prune(now)
	return len(f.at) < 10
}

func (f *failures) record(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at = append(f.at, now)
}
