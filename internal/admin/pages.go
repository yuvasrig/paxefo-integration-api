package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/alerts"
	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/worker"
)

var tenantIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,39}$`)

// view is everything a template may show; each page fills the fields it uses.
type view struct {
	Title, Nav   string
	Bare         bool // no navigation (sign-in page)
	Flash, Error string

	Snap    *Snapshot
	Tenants []TenantView

	// created tenant / key, shown once
	NewKey     string
	NewTenant  string
	NewWebhook string

	// rules page
	YAML       string
	Validation string
	Active     rulesInfo
	RuleRows   []ruleRow
	Fields     []rules.Field
	OffRules   int
	History    []store.RulesetRow

	// config page
	Scope   string // "" = global settings, else a tenant id
	Rows    []settingRow
	Restart []ConfigItem
	Refresh int

	// monitoring page
	Links []link

	// payloads page
	PF            payloadFilter
	Ranges        []struct{ Value, Label string }
	StatusOptions []string
	Limits        []int
	Payloads      []payloadView
	NextURL       string
	JSONURL       string
	PayloadEvent  *store.EventRecord
	PaxafeJSON    string
	SensorJSON    string
	LocationJSON  string
	RawJSON       string

	// conflicts page
	TenantID  string
	Conflicts []store.ConflictRow
	CanReplay bool
	Hours     int
	Replay    *ingest.ReplayReport
	Applied   bool
}

type link struct{ Title, URL, Note string }

type rulesInfo struct {
	Version, Source string
	LoadedAt        time.Time
	Rules           int
}

type settingRow struct {
	settings.Def
	Value, Default string // Default: the environment default (global) or the global value (a tenant)
	Overridden     bool   // changed from its default (global) or set by this tenant
	UpdatedBy      string
	UpdatedAt      time.Time
}

// TenantView is one tenant with its configuration and live figures.
type TenantView struct {
	Info    store.TenantInfo
	Mode    string
	Policy  store.ConflictSettings
	Stats   store.TenantStats
	Webhook string
	Keys    []store.APIKey

	Workers    int               // this tenant's workers running now, across all processes
	InFlight   int               // jobs those workers are processing
	Overrides  []string          // the tenant's own settings, "key=value": everything else is the global value
	Prediction worker.Prediction // whether this tenant's workers are keeping up
}

// Snapshot is the live picture the dashboard and the monitoring page share.
type Snapshot struct {
	At                              time.Time
	Tenants                         []TenantView
	Workers                         []store.Heartbeat
	WorkerCount, InFlight           int
	Pending, Processing             int
	Dead                            int
	OldestAge                       time.Duration
	ArrivalsPerMin, CompletedPerMin int
	ActiveDevices                   int
	LatencyP50, LatencyP95          time.Duration
	Prediction                      worker.Prediction
	Limits                          settings.Values
	Rejects                         []RejectRow
	Recent                          []RejectRow
	RulesVersion                    string
	Alerts                          []alerts.Alert
}

// RejectRow is a recent event with its tenant, for the monitoring tables.
type RejectRow struct {
	Tenant string
	store.RecentEvent
}

var funcs = template.FuncMap{
	"dur": func(d time.Duration) string {
		switch {
		case d <= 0:
			return "0s"
		case d < time.Second:
			return fmt.Sprintf("%dms", d.Milliseconds())
		case d < time.Minute:
			return fmt.Sprintf("%.1fs", d.Seconds())
		}
		return worker.FormatETA(d)
	},
	"ago": func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		d := time.Since(t).Round(time.Second)
		if d < 0 {
			d = 0
		}
		return worker.FormatETA(d) + " ago"
	},
	"ts":   func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") + " UTC" },
	"join": strings.Join,
	"num":  func(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) },
}

func (c *Console) render(w http.ResponseWriter, r *http.Request, name string, v view) {
	c.init()
	var buf bytes.Buffer
	if err := c.tmpl.ExecuteTemplate(&buf, name, v); err != nil {
		c.Log.Error("admin render failed", "page", name, "error", err)
		http.Error(w, "could not render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

func (c *Console) fail(w http.ResponseWriter, r *http.Request, what string, err error) {
	c.Log.Error("admin: "+what, "error", err)
	http.Error(w, what+": "+err.Error(), http.StatusInternalServerError)
}

// baseURL is the address operators and Tive should use to reach this service.
func (c *Console) baseURL(r *http.Request) string {
	if c.PublicBaseURL != "" {
		return strings.TrimRight(c.PublicBaseURL, "/")
	}
	if r == nil { // the alert notifier has no request
		return ""
	}
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (c *Console) webhookURL(r *http.Request) string { return c.baseURL(r) + "/api/webhook/tive" }

// ---- live figures ----

// snapshot gathers the figures. It asks the database a few small questions per tenant, which is fine for the number
// of tenants this service has; a much larger fleet would want these precomputed.
func (c *Console) snapshot(ctx context.Context, r *http.Request, withEvents bool) (*Snapshot, error) {
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return nil, err
	}
	s := &Snapshot{At: c.Now(), Limits: c.Settings.Current(), RulesVersion: c.Rules.Current().Version}
	for _, info := range infos {
		tv := TenantView{Info: info, Mode: c.modeOf(info), Webhook: c.webhookURL(r)}
		tv.Policy, _ = store.ParseConflictSettings(info.Config)
		tv.Overrides = overridesOf(info.Config)
		if tv.Stats, err = c.Store.TenantStats(ctx, info.ID); err != nil {
			return nil, err
		}
		st := tv.Stats
		s.Pending += st.Pending
		s.Processing += st.Processing
		s.Dead += st.Dead
		s.ArrivalsPerMin += st.EventsLastMinute
		s.CompletedPerMin += st.CompletedLastMinute
		s.ActiveDevices += st.ActiveDevicesLastMinute
		if st.OldestPendingAge > s.OldestAge {
			s.OldestAge = st.OldestPendingAge
		}
		if st.LatencyP95 > s.LatencyP95 {
			s.LatencyP95 = st.LatencyP95
		}
		if st.LatencyP50 > s.LatencyP50 {
			s.LatencyP50 = st.LatencyP50
		}
		s.Tenants = append(s.Tenants, tv)
		if withEvents {
			rej, err := c.Store.RecentEvents(ctx, info.ID, 10, []string{"rejected", "dead", "failed", "conflict"})
			if err != nil {
				return nil, err
			}
			for _, e := range rej {
				s.Rejects = append(s.Rejects, RejectRow{info.ID, e})
			}
			rec, err := c.Store.RecentEvents(ctx, info.ID, 10, nil)
			if err != nil {
				return nil, err
			}
			for _, e := range rec {
				s.Recent = append(s.Recent, RejectRow{info.ID, e})
			}
		}
	}
	newest := func(rows []RejectRow) {
		sort.Slice(rows, func(i, j int) bool { return rows[i].ReceivedAt.After(rows[j].ReceivedAt) })
	}
	newest(s.Rejects)
	newest(s.Recent)
	if len(s.Rejects) > 15 {
		s.Rejects = s.Rejects[:15]
	}
	if len(s.Recent) > 15 {
		s.Recent = s.Recent[:15]
	}

	beats, err := c.Store.ActiveHeartbeats(ctx, workerLiveWithin)
	if err != nil {
		return nil, err
	}
	perTenant, inFlight := map[string]int{}, map[string]int{}
	for _, h := range beats {
		if h.TenantID == "" { // a process with no tenant pools yet: keep it in the table only
			s.Workers = append(s.Workers, h)
			continue
		}
		s.Workers = append(s.Workers, h)
		perTenant[h.TenantID] += h.Workers
		inFlight[h.TenantID] += h.InFlight
		s.WorkerCount += h.Workers
		s.InFlight += h.InFlight
	}
	lag := time.Duration(s.Limits.LagSLOSeconds) * time.Second
	for i := range s.Tenants {
		tv := &s.Tenants[i]
		tv.Workers, tv.InFlight = perTenant[tv.Info.ID], inFlight[tv.Info.ID]
		tv.Prediction = worker.Predict(worker.PredictInput{
			Pending: tv.Stats.Pending, Processing: tv.Stats.Processing,
			ArrivalsPerMin: float64(tv.Stats.EventsLastMinute), CompletedPerMin: float64(tv.Stats.CompletedLastMinute),
			ActiveWorkers: tv.Workers, OldestAge: tv.Stats.OldestPendingAge, LagSLO: lag,
		})
	}
	s.Prediction = overall(s.Tenants)
	s.Alerts = alerts.Evaluate(c.alertInput(s))
	return s, nil
}

// alertInput turns the live figures into what the alert rules look at. Each tenant is judged by its own settings
// (its overrides on top of the global values).
func (c *Console) alertInput(s *Snapshot) alerts.Input {
	var in alerts.Input
	for _, t := range s.Tenants {
		total := 0
		for _, n := range t.Stats.Statuses15m {
			total += n
		}
		own := c.Settings.For(t.Info.ID)
		in.Tenants = append(in.Tenants, alerts.Tenant{
			ID: t.Info.ID, Async: t.Mode == ingest.ModeAsync, Pending: t.Stats.Pending, OldestWait: t.Stats.OldestPendingAge,
			Dead: t.Stats.Dead, Workers: t.Workers, Rejected15m: t.Stats.Statuses15m["rejected"], Total15m: total,
			LagSLO: time.Duration(own.LagSLOSeconds) * time.Second, QueueLimit: own.QueueMaxDepth, RejectPercent: own.AlertRejectPercent,
		})
	}
	return in
}

// Alerts is the list of current alerts; the notifier polls it.
func (c *Console) Alerts(ctx context.Context) ([]alerts.Alert, error) {
	c.init()
	s, err := c.snapshot(ctx, nil, false)
	if err != nil {
		return nil, err
	}
	return s.Alerts, nil
}

// overridesOf lists a tenant's own settings as "key=value", sorted.
func overridesOf(config []byte) []string {
	var out []string
	for k, v := range settings.TenantOverrides(config) {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// overall summarizes the tenants' predictions: the worst state wins, and the text names the tenant it comes from.
// Every tenant has its own workers, so "are the workers up to date" is a question about each tenant.
func overall(tenants []TenantView) worker.Prediction {
	rank := map[string]int{worker.StateUpToDate: 0, worker.StateCatchingUp: 1, worker.StateFallingBehind: 2, worker.StateStalled: 3}
	var worst *TenantView
	for i := range tenants {
		t := &tenants[i]
		if worst == nil || rank[t.Prediction.State] > rank[worst.Prediction.State] ||
			(t.Prediction.State == worst.Prediction.State && t.Prediction.ETA > worst.Prediction.ETA) {
			worst = t
		}
	}
	if worst == nil {
		return worker.Prediction{State: worker.StateUpToDate, Text: "No tenants yet."}
	}
	p := worst.Prediction
	if p.State == worker.StateUpToDate {
		p.Text = "Every tenant's workers are up to date."
		return p
	}
	p.Text = worst.Info.ID + ": " + p.Text
	return p
}

// modeOf is the tenant's ingest mode: its own setting, else the service default.
func (c *Console) modeOf(info store.TenantInfo) string {
	var cfg struct {
		IngestMode string `json:"ingest_mode"`
	}
	_ = json.Unmarshal(info.Config, &cfg)
	if cfg.IngestMode == ingest.ModeAsync || cfg.IngestMode == ingest.ModeSync {
		return cfg.IngestMode
	}
	if c.DefaultMode == ingest.ModeAsync {
		return ingest.ModeAsync
	}
	return ingest.ModeSync
}

// ---- dashboard and monitoring ----

func (c *Console) dashboard(w http.ResponseWriter, r *http.Request) {
	snap, err := c.snapshot(r.Context(), r, false)
	if err != nil {
		c.fail(w, r, "could not read live figures", err)
		return
	}
	c.render(w, r, "dashboard", view{Title: "Dashboard", Nav: "dashboard", Snap: snap})
}

func (c *Console) monitoring(w http.ResponseWriter, r *http.Request) {
	snap, err := c.snapshot(r.Context(), r, true)
	if err != nil {
		c.fail(w, r, "could not read live figures", err)
		return
	}
	v := view{Title: "Monitoring", Nav: "monitoring", Snap: snap, Links: c.links()}
	c.render(w, r, "monitoring", v)
}

// monitoringLive is the part of the monitoring page that refreshes itself.
func (c *Console) monitoringLive(w http.ResponseWriter, r *http.Request) {
	snap, err := c.snapshot(r.Context(), r, true)
	if err != nil {
		c.fail(w, r, "could not read live figures", err)
		return
	}
	c.render(w, r, "monitoring_live", view{Snap: snap})
}

func (c *Console) links() []link {
	out := []link{
		{"Prometheus metrics", "/metrics", "Raw metrics: request rates and latency, jobs finished, queue depth, workers."},
		{"Readiness", "/readyz", "Database reachable, migration version, ingest mode."},
		{"API reference", "/docs", "OpenAPI contract."},
	}
	if c.LogsURL != "" {
		out = append(out, link{"Logs", c.LogsURL, "The platform's log viewer (structured JSON; filter on request_id or event_id)."})
	}
	if c.DashboardURL != "" {
		out = append(out, link{"Metrics dashboard", c.DashboardURL, "Dashboard built on /metrics."})
	}
	return out
}

// ---- tenants ----

func (c *Console) tenantViews(ctx context.Context, r *http.Request) ([]TenantView, error) {
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return nil, err
	}
	var out []TenantView
	for _, info := range infos {
		tv := TenantView{Info: info, Mode: c.modeOf(info), Webhook: c.webhookURL(r)}
		tv.Policy, _ = store.ParseConflictSettings(info.Config)
		tv.Overrides = overridesOf(info.Config)
		if tv.Keys, err = c.Store.ListAPIKeys(ctx, info.ID); err != nil {
			return nil, err
		}
		out = append(out, tv)
	}
	return out, nil
}

func (c *Console) tenants(w http.ResponseWriter, r *http.Request) {
	c.renderTenants(w, r, view{Flash: r.URL.Query().Get("done")})
}

func (c *Console) renderTenants(w http.ResponseWriter, r *http.Request, v view) {
	tv, err := c.tenantViews(r.Context(), r)
	if err != nil {
		c.fail(w, r, "could not list tenants", err)
		return
	}
	v.Title, v.Nav, v.Tenants = "Tenants", "tenants", tv
	if v.NewKey != "" {
		w.Header().Set("Cache-Control", "no-store")
	}
	c.render(w, r, "tenants", v)
}

// tenantCreate adds a tenant, issues its first API key and shows the webhook setup. The key is shown once.
func (c *Console) tenantCreate(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.FormValue("id"))
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		name = id
	}
	if !tenantIDPattern.MatchString(id) {
		c.renderTenants(w, r, view{Error: "A tenant id is 2-40 characters: lowercase letters, digits and dashes, starting with a letter or digit."})
		return
	}
	if ok, err := c.Store.TenantExists(r.Context(), id); err != nil {
		c.fail(w, r, "could not check the tenant", err)
		return
	} else if ok {
		c.renderTenants(w, r, view{Error: fmt.Sprintf("Tenant %q already exists.", id)})
		return
	}
	cfg := map[string]any{}
	if m := r.FormValue("mode"); m == ingest.ModeAsync || m == ingest.ModeSync {
		cfg["ingest_mode"] = m
	}
	if err := c.Store.CreateTenant(r.Context(), id, name, cfg); err != nil {
		c.fail(w, r, "could not create the tenant", err)
		return
	}
	key, err := c.issueKey(r.Context(), id, "console")
	if err != nil {
		c.fail(w, r, "could not issue a key", err)
		return
	}
	c.Log.Info("admin: tenant created", "tenant_id", id)
	c.renderTenants(w, r, view{Flash: fmt.Sprintf("Tenant %q created.", id), NewKey: key, NewTenant: id, NewWebhook: c.webhookURL(r)})
}

func (c *Console) issueKey(ctx context.Context, tenant, name string) (string, error) {
	k, err := auth.NewKey()
	if err != nil {
		return "", err
	}
	if err := c.Store.CreateAPIKey(ctx, tenant, name, k.Prefix, k.Hash, nil); err != nil {
		return "", err
	}
	return k.Plaintext, nil
}

func (c *Console) keyCreate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ok, err := c.Store.TenantExists(r.Context(), id); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		name = "console"
	}
	key, err := c.issueKey(r.Context(), id, name)
	if err != nil {
		c.fail(w, r, "could not issue a key", err)
		return
	}
	c.Log.Info("admin: key issued", "tenant_id", id)
	c.renderTenants(w, r, view{Flash: fmt.Sprintf("New key issued for %q.", id), NewKey: key, NewTenant: id, NewWebhook: c.webhookURL(r)})
}

func (c *Console) keyRevoke(w http.ResponseWriter, r *http.Request) {
	kid, err := strconv.ParseInt(r.PathValue("key"), 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if err := c.Store.RevokeAPIKey(r.Context(), kid); err != nil {
		c.renderTenants(w, r, view{Error: "Could not revoke the key: " + err.Error()})
		return
	}
	c.Log.Info("admin: key revoked", "tenant_id", r.PathValue("id"), "key_id", kid)
	http.Redirect(w, r, "/admin/tenants?done="+urlText("Key revoked."), http.StatusSeeOther)
}

// tenantSettings sets a tenant's ingest mode and conflict handling.
func (c *Console) tenantSettings(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if ok, err := c.Store.TenantExists(r.Context(), id); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	mode := r.FormValue("mode")
	if mode != ingest.ModeSync && mode != ingest.ModeAsync {
		c.renderTenants(w, r, view{Error: "Ingest mode must be sync or async."})
		return
	}
	status, _ := strconv.Atoi(r.FormValue("http_status"))
	cs, err := store.NewConflictSettings(r.FormValue("policy"), status)
	if err != nil {
		c.renderTenants(w, r, view{Error: err.Error()})
		return
	}
	if err := c.Store.SetTenantIngestMode(r.Context(), id, mode); err != nil {
		c.fail(w, r, "could not set the ingest mode", err)
		return
	}
	if err := c.Store.SetConflictSettings(r.Context(), id, cs); err != nil {
		c.fail(w, r, "could not set the conflict policy", err)
		return
	}
	c.Log.Info("admin: tenant settings changed", "tenant_id", id, "ingest_mode", mode, "conflict_policy", cs.Policy)
	http.Redirect(w, r, "/admin/tenants?done="+urlText("Saved "+id+". Ingest mode changes reach every instance within a few seconds."), http.StatusSeeOther)
}

// ---- rules ----

func (c *Console) rulesValidate(w http.ResponseWriter, r *http.Request) {
	yaml := r.FormValue("yaml")
	v := view{}
	if rs, err := rules.Parse([]byte(yaml)); err != nil {
		v.Error = "Not valid, nothing changed: " + err.Error()
	} else {
		v.Validation = fmt.Sprintf("Valid. Version %q with %d rules. Publish it to make it live.", rs.Version, len(rs.Rules))
	}
	v, err := c.rulesView(r.Context(), yaml, v)
	if err != nil {
		c.fail(w, r, "could not read rules", err)
		return
	}
	c.render(w, r, "rules", v)
}

func (c *Console) rulesPublish(w http.ResponseWriter, r *http.Request) {
	yaml := r.FormValue("yaml")
	rs, err := rules.Parse([]byte(yaml))
	if err != nil {
		v, verr := c.rulesView(r.Context(), yaml, view{Error: "Not valid, nothing published: " + err.Error()})
		if verr != nil {
			c.fail(w, r, "could not read rules", verr)
			return
		}
		c.render(w, r, "rules", v)
		return
	}
	if _, err := c.Store.PublishRuleset(r.Context(), rs.Version, yaml, "admin-console"); err != nil {
		msg := "Could not publish: " + err.Error()
		if errors.Is(err, store.ErrVersionExists) {
			msg = fmt.Sprintf("Version %q was published before. Change `version:` at the top: it is recorded on every event, so a changed ruleset needs a new version.", rs.Version)
		}
		v, verr := c.rulesView(r.Context(), yaml, view{Error: msg})
		if verr != nil {
			c.fail(w, r, "could not read rules", verr)
			return
		}
		c.render(w, r, "rules", v)
		return
	}
	c.reloadRules(r.Context())
	c.Log.Info("admin: ruleset published", "version", rs.Version)
	http.Redirect(w, r, "/admin/rules?done="+urlText(fmt.Sprintf("Published %s. Every instance adopts it within %d seconds; new events record this version.", rs.Version, c.refresh())), http.StatusSeeOther)
}

func (c *Console) rulesReset(w http.ResponseWriter, r *http.Request) {
	if _, err := c.Store.ClearActiveRuleset(r.Context()); err != nil {
		c.fail(w, r, "could not reset the rules", err)
		return
	}
	c.reloadRules(r.Context())
	c.Log.Info("admin: ruleset reset to the base")
	http.Redirect(w, r, "/admin/rules?done="+urlText("Reset: the built-in default rules are in force again."), http.StatusSeeOther)
}

func (c *Console) reloadRules(ctx context.Context) {
	if c.ReloadRules != nil {
		if err := c.ReloadRules(ctx); err != nil {
			c.Log.Error("admin: could not apply rules at once; the next refresh will", "error", err)
		}
	}
}

func (c *Console) refresh() int {
	if c.RefreshSeconds > 0 {
		return c.RefreshSeconds
	}
	return 10
}

func urlText(s string) string { return url.QueryEscape(s) }

// ---- config ----

// configView builds the Config page for a scope: the global settings (scope ""), or one tenant's, where each value
// is the tenant's own override or, failing that, the global value.
func (c *Console) configView(ctx context.Context, scope string, v view) (view, error) {
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return v, err
	}
	var info *store.TenantInfo
	for i := range infos {
		v.Tenants = append(v.Tenants, TenantView{Info: infos[i]})
		if infos[i].ID == scope {
			info = &infos[i]
		}
	}
	if scope != "" && info == nil {
		return v, errUnknownTenant
	}
	v.Scope = scope
	if info == nil {
		overrides, err := c.Store.ListSettings(ctx)
		if err != nil {
			return v, err
		}
		byKey := map[string]store.SettingRow{}
		for _, o := range overrides {
			byKey[o.Key] = o
		}
		cur := c.Settings.Current()
		for _, d := range settings.Defs() {
			row := settingRow{Def: d, Value: cur.Get(d.Key), Default: c.Defaults.Get(d.Key)}
			if o, ok := byKey[d.Key]; ok {
				row.Overridden, row.UpdatedBy, row.UpdatedAt = true, o.UpdatedBy, o.UpdatedAt
			}
			v.Rows = append(v.Rows, row)
		}
	} else {
		own := settings.TenantOverrides(info.Config)
		effective, global := c.Settings.For(scope), c.Settings.Current()
		for _, d := range settings.TenantDefs() {
			_, mine := own[d.Key]
			v.Rows = append(v.Rows, settingRow{Def: d, Value: effective.Get(d.Key), Default: global.Get(d.Key), Overridden: mine})
		}
	}
	v.Title, v.Nav, v.Restart, v.Refresh = "Config", "config", c.Restart, c.refresh()
	return v, nil
}

func (c *Console) configPage(w http.ResponseWriter, r *http.Request) {
	v, err := c.configView(r.Context(), r.URL.Query().Get("tenant"), view{Flash: r.URL.Query().Get("done")})
	if errors.Is(err, errUnknownTenant) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		c.fail(w, r, "could not read settings", err)
		return
	}
	c.render(w, r, "config", v)
}

// configURL is where to return after a change: the same scope.
func configURL(scope, done string) string {
	q := "done=" + urlText(done)
	if scope != "" {
		q = "tenant=" + url.QueryEscape(scope) + "&" + q
	}
	return "/admin/config?" + q
}

// configSet saves a setting globally, or (with a tenant) as that tenant's own override. A tenant's value is validated
// against the settings it would have with the change, so it can not, for example, put its minimum above its maximum.
func (c *Console) configSet(w http.ResponseWriter, r *http.Request) {
	scope, key, raw := r.FormValue("tenant"), r.FormValue("key"), strings.TrimSpace(r.FormValue("value"))
	base := c.Settings.Current()
	if scope != "" {
		if settings.GlobalOnly(key) {
			c.configError(w, r, scope, "Not saved: "+key+" applies to the whole process and cannot be set per tenant.")
			return
		}
		if ok, err := c.Store.TenantExists(r.Context(), scope); err != nil || !ok {
			http.NotFound(w, r)
			return
		}
		base = c.Settings.For(scope)
	}
	if err := settings.Validate(base, key, raw); err != nil {
		c.configError(w, r, scope, "Not saved: "+err.Error())
		return
	}
	var err error
	if scope == "" {
		err = c.Store.SetSetting(r.Context(), key, raw, "admin-console")
	} else {
		err = c.Store.SetTenantSetting(r.Context(), scope, key, raw)
	}
	if err != nil {
		c.fail(w, r, "could not save the setting", err)
		return
	}
	c.reloadSettings(r.Context())
	c.Log.Info("admin: setting changed", "tenant_id", scope, "key", key, "value", raw)
	who := "globally"
	if scope != "" {
		who = "for " + scope
	}
	http.Redirect(w, r, configURL(scope, fmt.Sprintf("Saved %s = %s %s. Every instance applies it within %d seconds.", key, raw, who, c.refresh())), http.StatusSeeOther)
}

func (c *Console) configError(w http.ResponseWriter, r *http.Request, scope, msg string) {
	v, err := c.configView(r.Context(), scope, view{Error: msg})
	if err != nil {
		c.fail(w, r, "could not read settings", err)
		return
	}
	c.render(w, r, "config", v)
}

// configReset removes the global override (back to the environment default) or, for a tenant, its own override (back
// to the global value).
func (c *Console) configReset(w http.ResponseWriter, r *http.Request) {
	scope, key := r.FormValue("tenant"), r.FormValue("key")
	if _, ok := settings.Lookup(key); !ok {
		http.NotFound(w, r)
		return
	}
	var err error
	msg := "Reset " + key + " to its default."
	if scope == "" {
		err = c.Store.ResetSetting(r.Context(), key)
	} else {
		if err = c.Store.ClearTenantSetting(r.Context(), scope, key); errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		msg = scope + " now uses the global value of " + key + "."
	}
	if err != nil {
		c.fail(w, r, "could not reset the setting", err)
		return
	}
	c.reloadSettings(r.Context())
	c.Log.Info("admin: setting reset", "tenant_id", scope, "key", key)
	http.Redirect(w, r, configURL(scope, msg), http.StatusSeeOther)
}

func (c *Console) reloadSettings(ctx context.Context) {
	if c.ReloadSettings != nil {
		if err := c.ReloadSettings(ctx); err != nil {
			c.Log.Error("admin: could not apply settings at once; the next refresh will", "error", err)
		}
	}
}

// ---- conflicts ----

const defaultReplayHours = 24

// conflictsView lists a tenant's conflicts, and the tenant picker.
func (c *Console) conflictsView(ctx context.Context, r *http.Request, tenant string, v view) (view, error) {
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return v, err
	}
	for _, i := range infos {
		v.Tenants = append(v.Tenants, TenantView{Info: i})
	}
	if tenant == "" && len(infos) > 0 {
		tenant = infos[0].ID
	}
	v.Title, v.Nav, v.TenantID, v.CanReplay = "Conflicts", "conflicts", tenant, c.Replayer != nil
	if v.Hours == 0 {
		v.Hours = defaultReplayHours
	}
	if tenant != "" {
		if ok, err := c.Store.TenantExists(ctx, tenant); err != nil {
			return v, err
		} else if !ok {
			return v, errUnknownTenant
		}
		if v.Conflicts, err = c.Store.ListConflicts(ctx, tenant, 25); err != nil {
			return v, err
		}
	}
	return v, nil
}

var errUnknownTenant = errors.New("unknown tenant")

func (c *Console) conflictsPage(w http.ResponseWriter, r *http.Request) {
	v, err := c.conflictsView(r.Context(), r, r.URL.Query().Get("tenant"), view{Flash: r.URL.Query().Get("done")})
	if errors.Is(err, errUnknownTenant) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		c.fail(w, r, "could not read conflicts", err)
		return
	}
	c.render(w, r, "conflicts", v)
}

// conflictsReplay reprocesses a tenant's conflicting events from the last N hours with the rules and conflict policy
// in force now: a preview (dry run, nothing written) unless "apply" is chosen. Typical use: change the tenant's
// conflict policy on the Tenants page, preview here, then apply.
func (c *Console) conflictsReplay(w http.ResponseWriter, r *http.Request) {
	tenant := r.FormValue("tenant")
	hours, _ := strconv.Atoi(r.FormValue("hours"))
	if hours < 1 || hours > 24*90 {
		hours = defaultReplayHours
	}
	apply := r.FormValue("mode") == "apply"
	if c.Replayer == nil {
		http.NotFound(w, r)
		return
	}
	if ok, err := c.Store.TenantExists(r.Context(), tenant); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	now := c.Now()
	report, err := c.Replayer.ReplayWindow(r.Context(), ingest.ReplayOptions{
		Filter: store.ReplayFilter{TenantID: tenant, From: now.Add(-time.Duration(hours) * time.Hour), To: now.Add(time.Minute), Statuses: []string{"conflict"}},
		Apply:  apply, RunID: store.NewID("run_"), By: "admin-console",
	})
	if err != nil {
		c.fail(w, r, "replay failed", err)
		return
	}
	c.Log.Info("admin: conflict replay", "tenant_id", tenant, "apply", apply, "selected", report.Selected, "changed", report.Changed)
	v, err := c.conflictsView(r.Context(), r, tenant, view{Hours: hours, Replay: &report, Applied: apply})
	if err != nil {
		c.fail(w, r, "could not read conflicts", err)
		return
	}
	c.render(w, r, "conflicts", v)
}
