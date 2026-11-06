// Command api is the PAXAFE Tive integration service and its operator CLI.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/admin"
	"github.com/yuvasrig/paxefo-integration-api/internal/alerts"
	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/observability"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/rulesync"
	"github.com/yuvasrig/paxefo-integration-api/internal/settings"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
	"github.com/yuvasrig/paxefo-integration-api/internal/worker"
)

const shutdownTimeout = 10 * time.Second // F13-R5

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	cmd, rest := "serve", args
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, rest = args[0], args[1:]
	}
	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		fmt.Fprint(stdout, adminUsage)
		return 0
	}

	if cmd == "rules" {
		// `rules default` and `rules validate` need no database, so they work without any configuration.
		if handled, err := runRulesOffline(rest, stdout); handled {
			if err != nil {
				fmt.Fprintln(stderr, err)
				return 1
			}
			return 0
		}
	}

	cfg, err := config.FromEnv(os.Getenv)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	log := observability.NewLogger(cfg.LogLevel, stdout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "serve":
		ln, err := net.Listen("tcp", ":"+cfg.Port)
		if err != nil {
			fmt.Fprintln(stderr, "listen:", err)
			return 1
		}
		if err := serve(ctx, ln, cfg, log); err != nil {
			log.Error("server stopped", "error", err)
			return 1
		}
		return 0
	case "seed", "keys", "tenants", "rules", "replay":
		st, err := openStore(ctx, cfg)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		defer st.Close()
		if err := runAdmin(ctx, st, cmd, rest, stdout); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stderr, "unknown command %q\n%s", cmd, adminUsage)
	return 2
}

func openStore(ctx context.Context, cfg config.Server) (*store.PGStore, error) {
	st, err := store.Open(ctx, cfg.DatabaseURL, store.Options{MaxConns: cfg.DBMaxConns, QueryTimeout: cfg.QueryTimeout})
	if err != nil {
		return nil, err
	}
	if err := st.Migrate(ctx); err != nil { // F13-R1
		st.Close()
		return nil, err
	}
	return st, nil
}

func loadRules(path string) (*rules.Ruleset, error) {
	if path == "" {
		return rules.Default()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read RULES_PATH: %w", err)
	}
	return rules.Parse(data)
}

// serve runs the HTTP server on ln until ctx is cancelled, then drains in-flight requests and closes the
// database pool (F13-R1, F13-R5).
func serve(ctx context.Context, ln net.Listener, cfg config.Server, log *slog.Logger) error {
	base, err := loadRules(cfg.RulesPath) // F13-R2: an invalid base ruleset stops the process before serving
	if err != nil {
		return fmt.Errorf("load ruleset: %w", err)
	}
	baseSource := rules.SourceDefault
	if cfg.RulesPath != "" {
		baseSource = rules.SourceFile
	}
	st, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer st.Close()

	metrics := observability.NewMetrics(st.Pool().Stat)

	// The ruleset in force: one published with `api rules set` if there is one, else the base (RULES_PATH or the
	// embedded default). It is swapped while running, so rules can be changed on the fly (see package rulesync).
	holder := rules.NewHolder(base, baseSource, time.Now())
	reloader := &rulesync.Reloader{
		Source: st, Holder: holder, Base: base, BaseSource: baseSource,
		Interval: cfg.RulesRefresh, Log: log, Observer: metrics,
	}
	metrics.RulesActive(base.Version, baseSource)
	if err := reloader.Refresh(ctx); err != nil { // F13-R2: an invalid published ruleset stops startup
		return fmt.Errorf("load ruleset: %w", err)
	}
	go reloader.Run(ctx)
	rs := holder.Current()

	// Runtime settings: environment variables are the defaults; values changed in the admin console override them.
	defaults := settings.Values{
		QueueMaxDepth: cfg.QueueMaxDepth,
		WorkersMin:    cfg.WorkersMin, WorkersMax: cfg.WorkersMax, WorkersTotalMax: cfg.WorkersTotal, JobsPerWorker: cfg.JobsPerWorker,
		LagSLOSeconds: cfg.LagSLOSeconds, MaxAttempts: cfg.MaxAttempts, AlertRejectPercent: cfg.AlertRejectPercent,
	}
	live := settings.NewHolder(defaults)
	syncer := &settings.Syncer{Source: st, Holder: live, Base: defaults, Interval: cfg.SettingsRefresh, Log: log}
	if err := syncer.Refresh(ctx); err != nil {
		return fmt.Errorf("load runtime settings: %w", err)
	}
	go syncer.Run(ctx)

	svc := &ingest.Service{
		Store:       st,
		Provider:    tive.NewDynamic(holder, config.Default()),
		Now:         time.Now,
		Observer:    metrics,
		Log:         log,
		DefaultMode: cfg.IngestMode,
		Settings:    live,
	}
	api := &httpapi.API{
		Ingest:        svc,
		Authenticator: &auth.Authenticator{Keys: st, Now: time.Now},
		BodyLimit:     cfg.BodyLimitBytes,
		Log:           log,
		Rules:         holder,
		Metrics:       metrics.Handler(),
		Observer:      metrics,
		WorkerOnly:    cfg.Role == "worker",
		Ready: func(c context.Context) (httpapi.ReadyInfo, error) {
			if err := st.Ping(c); err != nil {
				return httpapi.ReadyInfo{}, err
			}
			v, err := st.MigrationVersion(c)
			if err != nil {
				return httpapi.ReadyInfo{}, err
			}
			return httpapi.ReadyInfo{MigrationVersion: v, StorageBackend: "postgres", IngestMode: cfg.IngestMode, Role: cfg.Role}, nil
		},
	}

	if cfg.Role != "worker" {
		console := &admin.Console{
			Store: st, Token: cfg.AdminToken, Settings: live, Defaults: defaults, Rules: holder,
			ReloadRules: reloader.Refresh, ReloadSettings: syncer.Refresh, Replayer: svc, DefaultMode: cfg.IngestMode,
			Restart: restartConfig(cfg), PublicBaseURL: cfg.PublicBaseURL, LogsURL: cfg.LogsURL, DashboardURL: cfg.DashboardURL,
			RefreshSeconds: int(cfg.SettingsRefresh.Seconds()), Log: log,
		}
		if cfg.AdminToken != "" {
			api.Admin = console.Handler()
			log.Info("admin console enabled at /admin")
		}
		// Alerts: shown in the console, and posted to ALERT_WEBHOOK_URL when one starts or clears.
		if cfg.AlertWebhookURL != "" {
			go (&alerts.Notifier{Source: console.Alerts, URL: cfg.AlertWebhookURL, Log: log}).Run(ctx)
			log.Info("alert notifications enabled")
		}
	}

	// Workers (roles all and worker): claim queued payloads and process them like a synchronous request would.
	poolDone := make(chan struct{})
	if cfg.Role == "all" || cfg.Role == "worker" {
		host, _ := os.Hostname()
		// One pool of workers per tenant, each sized from that tenant's own backlog.
		workers := &worker.Manager{
			Source: st, Processor: svc, Settings: live, Role: cfg.Role, DefaultMode: cfg.IngestMode, Log: log, Observer: metrics,
			Instance: fmt.Sprintf("%s-%d-%s", host, os.Getpid(), store.NewID("")[20:]),
		}
		go func() { workers.Run(ctx); close(poolDone) }()
	} else {
		close(poolDone)
	}

	srv := &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", ln.Addr().String(), "role", cfg.Role, "ingest_mode", cfg.IngestMode, "rules_version", rs.Version, "rules_source", holder.Loaded().Source, "rules_refresh", cfg.RulesRefresh.String())
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-ctx.Done():
		log.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		select { // workers finish the jobs they already claimed before the database closes
		case <-poolDone:
		case <-shutCtx.Done():
		}
		return nil
	}
}

// restartConfig lists the settings that need a restart, for the admin console's config page. Secrets are left out.
func restartConfig(cfg config.Server) []admin.ConfigItem {
	ms := func(d time.Duration) string { return d.String() }
	return []admin.ConfigItem{
		{Name: "PORT", Value: cfg.Port, Note: "Port the server listens on."},
		{Name: "ROLE", Value: cfg.Role, Note: "all = API and workers in one process; api = webhooks only; worker = workers only."},
		{Name: "INGEST_MODE", Value: cfg.IngestMode, Note: "Default for tenants with no mode of their own."},
		{Name: "DATABASE_URL", Value: "(set)", Note: "Postgres connection. Never shown."},
		{Name: "DB_MAX_CONNS", Value: fmt.Sprint(cfg.DBMaxConns), Note: "Database connection pool size; keep workers x instances within it."},
		{Name: "QUERY_TIMEOUT", Value: ms(cfg.QueryTimeout), Note: "Per-query timeout."},
		{Name: "BODY_LIMIT_BYTES", Value: fmt.Sprint(cfg.BodyLimitBytes), Note: "Largest accepted webhook body."},
		{Name: "RULES_PATH", Value: orNone(cfg.RulesPath), Note: "Base ruleset file; a ruleset published from the console overrides it."},
		{Name: "RULES_REFRESH_INTERVAL", Value: ms(cfg.RulesRefresh), Note: "How often each instance checks for a newly published ruleset."},
		{Name: "SETTINGS_REFRESH_INTERVAL", Value: ms(cfg.SettingsRefresh), Note: "How often each instance checks for changed live settings."},
		{Name: "LOG_LEVEL", Value: cfg.LogLevel, Note: "debug, info, warn or error."},
		{Name: "PUBLIC_BASE_URL", Value: orNone(cfg.PublicBaseURL), Note: "Address shown in webhook setup instructions."},
		{Name: "LOGS_URL / DASHBOARD_URL", Value: orNone(cfg.LogsURL) + " / " + orNone(cfg.DashboardURL), Note: "Links on the monitoring page."},
		{Name: "ALERT_WEBHOOK_URL", Value: setOrNot(cfg.AlertWebhookURL), Note: "Never shown (it can hold a secret). Where alerts are posted when they start and clear, for example a Slack incoming webhook. Alerts always show in this console."},
		{Name: "ADMIN_TOKEN", Value: "(set)", Note: "Enables this console. Never shown."},
	}
}

func orNone(s string) string {
	if s == "" {
		return "(not set)"
	}
	return s
}

// setOrNot reports whether a secret value is configured without showing it.
func setOrNot(s string) string {
	if s == "" {
		return "(not set)"
	}
	return "(set)"
}
