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

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
	"github.com/yuvasrig/paxefo-integration-api/internal/config"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi"
	"github.com/yuvasrig/paxefo-integration-api/internal/httpapi/middleware"
	"github.com/yuvasrig/paxefo-integration-api/internal/ingest"
	"github.com/yuvasrig/paxefo-integration-api/internal/observability"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider/tive"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/rulesync"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
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

	svc := &ingest.Service{
		Store:    st,
		Provider: tive.NewDynamic(holder, config.Default()),
		Now:      time.Now,
		Observer: metrics,
		Log:      log,
	}
	api := &httpapi.API{
		Ingest:        svc,
		Authenticator: &auth.Authenticator{Keys: st, Now: time.Now},
		Limiter:       &middleware.RateLimiter{Rate: cfg.RateLimitPerSec, Burst: cfg.RateLimitBurst, Now: time.Now},
		BodyLimit:     cfg.BodyLimitBytes,
		Log:           log,
		Rules:         holder,
		Metrics:       metrics.Handler(),
		Observer:      metrics,
		Ready: func(c context.Context) (httpapi.ReadyInfo, error) {
			if err := st.Ping(c); err != nil {
				return httpapi.ReadyInfo{}, err
			}
			v, err := st.MigrationVersion(c)
			if err != nil {
				return httpapi.ReadyInfo{}, err
			}
			return httpapi.ReadyInfo{MigrationVersion: v, StorageBackend: "postgres", IngestMode: cfg.IngestMode}, nil
		},
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
		log.Info("listening", "addr", ln.Addr().String(), "ingest_mode", cfg.IngestMode, "rules_version", rs.Version, "rules_source", holder.Loaded().Source, "rules_refresh", cfg.RulesRefresh.String())
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
		return nil
	}
}
