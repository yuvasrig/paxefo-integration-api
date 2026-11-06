package config

import (
	"fmt"
	"strconv"
	"time"
)

// Server is the process configuration, read from the environment only (DESIGN.md section 15.2).
type Server struct {
	Port            string
	DatabaseURL     string
	LogLevel        string
	IngestMode      string
	Role            string
	BodyLimitBytes  int64
	RateLimitPerSec float64
	RateLimitBurst  int
	DBMaxConns      int32
	QueryTimeout    time.Duration
	RulesPath       string
	// RulesRefresh is how often a running instance checks for a ruleset published with `api rules set`.
	// 0 turns the check off (rules then change only at restart).
	RulesRefresh time.Duration

	// Queue and worker defaults (async ingest). Each can be overridden at runtime from the admin console (package settings).
	QueueMaxDepth int
	WorkersMin    int
	WorkersMax    int
	WorkersTotal  int // hard cap on workers per process across all tenants (0 = no cap)
	JobsPerWorker int
	LagSLOSeconds int
	MaxAttempts   int
	// SettingsRefresh is how often a running instance checks for changed runtime settings (0 turns it off).
	SettingsRefresh time.Duration

	// Admin console (served by the api role): disabled unless AdminToken is set.
	AdminToken    string
	PublicBaseURL string // shown in webhook setup instructions; defaults to the request's own address
	LogsURL       string // link to the platform's log viewer (for example the Render logs page)
	DashboardURL  string // link to a metrics dashboard (for example Grafana)
}

// FromEnv reads and validates the configuration; getenv is os.Getenv in production.
func FromEnv(getenv func(string) string) (Server, error) {
	var err error
	str := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	num := func(key string, def float64) float64 {
		v := getenv(key)
		if v == "" || err != nil {
			return def
		}
		f, perr := strconv.ParseFloat(v, 64)
		if perr != nil || f <= 0 {
			err = fmt.Errorf("config: %s must be a positive number (got %q)", key, v)
		}
		return f
	}

	s := Server{
		Port:            str("PORT", "8080"),
		DatabaseURL:     getenv("DATABASE_URL"),
		LogLevel:        str("LOG_LEVEL", "info"),
		IngestMode:      str("INGEST_MODE", "sync"),
		Role:            str("ROLE", "all"),
		BodyLimitBytes:  int64(num("BODY_LIMIT_BYTES", 256<<10)),
		RateLimitPerSec: num("RATE_LIMIT_PER_SEC", 100),
		RateLimitBurst:  int(num("RATE_LIMIT_BURST", 200)),
		DBMaxConns:      int32(num("DB_MAX_CONNS", 10)),
		RulesPath:       getenv("RULES_PATH"),
		QueueMaxDepth:   int(num("QUEUE_MAX_DEPTH", 5000)),
		WorkersMin:      -1,
		WorkersMax:      int(num("WORKERS_MAX", 8)),
		WorkersTotal:    int(num("WORKERS_TOTAL_MAX", 0)),
		JobsPerWorker:   int(num("JOBS_PER_WORKER", 50)),
		LagSLOSeconds:   int(num("LAG_SLO_SECONDS", 10)),
		MaxAttempts:     int(num("MAX_ATTEMPTS", 5)),
		AdminToken:      getenv("ADMIN_TOKEN"),
		PublicBaseURL:   getenv("PUBLIC_BASE_URL"),
		LogsURL:         getenv("LOGS_URL"),
		DashboardURL:    getenv("DASHBOARD_URL"),
	}
	// WORKERS_MIN may be 0 (scale to zero), which num() does not allow.
	s.WorkersMin = 2
	if v := getenv("WORKERS_MIN"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 {
			return Server{}, fmt.Errorf("config: WORKERS_MIN must be 0 or more (got %q)", v)
		}
		s.WorkersMin = n
	}
	if err != nil {
		return Server{}, err
	}
	s.QueryTimeout = 5 * time.Second
	if v := getenv("DB_QUERY_TIMEOUT"); v != "" {
		d, perr := time.ParseDuration(v)
		if perr != nil || d <= 0 {
			return Server{}, fmt.Errorf("config: DB_QUERY_TIMEOUT must be a positive duration (got %q)", v)
		}
		s.QueryTimeout = d
	}

	s.RulesRefresh = 10 * time.Second
	if v := getenv("RULES_REFRESH_INTERVAL"); v != "" {
		if v == "0" {
			s.RulesRefresh = 0
		} else {
			d, perr := time.ParseDuration(v)
			if perr != nil || d <= 0 {
				return Server{}, fmt.Errorf("config: RULES_REFRESH_INTERVAL must be a positive duration or 0 to disable (got %q)", v)
			}
			s.RulesRefresh = d
		}
	}

	if s.DatabaseURL == "" {
		return Server{}, fmt.Errorf("config: DATABASE_URL is required")
	}
	if n, perr := strconv.Atoi(s.Port); perr != nil || n < 1 || n > 65535 {
		return Server{}, fmt.Errorf("config: PORT must be 1-65535 (got %q)", s.Port)
	}
	switch s.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Server{}, fmt.Errorf("config: LOG_LEVEL must be debug, info, warn or error (got %q)", s.LogLevel)
	}
	if s.IngestMode != "sync" && s.IngestMode != "async" {
		return Server{}, fmt.Errorf("config: INGEST_MODE must be sync or async (got %q)", s.IngestMode)
	}
	if s.Role != "all" && s.Role != "api" && s.Role != "worker" {
		return Server{}, fmt.Errorf("config: ROLE must be all, api or worker (got %q)", s.Role)
	}
	if s.WorkersMin > s.WorkersMax {
		return Server{}, fmt.Errorf("config: WORKERS_MIN (%d) cannot be more than WORKERS_MAX (%d)", s.WorkersMin, s.WorkersMax)
	}
	s.SettingsRefresh = 10 * time.Second
	if v := getenv("SETTINGS_REFRESH_INTERVAL"); v != "" {
		if v == "0" {
			s.SettingsRefresh = 0
		} else {
			d, perr := time.ParseDuration(v)
			if perr != nil || d <= 0 {
				return Server{}, fmt.Errorf("config: SETTINGS_REFRESH_INTERVAL must be a positive duration or 0 to disable (got %q)", v)
			}
			s.SettingsRefresh = d
		}
	}
	return s, nil
}
