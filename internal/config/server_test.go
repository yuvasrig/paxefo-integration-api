package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestFromEnv(t *testing.T) {
	s, err := FromEnv(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if s.Port != "8080" || s.LogLevel != "info" || s.IngestMode != "sync" || s.Role != "all" ||
		s.BodyLimitBytes != 256<<10 ||
		s.DBMaxConns != 10 || s.QueryTimeout != 5*time.Second || s.RulesRefresh != 10*time.Second {
		t.Fatalf("unexpected defaults: %+v", s)
	}

	s, err = FromEnv(env(map[string]string{
		"DATABASE_URL": "postgres://x", "PORT": "10000", "LOG_LEVEL": "debug", "BODY_LIMIT_BYTES": "1024",
		"DB_MAX_CONNS": "3", "DB_QUERY_TIMEOUT": "750ms", "RULES_PATH": "/r.yaml", "RULES_REFRESH_INTERVAL": "2s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.Port != "10000" || s.LogLevel != "debug" || s.BodyLimitBytes != 1024 ||
		s.DBMaxConns != 3 || s.QueryTimeout != 750*time.Millisecond || s.RulesPath != "/r.yaml" || s.RulesRefresh != 2*time.Second {
		t.Fatalf("unexpected overrides: %+v", s)
	}
}

// F13-R2: invalid configuration fails before serving.
func TestFromEnvErrors_F13R2(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "postgres://x"}
	tests := []struct {
		name string
		set  map[string]string
		want string
	}{
		{"no database", map[string]string{}, "DATABASE_URL"},
		{"bad mode", map[string]string{"INGEST_MODE": "batch"}, "INGEST_MODE"},
		{"unknown role", map[string]string{"ROLE": "scheduler"}, "ROLE"},
		{"min above max", map[string]string{"WORKERS_MIN": "9", "WORKERS_MAX": "4"}, "WORKERS_MIN"},
		{"bad min", map[string]string{"WORKERS_MIN": "-1"}, "WORKERS_MIN"},
		{"bad settings refresh", map[string]string{"SETTINGS_REFRESH_INTERVAL": "soon"}, "SETTINGS_REFRESH_INTERVAL"},
		{"bad port", map[string]string{"PORT": "abc"}, "PORT"},
		{"bad level", map[string]string{"LOG_LEVEL": "loud"}, "LOG_LEVEL"},
		{"bad body limit", map[string]string{"BODY_LIMIT_BYTES": "0"}, "BODY_LIMIT_BYTES"},
		{"bad timeout", map[string]string{"DB_QUERY_TIMEOUT": "soon"}, "DB_QUERY_TIMEOUT"},
		{"bad refresh", map[string]string{"RULES_REFRESH_INTERVAL": "-1s"}, "RULES_REFRESH_INTERVAL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := map[string]string{}
			if tt.name != "no database" {
				for k, v := range base {
					m[k] = v
				}
			}
			for k, v := range tt.set {
				m[k] = v
			}
			_, err := FromEnv(env(m))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %s", err, tt.want)
			}
		})
	}
}

func TestFromEnvAsyncAndWorkers(t *testing.T) {
	s, err := FromEnv(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil || s.QueueMaxDepth != 5000 || s.WorkersMin != 2 || s.WorkersMax != 8 || s.JobsPerWorker != 50 || s.LagSLOSeconds != 10 || s.MaxAttempts != 5 || s.SettingsRefresh != 10*time.Second || s.AdminToken != "" {
		t.Fatalf("defaults: %+v %v", s, err)
	}
	s, err = FromEnv(env(map[string]string{
		"DATABASE_URL": "postgres://x", "INGEST_MODE": "async", "ROLE": "worker", "WORKERS_MIN": "0", "WORKERS_MAX": "3", "QUEUE_MAX_DEPTH": "10",
		"JOBS_PER_WORKER": "5", "LAG_SLO_SECONDS": "2", "MAX_ATTEMPTS": "3", "SETTINGS_REFRESH_INTERVAL": "0", "ADMIN_TOKEN": "t", "PUBLIC_BASE_URL": "https://x", "LOGS_URL": "https://logs", "DASHBOARD_URL": "https://dash",
	}))
	if err != nil || s.IngestMode != "async" || s.Role != "worker" || s.WorkersMin != 0 || s.WorkersMax != 3 || s.QueueMaxDepth != 10 || s.JobsPerWorker != 5 ||
		s.LagSLOSeconds != 2 || s.MaxAttempts != 3 || s.SettingsRefresh != 0 || s.AdminToken != "t" || s.PublicBaseURL != "https://x" || s.LogsURL != "https://logs" || s.DashboardURL != "https://dash" {
		t.Fatalf("overrides: %+v %v", s, err)
	}
}
