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
		s.BodyLimitBytes != 256<<10 || s.RateLimitPerSec != 100 || s.RateLimitBurst != 200 ||
		s.DBMaxConns != 10 || s.QueryTimeout != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", s)
	}

	s, err = FromEnv(env(map[string]string{
		"DATABASE_URL": "postgres://x", "PORT": "10000", "LOG_LEVEL": "debug", "BODY_LIMIT_BYTES": "1024",
		"RATE_LIMIT_PER_SEC": "2.5", "RATE_LIMIT_BURST": "7", "DB_MAX_CONNS": "3", "DB_QUERY_TIMEOUT": "750ms", "RULES_PATH": "/r.yaml",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.Port != "10000" || s.LogLevel != "debug" || s.BodyLimitBytes != 1024 || s.RateLimitPerSec != 2.5 || s.RateLimitBurst != 7 ||
		s.DBMaxConns != 3 || s.QueryTimeout != 750*time.Millisecond || s.RulesPath != "/r.yaml" {
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
		{"async not built", map[string]string{"INGEST_MODE": "async"}, "INGEST_MODE"},
		{"unknown role", map[string]string{"ROLE": "worker"}, "ROLE"},
		{"bad port", map[string]string{"PORT": "abc"}, "PORT"},
		{"bad level", map[string]string{"LOG_LEVEL": "loud"}, "LOG_LEVEL"},
		{"bad body limit", map[string]string{"BODY_LIMIT_BYTES": "0"}, "BODY_LIMIT_BYTES"},
		{"bad rate", map[string]string{"RATE_LIMIT_PER_SEC": "-1"}, "RATE_LIMIT_PER_SEC"},
		{"bad timeout", map[string]string{"DB_QUERY_TIMEOUT": "soon"}, "DB_QUERY_TIMEOUT"},
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
