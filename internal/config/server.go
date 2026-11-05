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
	if s.IngestMode != "sync" {
		return Server{}, fmt.Errorf("config: INGEST_MODE %q is not available; only sync is built (async is designed, DESIGN.md section 8)", s.IngestMode)
	}
	if s.Role != "all" && s.Role != "api" {
		return Server{}, fmt.Errorf("config: ROLE %q is not available; use all or api", s.Role)
	}
	return s, nil
}
