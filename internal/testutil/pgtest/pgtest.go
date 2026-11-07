// Package pgtest gives integration tests a fresh, migrated Postgres database.
// It uses TEST_DATABASE_URL (a superuser DSN, for example the CI service container or `make up`)
// when set, and otherwise starts a postgres:16 container with testcontainers-go.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	once    sync.Once
	baseDSN string
	baseErr error
)

// NewDSN returns the DSN of a new empty database owned by a superuser. The database is dropped when the test ends.
func NewDSN(t testing.TB) string {
	t.Helper()
	once.Do(func() { baseDSN, baseErr = startBase() })
	if baseErr != nil {
		t.Skipf("no Postgres available (set TEST_DATABASE_URL or start Docker): %v", baseErr)
	}

	ctx := context.Background()
	admin, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)

	ensureAppRole(ctx, t, admin)

	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "t_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), baseDSN)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})

	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

func startBase() (string, error) {
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("postgres"), tcpostgres.WithUsername("postgres"), tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return "", fmt.Errorf("start postgres container: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", fmt.Errorf("container dsn: %w", err)
	}
	return dsn, nil // the container is removed by Ryuk when the test binary exits
}

// roleLockKey identifies the advisory lock that serialises creating the application role.
const roleLockKey = 727274

// ensureAppRole creates the cluster-wide application role (and the membership the migration grants) before any test
// database is migrated. Roles belong to the whole server, not to one database, so test packages that run in parallel
// processes against one server would otherwise race to create it inside their migrations and fail with a duplicate
// key error. The advisory lock is taken on the shared admin database, so it also orders separate processes.
func ensureAppRole(ctx context.Context, t testing.TB, admin *pgx.Conn) {
	t.Helper()
	if _, err := admin.Exec(ctx, "SELECT pg_advisory_lock($1)", roleLockKey); err != nil {
		t.Fatalf("lock for the application role: %v", err)
	}
	defer func() { _, _ = admin.Exec(ctx, "SELECT pg_advisory_unlock($1)", roleLockKey) }()
	_, err := admin.Exec(ctx, `DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        CREATE ROLE paxafe_app NOLOGIN NOBYPASSRLS;
    END IF;
    EXECUTE format('GRANT paxafe_app TO %I', current_user);
END $$`)
	if err != nil {
		t.Fatalf("create the application role: %v", err)
	}
}
