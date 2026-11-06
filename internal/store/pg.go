// Package store holds the Postgres implementation of the ingest store: transactions that run under
// row-level security, the raw/outcome/reading writes, and tenant and key lookups (DESIGN.md sections 4, 7).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/yuvasrig/paxefo-integration-api/migrations"
)

// ErrNotFound is returned when a row does not exist or is not visible to the tenant.
var ErrNotFound = errors.New("not found")

// ErrBadCursor is returned for a malformed pagination cursor.
var ErrBadCursor = errors.New("bad cursor")

const appRole = "paxafe_app"

// Options tunes the connection pool.
type Options struct {
	MaxConns     int32
	QueryTimeout time.Duration // applied to every database call
}

// PGStore is the Postgres store.
type PGStore struct {
	pool       *pgxpool.Pool
	timeout    time.Duration
	useAppRole bool
}

// Open connects to Postgres. Call Migrate before serving.
func Open(ctx context.Context, dsn string, opts Options) (*PGStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse database url: %w", err)
	}
	if opts.MaxConns > 0 {
		cfg.MaxConns = opts.MaxConns
	}
	if opts.QueryTimeout <= 0 {
		opts.QueryTimeout = 5 * time.Second
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	s := &PGStore{pool: pool, timeout: opts.QueryTimeout}
	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the pool.
func (s *PGStore) Close() { s.pool.Close() }

// Pool exposes the pool for metrics.
func (s *PGStore) Pool() *pgxpool.Pool { return s.pool }

// Ping checks the database is reachable.
func (s *PGStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("store: ping: %w", err)
	}
	return nil
}

// Migrate applies pending goose migrations (F13-R1) and then checks whether the application role is usable.
func (s *PGStore) Migrate(ctx context.Context) error {
	db := stdlib.OpenDBFromPool(s.pool)
	defer db.Close()
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		return fmt.Errorf("store: migrations provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return s.detectAppRole(ctx)
}

// MigrationVersion returns the highest applied migration.
func (s *PGStore) MigrationVersion(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var v int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(version_id), 0) FROM goose_db_version WHERE is_applied`).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("store: migration version: %w", err)
	}
	return v, nil
}

func (s *PGStore) detectAppRole(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var ok bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1) AND pg_has_role(current_user, $1, 'MEMBER')`, appRole).Scan(&ok)
	if err != nil {
		return fmt.Errorf("store: detect app role: %w", err)
	}
	s.useAppRole = ok
	return nil
}

// UsesAppRole reports whether tenant transactions run as the non-bypass role.
func (s *PGStore) UsesAppRole() bool { return s.useAppRole }

// WithTenantTx runs fn in a transaction whose row-level security is scoped to tenantID: it switches to the
// application role and sets app.tenant_id for the transaction only. Everything fn does is committed or rolled
// back together.
func (s *PGStore) WithTenantTx(ctx context.Context, tenantID string, fn func(pgx.Tx) error) error {
	return s.withTenantTxTimeout(ctx, tenantID, s.timeout, fn)
}

func (s *PGStore) withTenantTxTimeout(ctx context.Context, tenantID string, timeout time.Duration, fn func(pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if s.useAppRole {
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE "+appRole); err != nil {
			return fmt.Errorf("store: set role: %w", err)
		}
	}
	if err := setTenant(ctx, tx, tenantID); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}

func setTenant(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, tenantID); err != nil {
		return fmt.Errorf("store: set tenant: %w", err)
	}
	return nil
}

var errRollback = errors.New("rollback requested")

// InRollbackTx runs fn in a tenant transaction that is always rolled back, handing it a commit function that
// writes inside that transaction. It is how a replay dry run predicts the effect of many events in order, with
// each one seeing the writes of the ones before it, without changing anything. timeout bounds the whole run.
func (s *PGStore) InRollbackTx(ctx context.Context, tenantID string, timeout time.Duration,
	fn func(commit func(context.Context, CommitInput) (CommitResult, error)) error) error {
	err := s.withTenantTxTimeout(ctx, tenantID, timeout, func(tx pgx.Tx) error {
		if err := fn(func(c context.Context, in CommitInput) (CommitResult, error) { return CommitTx(c, tx, in) }); err != nil {
			return err
		}
		return errRollback
	})
	if errors.Is(err, errRollback) {
		return nil
	}
	return err
}
