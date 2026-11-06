package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yuvasrig/paxefo-integration-api/internal/auth"
)

// These methods touch the registry and auth tables, which are not tenant-scoped, so they use the pool directly.

// CreateTenant registers a tenant; it is a no-op if the id exists.
func (s *PGStore) CreateTenant(ctx context.Context, id, name string, config map[string]any) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if config == nil {
		config = map[string]any{}
	}
	cfg, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("store: marshal tenant config: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, name, config) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`, id, name, cfg); err != nil {
		return fmt.Errorf("store: create tenant: %w", err)
	}
	return nil
}

// TenantExists reports whether the tenant id is registered.
func (s *PGStore) TenantExists(ctx context.Context, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var ok bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, id).Scan(&ok); err != nil {
		return false, fmt.Errorf("store: tenant exists: %w", err)
	}
	return ok, nil
}

// CreateAPIKey stores a key hash for a tenant.
func (s *PGStore) CreateAPIKey(ctx context.Context, tenantID, name, prefix, hash string, expiresAt *time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `INSERT INTO api_keys (key_hash, prefix, tenant_id, name, expires_at) VALUES ($1, $2, $3, $4, $5)`,
		hash, prefix, tenantID, name, expiresAt)
	if err != nil {
		return fmt.Errorf("store: create api key: %w", err)
	}
	return nil
}

// LookupAPIKey finds a key by hash; ErrNotFound if unknown. Expiry and revocation are checked by the caller.
func (s *PGStore) LookupAPIKey(ctx context.Context, hash string) (APIKey, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var k APIKey
	err := s.pool.QueryRow(ctx, `SELECT id, prefix, tenant_id, name, created_at, expires_at, revoked_at FROM api_keys WHERE key_hash = $1`, hash).
		Scan(&k.ID, &k.Prefix, &k.TenantID, &k.Name, &k.CreatedAt, &k.ExpiresAt, &k.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKey{}, ErrNotFound
	}
	if err != nil {
		return APIKey{}, fmt.Errorf("store: lookup api key: %w", err)
	}
	return k, nil
}

// ListAPIKeys lists keys, newest first; tenantID "" lists all.
func (s *PGStore) ListAPIKeys(ctx context.Context, tenantID string) ([]APIKey, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT id, prefix, tenant_id, name, created_at, expires_at, revoked_at FROM api_keys
		WHERE $1 = '' OR tenant_id = $1 ORDER BY id DESC`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("store: list api keys: %w", err)
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.Prefix, &k.TenantID, &k.Name, &k.CreatedAt, &k.ExpiresAt, &k.RevokedAt); err != nil {
			return nil, fmt.Errorf("store: scan api key: %w", err)
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeAPIKey revokes a key by id; ErrNotFound if it does not exist.
func (s *PGStore) RevokeAPIKey(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: revoke api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// LookupKey adapts LookupAPIKey to auth.KeyStore.
func (s *PGStore) LookupKey(ctx context.Context, hash string) (auth.KeyRecord, error) {
	k, err := s.LookupAPIKey(ctx, hash)
	if errors.Is(err, ErrNotFound) {
		return auth.KeyRecord{}, auth.ErrKeyNotFound
	}
	if err != nil {
		return auth.KeyRecord{}, err
	}
	return auth.KeyRecord{ID: k.ID, TenantID: k.TenantID, Name: k.Name, ExpiresAt: k.ExpiresAt, RevokedAt: k.RevokedAt}, nil
}

// SettingRow is a runtime setting overriding an environment default.
type SettingRow struct {
	Key       string
	Value     string
	UpdatedBy string
	UpdatedAt time.Time
}

// ListSettings returns every runtime setting that has been changed.
func (s *PGStore) ListSettings(ctx context.Context) ([]SettingRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT key, value, updated_by, updated_at FROM settings ORDER BY key`)
	if err != nil {
		return nil, fmt.Errorf("store: list settings: %w", err)
	}
	defer rows.Close()
	var out []SettingRow
	for rows.Next() {
		var r SettingRow
		if err := rows.Scan(&r.Key, &r.Value, &r.UpdatedBy, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("store: scan setting: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetSetting stores a runtime setting. The caller validates it (package settings).
func (s *PGStore) SetSetting(ctx context.Context, key, value, by string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO settings (key, value, updated_by) VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = now()`, key, value, by)
	if err != nil {
		return fmt.Errorf("store: set setting: %w", err)
	}
	return nil
}

// ResetSetting removes a runtime setting, so the environment default applies again.
func (s *PGStore) ResetSetting(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `DELETE FROM settings WHERE key = $1`, key); err != nil {
		return fmt.Errorf("store: reset setting: %w", err)
	}
	return nil
}
