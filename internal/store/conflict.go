package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Conflict policies: what happens when a payload has the same (tenant, device, timestamp) as a stored
// reading but different content. Chosen per tenant in tenants.config -> conflict.policy.
const (
	PolicyKeepFirst = "keep_first" // default: keep the stored reading, save the new version in reading_conflicts
	PolicyKeepLast  = "keep_last"  // replace the stored reading with the new one; the old event stays archived
	PolicyReject    = "reject"     // write nothing to the readings; the conflicting version is only recorded
)

// Conflict resolutions stored in reading_conflicts.resolution.
const (
	ResolutionKeptFirst = "kept_first"
	ResolutionReplaced  = "replaced"
	ResolutionRejected  = "rejected"
)

// ConflictSettings is a tenant's conflict configuration.
type ConflictSettings struct {
	Policy     string `json:"policy"`
	HTTPStatus int    `json:"http_status"` // HTTP status of a conflict response: 200 or 409
}

// DefaultConflictSettings is keep_first answering 200, the behavior before this was configurable.
func DefaultConflictSettings() ConflictSettings {
	return ConflictSettings{Policy: PolicyKeepFirst, HTTPStatus: 200}
}

// NewConflictSettings validates a policy and an optional HTTP status (0 = the policy's default:
// 409 for reject, 200 otherwise).
func NewConflictSettings(policy string, httpStatus int) (ConflictSettings, error) {
	switch policy {
	case PolicyKeepFirst, PolicyKeepLast, PolicyReject:
	default:
		return ConflictSettings{}, fmt.Errorf("conflict policy must be %s, %s or %s (got %q)", PolicyKeepFirst, PolicyKeepLast, PolicyReject, policy)
	}
	switch httpStatus {
	case 0:
		httpStatus = 200
		if policy == PolicyReject {
			httpStatus = 409
		}
	case 200, 409:
	default:
		return ConflictSettings{}, fmt.Errorf("conflict http_status must be 200 or 409 (got %d)", httpStatus)
	}
	return ConflictSettings{Policy: policy, HTTPStatus: httpStatus}, nil
}

// ParseConflictSettings reads the "conflict" object of a tenant's config. Missing means the default.
func ParseConflictSettings(config []byte) (ConflictSettings, error) {
	if len(config) == 0 {
		return DefaultConflictSettings(), nil
	}
	var c struct {
		Conflict *struct {
			Policy     string `json:"policy"`
			HTTPStatus int    `json:"http_status"`
		} `json:"conflict"`
	}
	if err := json.Unmarshal(config, &c); err != nil {
		return ConflictSettings{}, fmt.Errorf("tenant config is not valid JSON: %w", err)
	}
	if c.Conflict == nil {
		return DefaultConflictSettings(), nil
	}
	return NewConflictSettings(c.Conflict.Policy, c.Conflict.HTTPStatus)
}

// conflictSettings reads the tenant's settings inside the transaction. A malformed stored config must never
// stop ingest, so it falls back to the default.
func conflictSettings(ctx context.Context, tx pgx.Tx, tenantID string) ConflictSettings {
	var cfg []byte
	err := tx.QueryRow(ctx, `SELECT config FROM tenants WHERE id = $1`, tenantID).Scan(&cfg)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return DefaultConflictSettings()
	}
	s, perr := ParseConflictSettings(cfg)
	if perr != nil {
		return DefaultConflictSettings()
	}
	return s
}

// SetConflictSettings stores a tenant's conflict policy, leaving the rest of its config alone.
func (s *PGStore) SetConflictSettings(ctx context.Context, tenantID string, cs ConflictSettings) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	b, err := json.Marshal(cs)
	if err != nil {
		return fmt.Errorf("store: marshal conflict settings: %w", err)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE tenants SET config = jsonb_set(CASE WHEN jsonb_typeof(config) = 'object' THEN config ELSE '{}'::jsonb END, '{conflict}', $2::jsonb) WHERE id = $1`, tenantID, b)
	if err != nil {
		return fmt.Errorf("store: set conflict settings: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// TenantConfig returns a tenant's raw config JSON.
func (s *PGStore) TenantConfig(ctx context.Context, tenantID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var cfg []byte
	err := s.pool.QueryRow(ctx, `SELECT config FROM tenants WHERE id = $1`, tenantID).Scan(&cfg)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: tenant config: %w", err)
	}
	return cfg, nil
}
