package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrVersionExists is returned when a ruleset version was published before. Versions identify rulesets in every
// event's audit trail, so a changed ruleset must carry a new version.
var ErrVersionExists = errors.New("ruleset version already published")

// RulesetRow is a ruleset published at runtime.
type RulesetRow struct {
	ID        int64
	Version   string
	YAML      string
	CreatedBy string
	CreatedAt time.Time
	Active    bool
}

// PublishRuleset stores a new ruleset and makes it the active one, in one transaction. The caller must have
// validated the YAML (rules.Parse); running instances pick it up on their next refresh.
func (s *PGStore) PublishRuleset(ctx context.Context, version, yaml, by string) (RulesetRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RulesetRow{}, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM rulesets WHERE version = $1)`, version).Scan(&exists); err != nil {
		return RulesetRow{}, fmt.Errorf("store: check ruleset version: %w", err)
	}
	if exists {
		return RulesetRow{}, fmt.Errorf("%w: %q", ErrVersionExists, version)
	}
	if _, err := tx.Exec(ctx, `UPDATE rulesets SET active = false WHERE active`); err != nil {
		return RulesetRow{}, fmt.Errorf("store: deactivate ruleset: %w", err)
	}
	var row RulesetRow
	err = tx.QueryRow(ctx, `
		INSERT INTO rulesets (version, yaml, created_by, active) VALUES ($1, $2, $3, true)
		RETURNING id, version, yaml, created_by, created_at, active`, version, yaml, by).
		Scan(&row.ID, &row.Version, &row.YAML, &row.CreatedBy, &row.CreatedAt, &row.Active)
	if err != nil {
		return RulesetRow{}, fmt.Errorf("store: insert ruleset: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RulesetRow{}, fmt.Errorf("store: commit ruleset: %w", err)
	}
	return row, nil
}

// ActiveRuleset returns the ruleset published at runtime; ok is false when none is active (the service then
// uses RULES_PATH or the embedded default).
func (s *PGStore) ActiveRuleset(ctx context.Context) (row RulesetRow, ok bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	err = s.pool.QueryRow(ctx, `
		SELECT id, version, yaml, created_by, created_at, active FROM rulesets WHERE active`).
		Scan(&row.ID, &row.Version, &row.YAML, &row.CreatedBy, &row.CreatedAt, &row.Active)
	if errors.Is(err, pgx.ErrNoRows) {
		return RulesetRow{}, false, nil
	}
	if err != nil {
		return RulesetRow{}, false, fmt.Errorf("store: active ruleset: %w", err)
	}
	return row, true, nil
}

// ClearActiveRuleset deactivates the published ruleset (history is kept), so instances fall back to RULES_PATH or
// the embedded default. It reports whether one was active.
func (s *PGStore) ClearActiveRuleset(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE rulesets SET active = false WHERE active`)
	if err != nil {
		return false, fmt.Errorf("store: clear ruleset: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// RulesetHistory lists published rulesets, newest first (without their YAML).
func (s *PGStore) RulesetHistory(ctx context.Context, limit int) ([]RulesetRow, error) {
	if limit <= 0 {
		limit = 20
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id, version, created_by, created_at, active FROM rulesets ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: ruleset history: %w", err)
	}
	defer rows.Close()
	var out []RulesetRow
	for rows.Next() {
		var r RulesetRow
		if err := rows.Scan(&r.ID, &r.Version, &r.CreatedBy, &r.CreatedAt, &r.Active); err != nil {
			return nil, fmt.Errorf("store: scan ruleset: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
