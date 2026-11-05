package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ResolveDevice finds the IMEI of the open device assignment with this name in the tenant (F5-R2).
// ok is false when the name is unknown or maps to more than one IMEI (F5-R3).
func (s *PGStore) ResolveDevice(ctx context.Context, tenantID, name string) (imei string, ok bool, err error) {
	err = s.WithTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT imei FROM devices WHERE tenant_id = $1 AND device_name = $2 AND valid_to IS NULL LIMIT 2`, tenantID, name)
		if err != nil {
			return fmt.Errorf("store: resolve device: %w", err)
		}
		defer rows.Close()
		var found []string
		for rows.Next() {
			var i string
			if err := rows.Scan(&i); err != nil {
				return fmt.Errorf("store: scan device: %w", err)
			}
			found = append(found, i)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("store: resolve device rows: %w", err)
		}
		if len(found) == 1 {
			imei, ok = found[0], true
		}
		return nil
	})
	return imei, ok, err
}

// ReassignDevice moves a tracker between tenants at time at: it closes the open assignment in fromTenant
// and opens one in toTenant, in one transaction (F5-R4). It is an operator action, never triggered by a payload.
func (s *PGStore) ReassignDevice(ctx context.Context, imei, name, fromTenant, toTenant string, at time.Time) error {
	return s.WithTenantTx(ctx, fromTenant, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE devices SET valid_to = $3 WHERE tenant_id = $1 AND imei = $2 AND valid_to IS NULL`, fromTenant, imei, at)
		if err != nil {
			return fmt.Errorf("store: close assignment: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("store: device %s has no open assignment in tenant %s: %w", imei, fromTenant, ErrNotFound)
		}
		if err := setTenant(ctx, tx, toTenant); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO devices (tenant_id, imei, device_name, valid_from, first_seen, last_seen)
			VALUES ($1, $2, $3, $4, $4, $4)`, toTenant, imei, name, at)
		if err != nil {
			return fmt.Errorf("store: open assignment: %w", err)
		}
		return nil
	})
}
