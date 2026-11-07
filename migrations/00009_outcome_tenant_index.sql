-- +goose Up
-- The console reads each tenant's recent outcomes (completed per minute, status mix, latency) every few seconds. The
-- existing indexes lead with the status or only the time, so a busy system made those reads scan other tenants' rows.
-- This one leads with the tenant, is ordered newest first, and carries the status, so the reads are index-only.
CREATE INDEX event_outcomes_tenant_processed ON event_outcomes (tenant_id, processed_at DESC) INCLUDE (status);

-- +goose Down
DROP INDEX IF EXISTS event_outcomes_tenant_processed;
