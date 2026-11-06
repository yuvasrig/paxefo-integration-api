-- +goose Up
-- A tenant's worker limits are now ordinary per-tenant settings (tenants.config.settings.workers_min / workers_max),
-- like every other overridable setting. Move any limits stored the earlier way (config.workers = {min, max}).
UPDATE tenants
SET config = (config - 'workers') || jsonb_build_object('settings',
        COALESCE(CASE WHEN jsonb_typeof(config->'settings') = 'object' THEN config->'settings' END, '{}'::jsonb)
        || jsonb_build_object('workers_min', config->'workers'->>'min', 'workers_max', config->'workers'->>'max'))
WHERE jsonb_typeof(config) = 'object' AND jsonb_typeof(config->'workers') = 'object'
  AND config->'workers' ? 'min' AND config->'workers' ? 'max';

-- Anything else under the old key (an unrecognised shape) is dropped so it cannot be mistaken for a setting.
UPDATE tenants SET config = config - 'workers' WHERE jsonb_typeof(config) = 'object' AND config ? 'workers';

-- +goose Down
-- Nothing to undo: the settings stay where they are.
SELECT 1;
