-- +goose Up
-- Rate limiting was removed: Tive does not honour 429 or Retry-After, so the service no longer refuses traffic for rate.
-- Remove the stored values of the settings that no longer exist, so they are not reported as unknown on every refresh.
DELETE FROM settings WHERE key IN ('rate_limit_per_sec', 'rate_limit_burst', 'rate_limit_webhook');

UPDATE tenants
SET config = jsonb_set(config, '{settings}', (config->'settings') - 'rate_limit_per_sec' - 'rate_limit_burst' - 'rate_limit_webhook')
WHERE jsonb_typeof(config) = 'object' AND jsonb_typeof(config->'settings') = 'object'
  AND (config->'settings' ?| ARRAY['rate_limit_per_sec', 'rate_limit_burst', 'rate_limit_webhook']);

-- +goose Down
-- Nothing to restore: the values were settings of a feature that no longer exists.
SELECT 1;
