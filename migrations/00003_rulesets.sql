-- +goose Up

-- Rulesets published at runtime with `api rules set`. Running instances poll for the active row and swap it in
-- without a restart. This is global service configuration, not tenant data: it is not row-level secured, and the
-- application role may only read it (operators write through the owner connection).
CREATE TABLE rulesets (
    id         bigserial PRIMARY KEY,
    version    text NOT NULL UNIQUE,   -- recorded on every event; a new publish must bump it
    yaml       text NOT NULL,
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    active     boolean NOT NULL DEFAULT false
);
-- At most one active ruleset. No active row means "use RULES_PATH, or the embedded default".
CREATE UNIQUE INDEX rulesets_one_active ON rulesets (active) WHERE active;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'paxafe_app') THEN
        GRANT SELECT ON rulesets TO paxafe_app;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'could not grant SELECT on rulesets to paxafe_app';
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS rulesets;
