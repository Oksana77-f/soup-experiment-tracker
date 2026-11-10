CREATE TABLE IF NOT EXISTS projects (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS api_keys (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    scope text NOT NULL CHECK (scope IN ('read', 'write')),
    digest text NOT NULL UNIQUE,
    revoked_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_key_per_scope
    ON api_keys (project_id, scope) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS runs (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    name text NOT NULL,
    last_seq bigint NOT NULL DEFAULT 0 CHECK (last_seq BETWEEN 0 AND 9007199254740991),
    UNIQUE (project_id, name)
);

CREATE TABLE IF NOT EXISTS metrics (
    run_id uuid NOT NULL REFERENCES runs(id),
    seq bigint NOT NULL CHECK (seq BETWEEN 1 AND 9007199254740991),
    name text NOT NULL,
    step bigint NOT NULL CHECK (step BETWEEN 0 AND 9007199254740991),
    value double precision NOT NULL,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, name, step)
);

CREATE TABLE IF NOT EXISTS receipts (
    run_id uuid NOT NULL REFERENCES runs(id),
    batch_id uuid NOT NULL,
    digest text NOT NULL,
    acknowledgment jsonb NOT NULL,
    PRIMARY KEY (run_id, batch_id)
);
