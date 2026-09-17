CREATE TABLE IF NOT EXISTS documents (
    id         UUID PRIMARY KEY,
    title      TEXT NOT NULL DEFAULT '',
    body       TEXT NOT NULL,
    -- Must increase on every change, including when a deleted id is created
    -- again. The indexer uses it as the OpenSearch external version so that a
    -- stale or replayed event can never overwrite newer indexed data.
    version    BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
