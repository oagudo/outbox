CREATE TABLE IF NOT EXISTS Outbox (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    context BYTEA NOT NULL,
    payload BYTEA NOT NULL,
    times_attempted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON Outbox (created_at);