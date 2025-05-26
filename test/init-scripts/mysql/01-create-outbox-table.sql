CREATE TABLE IF NOT EXISTS Outbox (
    id BINARY(16) PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);