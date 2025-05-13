CREATE TABLE IF NOT EXISTS Outbox (
    id CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    context BLOB NOT NULL,
    payload BLOB NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);