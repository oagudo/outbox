CREATE TABLE IF NOT EXISTS outbox (
    id BINARY(16) PRIMARY KEY,
    created_at TIMESTAMP(3) NOT NULL,
    scheduled_at TIMESTAMP(3) NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted INT NOT NULL
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON outbox (scheduled_at);