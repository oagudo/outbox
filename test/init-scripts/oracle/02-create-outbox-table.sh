sqlplus -s app_user/pass@//localhost/FREEPDB1 <<EOF
CREATE TABLE Outbox (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata BLOB,
    payload BLOB NOT NULL,
    times_attempted NUMBER(10) NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);
CREATE INDEX idx_outbox_scheduled_at ON Outbox (scheduled_at);

EXIT;
EOF