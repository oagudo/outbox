sqlplus -s app_user/pass@//localhost/FREEPDB1 <<EOF
CREATE TABLE Outbox (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
    context BLOB NOT NULL,
    payload BLOB NOT NULL,
    times_attempted NUMBER(10) DEFAULT 0 NOT NULL
);

CREATE INDEX idx_outbox_created_at ON Outbox (created_at);

EXIT;
EOF