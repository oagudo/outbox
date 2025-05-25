sqlplus -s app_user/pass@//localhost/FREEPDB1 <<EOF
CREATE TABLE Entity (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);

EXIT;
EOF