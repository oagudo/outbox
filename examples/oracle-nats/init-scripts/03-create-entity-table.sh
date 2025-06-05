sqlplus -s app_user/pass@//localhost/FREEPDB1 <<EOF
CREATE TABLE entity (
    id RAW(16) PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

EXIT;
EOF