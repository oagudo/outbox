package test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/oagudo/outbox/pkg/outbox"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func TestDialectSucceeds(t *testing.T) {
	type dialectTestCase struct {
		openDB  func() (*sql.DB, error)
		dialect outbox.SQLDialect
	}

	tests := []dialectTestCase{
		{
			openDB: func() (*sql.DB, error) {
				db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/outbox?parseTime=true")
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("TRUNCATE TABLE Outbox")
				return db, err
			},
			dialect: outbox.SQLDialectMySQL,
		},
		{
			openDB: func() (*sql.DB, error) {
				db, err := sql.Open("oracle", "oracle://app_user:pass@localhost:1521/FREEPDB1")
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("TRUNCATE TABLE Outbox")
				return db, err
			},
			dialect: outbox.SQLDialectOracle,
		},
		{
			openDB: func() (*sql.DB, error) {
				db, err := sql.Open("sqlserver", "sqlserver://sa:SqlServer123!@localhost:1433?database=outbox")
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("TRUNCATE TABLE Outbox")
				return db, err
			},
			dialect: outbox.SQLDialectSQLServer,
		},
		{
			openDB: func() (*sql.DB, error) {
				db, err := sql.Open("sqlite3", ":memory:")
				if err != nil {
					return nil, err
				}

				// Create tables since SQLite runs in-process
				_, err = db.Exec(`
					CREATE TABLE IF NOT EXISTS Outbox (
						id TEXT PRIMARY KEY,
						created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
						context BLOB NOT NULL,
						payload BLOB NOT NULL,
						times_attempted INTEGER NOT NULL DEFAULT 0
					);
					CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON Outbox (created_at);
				`)
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("DELETE FROM Outbox")
				if err != nil {
					return nil, err
				}

				return db, nil
			},
			dialect: outbox.SQLDialectSQLite,
		},
		{
			openDB: func() (*sql.DB, error) {
				db, err := sql.Open("mysql", "user:password@tcp(localhost:3307)/outbox?parseTime=true")
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("TRUNCATE TABLE Outbox")
				return db, err
			},
			dialect: outbox.SQLDialectMariaDB,
		},
	}
	for _, tt := range tests {
		t.Run(string(tt.dialect), func(t *testing.T) {
			db, err := tt.openDB()
			require.NoError(t, err)
			defer func() {
				_ = db.Close()
			}()

			anyMsg := createMessageFixture()
			dbCtx := outbox.NewDBContext(db, tt.dialect)
			w := outbox.NewWriter(dbCtx)
			err = w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.ExecInTxFunc) error {
				return nil
			})
			require.NoError(t, err)

			r := outbox.NewReader(dbCtx, &fakePublisher{
				onPublish: func(_ context.Context, msg *outbox.Message) error {
					assertMessageEqual(t, anyMsg, msg)
					return nil
				},
			}, outbox.WithInterval(readerInterval))
			r.Start()

			require.Eventually(t, func() bool {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
				return err == nil && count == 0
			}, testTimeout, pollInterval)

			err = r.Stop(context.Background())
			require.NoError(t, err)
		})
	}
}
