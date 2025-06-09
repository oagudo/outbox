package test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

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

				_, err = db.Exec("TRUNCATE TABLE outbox")
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

				_, err = db.Exec("TRUNCATE TABLE outbox")
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

				_, err = db.Exec("TRUNCATE TABLE outbox")
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
					CREATE TABLE IF NOT EXISTS outbox (
						id TEXT PRIMARY KEY,
						created_at DATETIME NOT NULL,
						scheduled_at DATETIME NOT NULL,
						metadata BLOB,
						payload BLOB NOT NULL,
						times_attempted INTEGER NOT NULL
					);
					CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON outbox (created_at);
					CREATE INDEX IF NOT EXISTS idx_outbox_scheduled_at ON outbox (scheduled_at);
				`)
				if err != nil {
					return nil, err
				}

				_, err = db.Exec("DELETE FROM outbox")
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

				_, err = db.Exec("TRUNCATE TABLE outbox")
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

			dbCtx := outbox.NewDBContext(db, tt.dialect)
			w := outbox.NewWriter(dbCtx)

			successMsg := createMessageFixture()
			err = w.Write(context.Background(), successMsg, func(_ context.Context, _ outbox.TxQueryer) error {
				return nil
			})
			require.NoError(t, err)

			failingMsg := createMessageFixture(outbox.WithCreatedAt(successMsg.CreatedAt.Add(1 * time.Second)))
			err = w.Write(context.Background(), failingMsg, func(_ context.Context, _ outbox.TxQueryer) error {
				return nil
			})
			require.NoError(t, err)

			r := outbox.NewReader(dbCtx, &fakePublisher{
				onPublish: func(_ context.Context, msg *outbox.Message) error {
					if msg.ID == failingMsg.ID {
						assertMessageEqual(t, failingMsg, msg)
						return errors.New("failed to publish")
					}
					assertMessageEqual(t, successMsg, msg)
					return nil
				},
			},
				outbox.WithInterval(readerInterval),
				outbox.WithReadBatchSize(1),
				outbox.WithMaxAttempts(1),
				outbox.WithDelay(0),
			)
			r.Start()

			waitForReaderDiscardedMessage(t, r, failingMsg)

			require.Eventually(t, func() bool {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&count)
				return err == nil && count == 0
			}, testTimeout, pollInterval)

			require.NoError(t, r.Stop(context.Background()))
		})
	}
}
