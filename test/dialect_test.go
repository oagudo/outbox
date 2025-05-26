package test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/oagudo/outbox/pkg/outbox"
	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func TestDialectSucceeds(t *testing.T) {
	type test struct {
		openConn func() (*sql.DB, error)
		dialect  outbox.SQLDialect
	}

	tests := []test{
		{
			openConn: func() (*sql.DB, error) {
				return sql.Open("mysql", "user:password@tcp(localhost:3306)/outbox?parseTime=true")
			},
			dialect: outbox.MySQLDialect,
		},
		{
			openConn: func() (*sql.DB, error) {
				return sql.Open("oracle", "oracle://app_user:pass@localhost:1521/FREEPDB1")
			},
			dialect: outbox.OracleDialect,
		},

		// TODO: add tests for sqlite and sql server
	}
	for _, test := range tests {
		t.Run(string(test.dialect), func(t *testing.T) {
			t.Cleanup(func() {
				outbox.SetSQLDialect(outbox.PostgresDialect)
			})
			outbox.SetSQLDialect(test.dialect)
			dialectDB, err := test.openConn()
			require.NoError(t, err)
			defer func() {
				_ = dialectDB.Close()
			}()

			_, err = dialectDB.Exec("TRUNCATE TABLE Outbox")
			require.NoError(t, err)

			anyMsg := createMessageFixture()
			w := outbox.NewWriter(dialectDB)
			err = w.Write(context.Background(), anyMsg, func(_ context.Context, _ outbox.TxExecFunc) error {
				return nil
			})
			require.NoError(t, err)

			r := outbox.NewReader(dialectDB, &fakePublisher{
				onPublish: func(_ context.Context, msg outbox.Message) {
					assertMessageEqual(t, anyMsg, msg)
				},
			}, outbox.WithInterval(readerInterval))
			r.Start()

			require.Eventually(t, func() bool {
				var count int
				err := dialectDB.QueryRow("SELECT COUNT(*) FROM Outbox").Scan(&count)
				return err == nil && count == 0
			}, testTimeout, pollInterval)

			err = r.Stop(context.Background())
			require.NoError(t, err)
		})
	}
}
