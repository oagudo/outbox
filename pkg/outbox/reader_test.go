package outbox

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSelectMessagesQuery(t *testing.T) {
	tests := []struct {
		driver  SQLDialect
		wantSQL string
	}{
		{
			driver:  PostgresDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT $1",
		},
		{
			driver:  MySQLDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  MariaDBDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  SQLiteDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  SQLServerDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT @p1",
		},
		{
			driver:  OracleDialect,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC FETCH FIRST :1 ROWS ONLY",
		},
		{
			driver:  SQLDialect("unknown"),
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("driver %s", tt.driver), func(t *testing.T) {
			SetSQLDialect(tt.driver)

			got := buildSelectMessagesQuery()

			assert.Equal(t, tt.wantSQL, got)
		})
	}
}
