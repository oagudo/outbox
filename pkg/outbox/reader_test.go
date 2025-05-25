package outbox

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSelectMessagesQuery(t *testing.T) {
	tests := []struct {
		driver  DriverType
		wantSQL string
	}{
		{
			driver:  DriverPostgres,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT $1",
		},
		{
			driver:  DriverMySQL,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  DriverMariaDB,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  DriverSQLite,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
		{
			driver:  DriverSQLServer,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT @p1",
		},
		{
			driver:  DriverOracle,
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC FETCH FIRST :1 ROWS ONLY",
		},
		{
			driver:  DriverType("unknown"),
			wantSQL: "SELECT id, payload, created_at, context FROM Outbox ORDER BY created_at ASC LIMIT ?",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("driver %s", tt.driver), func(t *testing.T) {
			SetDriver(tt.driver)

			got := buildSelectMessagesQuery()

			assert.Equal(t, tt.wantSQL, got)
		})
	}
}
