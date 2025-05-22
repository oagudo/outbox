package outbox

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSQLPlaceholder(t *testing.T) {
	tests := []struct {
		driver          DriverType
		index           int
		wantPlaceholder string
	}{
		{
			driver:          DriverPostgres,
			index:           1,
			wantPlaceholder: "$1",
		},
		{
			driver:          DriverMariaDB,
			index:           2,
			wantPlaceholder: "?",
		},
		{
			driver:          DriverSQLite,
			index:           3,
			wantPlaceholder: "?",
		},
		{
			driver:          DriverOracle,
			index:           4,
			wantPlaceholder: ":4",
		},
		{
			driver:          DriverMySQL,
			index:           5,
			wantPlaceholder: "?",
		},
		{
			driver:          DriverSQLServer,
			index:           6,
			wantPlaceholder: "@p6",
		},
		{
			driver:          DriverType("unknown"),
			index:           7,
			wantPlaceholder: "?",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("driver %s", tt.driver), func(t *testing.T) {
			SetDriver(tt.driver)

			got := getSQLPlaceholder(tt.index)
			assert.Equal(t, tt.wantPlaceholder, got)
		})
	}
}
