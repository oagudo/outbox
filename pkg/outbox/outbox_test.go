package outbox

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestGetSQLPlaceholder(t *testing.T) {
	tests := []struct {
		driver          SQLDialect
		index           int
		wantPlaceholder string
	}{
		{
			driver:          PostgresDialect,
			index:           1,
			wantPlaceholder: "$1",
		},
		{
			driver:          MariaDBDialect,
			index:           2,
			wantPlaceholder: "?",
		},
		{
			driver:          SQLiteDialect,
			index:           3,
			wantPlaceholder: "?",
		},
		{
			driver:          OracleDialect,
			index:           4,
			wantPlaceholder: ":4",
		},
		{
			driver:          MySQLDialect,
			index:           5,
			wantPlaceholder: "?",
		},
		{
			driver:          SQLServerDialect,
			index:           6,
			wantPlaceholder: "@p6",
		},
		{
			driver:          SQLDialect("unknown"),
			index:           7,
			wantPlaceholder: "?",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("driver %s", tt.driver), func(t *testing.T) {
			SetSQLDialect(tt.driver)

			got := getSQLPlaceholder(tt.index)
			assert.Equal(t, tt.wantPlaceholder, got)
		})
	}
}

func TestGetIDType(t *testing.T) {
	anyUUIDAsString := "217eefca-36bf-4ce1-885b-00b520730005"
	anyUUID := uuid.MustParse(anyUUIDAsString)
	anyUUIDBytes, _ := anyUUID.MarshalBinary()
	testMessage := Message{ID: anyUUID}

	tests := []struct {
		driver   SQLDialect
		wantType any
	}{
		{
			driver:   MySQLDialect,
			wantType: anyUUIDBytes,
		},
		{
			driver:   MariaDBDialect,
			wantType: anyUUIDBytes,
		},
		{
			driver:   OracleDialect,
			wantType: anyUUIDBytes,
		},
		{
			driver:   PostgresDialect,
			wantType: anyUUID,
		},
		{
			driver:   SQLServerDialect,
			wantType: anyUUID,
		},
		{
			driver:   SQLiteDialect,
			wantType: anyUUIDAsString,
		},
		{
			driver:   SQLDialect("unknown"),
			wantType: anyUUIDAsString, // default to string
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("driver %s", tt.driver), func(t *testing.T) {
			SetSQLDialect(tt.driver)

			got := formatMessageIDForDB(testMessage)

			assert.Equal(t, tt.wantType, got)
		})
	}
}
