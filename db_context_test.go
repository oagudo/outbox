package outbox

import (
	"testing"
)

func TestWithTableName(t *testing.T) {
	t.Run("uses default table name when no option provided", func(t *testing.T) {
		dbCtx := NewDBContextWithDB(&fakeDB{}, SQLDialectPostgres)

		if dbCtx.tableName != "outbox" {
			t.Errorf("expected default table name 'outbox', got %q", dbCtx.tableName)
		}
	})

	t.Run("uses custom table name in queries", func(t *testing.T) {
		customTable := "custom_events"

		dbCtx := NewDBContextWithDB(&fakeDB{}, SQLDialectPostgres, WithTableName(customTable))

		if dbCtx.tableName != customTable {
			t.Errorf("expected table name %q, got %q", customTable, dbCtx.tableName)
		}
	})
}
