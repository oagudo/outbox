package outbox

import (
	"strings"
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

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		panicMsg  string
	}{
		{
			name:      "valid table name with letters",
			tableName: "outbox",
		},
		{
			name:      "valid table name with underscore",
			tableName: "outbox_table",
		},
		{
			name:      "valid table name starting with underscore",
			tableName: "_outbox",
		},
		{
			name:      "valid table name with numbers",
			tableName: "outbox123",
		},
		{
			name:      "valid table name with mixed case",
			tableName: "OutboxTable",
		},
		{
			name:      "empty table name",
			tableName: "",
			panicMsg:  "table name cannot be empty",
		},
		{
			name:      "table name starting with number",
			tableName: "123outbox",
			panicMsg:  "invalid table name",
		},
		{
			name:      "table name with dash",
			tableName: "outbox-table",
			panicMsg:  "invalid table name",
		},
		{
			name:      "table name with space",
			tableName: "outbox table",
			panicMsg:  "invalid table name",
		},
		{
			name:      "table name with dot",
			tableName: "schema.outbox",
			panicMsg:  "invalid table name",
		},
		{
			name:      "table name with special characters",
			tableName: "outbox@table",
			panicMsg:  "invalid table name",
		},
		{
			name:      "table name with only numbers",
			tableName: "123",
			panicMsg:  "invalid table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.panicMsg != "" {
					if r == nil {
						t.Errorf("expected panic for table name %q, but got none", tt.tableName)
						return
					}
					errMsg := r.(error).Error()
					if tt.panicMsg != "" && !strings.Contains(errMsg, tt.panicMsg) {
						t.Errorf("expected panic message to contain %q, got %q", tt.panicMsg, errMsg)
					}
				} else if r != nil {
					t.Errorf("unexpected panic for table name %q: %v", tt.tableName, r)
				}
			}()

			_ = NewDBContextWithDB(&fakeDB{}, SQLDialectPostgres, WithTableName(tt.tableName))
		})
	}
}
