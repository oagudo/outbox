package test

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

var (
	db *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	db, err = sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/outbox?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %s", err)
	}

	err = truncateOutboxTable()
	if err != nil {
		log.Fatalf("Failed to truncate outbox table: %s", err)
	}

	err = truncateEntityTable()
	if err != nil {
		log.Fatalf("Failed to truncate entity table: %s", err)
	}

	os.Exit(m.Run())
}

func truncateOutboxTable() error {
	_, err := db.Exec("TRUNCATE TABLE Outbox")
	return err
}

func truncateEntityTable() error {
	_, err := db.Exec("TRUNCATE TABLE Entity")
	return err
}
