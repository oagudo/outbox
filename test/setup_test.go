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
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	var err error
	db, err = sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/outbox?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %s", err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Ping()
	if err != nil {
		log.Printf("Failed to ping database: %s", err)
		return 1
	}

	err = truncateOutboxTable()
	if err != nil {
		log.Printf("Failed to truncate outbox table: %s", err)
		return 1
	}

	err = truncateEntityTable()
	if err != nil {
		log.Printf("Failed to truncate entity table: %s", err)
		return 1
	}

	return m.Run()
}

func truncateOutboxTable() error {
	_, err := db.Exec("TRUNCATE TABLE outbox")
	return err
}

func truncateEntityTable() error {
	_, err := db.Exec("TRUNCATE TABLE entity")
	return err
}
