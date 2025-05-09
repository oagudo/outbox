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

	log.Println("Connected to database")
	_, err = db.Exec("TRUNCATE TABLE Outbox")
	if err != nil {
		log.Fatalf("Failed to truncate outbox table: %s", err)
	}

	_, err = db.Exec("TRUNCATE TABLE Entity")
	if err != nil {
		log.Fatalf("Failed to truncate entity table: %s", err)
	}

	os.Exit(m.Run())
}
