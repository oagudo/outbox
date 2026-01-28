package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oagudo/outbox"
	"github.com/segmentio/kafka-go"
)

type kafkaHeaderKey string

const (
	kafkaTraceIDHeaderKey       kafkaHeaderKey = "trace_id"
	kafkaCorrelationIDHeaderKey kafkaHeaderKey = "correlation_id"
)

type Entity struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateEntityRequest struct {
}

type messagePublisher struct {
	kafkaWriter *kafka.Writer
}

func (p *messagePublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	msgMetadata := map[string]string{}
	if err := json.Unmarshal(msg.Metadata, &msgMetadata); err != nil {
		log.Printf("failed to unmarshal message metadata: %v", err)
		return err
	}
	headers := []kafka.Header{}
	for k, v := range msgMetadata {
		headers = append(headers, kafka.Header{
			Key:   string(k),
			Value: []byte(v),
		})
	}
	err := p.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:     []byte(msg.ID.String()),
		Value:   msg.Payload,
		Headers: headers,
	})
	if err != nil {
		log.Printf("failed to publish message: %v", err)
		return err
	}

	log.Printf("published message %s with content %s and metadata %s", msg.ID, string(msg.Payload), string(msg.Metadata))

	return nil
}

func main() {

	// Postgres setup
	db, err := sql.Open("pgx", "postgres://postgres:postgres@localhost:5432/outbox?sslmode=disable")
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	// Kafka setup
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:29092"},
		Topic:    "entity",
		Balancer: &kafka.LeastBytes{},
	})
	defer kafkaWriter.Close()

	// Outbox setup
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectPostgres)
	writer := outbox.NewWriter(dbCtx)
	reader := outbox.NewReader(dbCtx, &messagePublisher{kafkaWriter: kafkaWriter}, outbox.WithInterval(1*time.Second))
	reader.Start()
	defer reader.Stop(context.Background())
	r := http.NewServeMux()

	r.HandleFunc("/entity", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req CreateEntityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}

		entity := Entity{
			ID:        uuid.New(),
			CreatedAt: time.Now().UTC(),
		}
		entityAsJSON, err := json.Marshal(entity)
		if err != nil {
			http.Error(w, "failed to marshal entity", http.StatusInternalServerError)
			return
		}

		msgMetadata := map[string]string{
			string(kafkaTraceIDHeaderKey):       uuid.New().String(), // Add any metadata you need to the message (eg. trace_id, correlation_id, etc)
			string(kafkaCorrelationIDHeaderKey): uuid.New().String(),
		}
		msgMetadataAsJSON, err := json.Marshal(msgMetadata)
		if err != nil {
			http.Error(w, "failed to marshal message metadata", http.StatusInternalServerError)
			return
		}
		msg := outbox.NewMessage(entityAsJSON, outbox.WithCreatedAt(entity.CreatedAt), outbox.WithMetadata(msgMetadataAsJSON))
		err = writer.WriteOne(r.Context(), msg, func(ctx context.Context, tx outbox.TxQueryer) error {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO entity (id, created_at) VALUES ($1, $2)",
				entity.ID, entity.CreatedAt,
			)
			if err != nil {
				log.Printf("failed to write entity: %v", err)
				return err
			}
			return nil
		})
		if err != nil {
			http.Error(w, "failed to write entity", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write(entityAsJSON)
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("HTTP service started on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}
