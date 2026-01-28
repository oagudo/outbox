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
	"github.com/nats-io/nats.go"
	"github.com/oagudo/outbox"
	_ "github.com/sijms/go-ora/v2"
)

type natsHeaderKey string

const (
	natsTraceIDHeaderKey       natsHeaderKey = "trace_id"
	natsCorrelationIDHeaderKey natsHeaderKey = "correlation_id"
)

type Entity struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateEntityRequest struct {
}

type messagePublisher struct {
	natsConn *nats.Conn
}

func (p *messagePublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	msgMetadata := map[string]string{}
	if err := json.Unmarshal(msg.Metadata, &msgMetadata); err != nil {
		log.Printf("failed to unmarshal message metadata: %v", err)
		return err
	}

	// Create NATS message with headers
	natsMsg := &nats.Msg{
		Subject: "entity",
		Data:    msg.Payload,
		Header:  make(nats.Header),
	}

	// Add message ID as a header
	natsMsg.Header.Set("message_id", msg.ID.String())

	// Add metadata as headers
	for k, v := range msgMetadata {
		natsMsg.Header.Set(k, v)
	}

	err := p.natsConn.PublishMsg(natsMsg)
	if err != nil {
		log.Printf("failed to publish message: %v", err)
		return err
	}

	log.Printf("published message %s with content %s and metadata %s", msg.ID, string(msg.Payload), string(msg.Metadata))

	return nil
}

func main() {

	// Oracle setup
	db, err := sql.Open("oracle", "oracle://app_user:pass@localhost:1521/FREEPDB1")
	if err != nil {
		log.Fatalf("failed to connect to oracle: %v", err)
	}
	defer db.Close()

	// NATS setup
	natsConn, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatalf("failed to connect to NATS: %v", err)
	}
	defer natsConn.Close()

	// Outbox setup
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectOracle)
	writer := outbox.NewWriter(dbCtx)
	reader := outbox.NewReader(dbCtx, &messagePublisher{natsConn: natsConn}, outbox.WithInterval(1*time.Second))
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
			string(natsTraceIDHeaderKey):       uuid.New().String(), // Add any metadata you need to the message (eg. trace_id, correlation_id, etc)
			string(natsCorrelationIDHeaderKey): uuid.New().String(),
		}
		msgMetadataAsJSON, err := json.Marshal(msgMetadata)
		if err != nil {
			http.Error(w, "failed to marshal message metadata", http.StatusInternalServerError)
			return
		}
		msg := outbox.NewMessage(entityAsJSON, outbox.WithCreatedAt(entity.CreatedAt), outbox.WithMetadata(msgMetadataAsJSON))
		err = writer.WriteOne(r.Context(), msg, func(ctx context.Context, tx outbox.TxQueryer) error {
			_, err := tx.ExecContext(ctx,
				"INSERT INTO entity (id, created_at) VALUES (:1, :2)",
				entity.ID[:], entity.CreatedAt,
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
