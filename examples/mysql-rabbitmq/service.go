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

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/oagudo/outbox/pkg/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitHeaderKey string

const (
	rabbitTraceIDHeaderKey       rabbitHeaderKey = "trace_id"
	rabbitCorrelationIDHeaderKey rabbitHeaderKey = "correlation_id"
)

type Entity struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateEntityRequest struct {
}

type messagePublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
}

func (p *messagePublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	msgMetadata := map[string]string{}
	if err := json.Unmarshal(msg.Metadata, &msgMetadata); err != nil {
		log.Printf("failed to unmarshal message metadata: %v", err)
		return err
	}

	headers := amqp.Table{}
	for k, v := range msgMetadata {
		headers[k] = v
	}

	err := p.channel.PublishWithContext(
		ctx,
		"",      // exchange
		p.queue, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         msg.Payload,
			MessageId:    msg.ID.String(),
			Headers:      headers,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		log.Printf("failed to publish message: %v", err)
		return err
	}

	log.Printf("published message %s with content %s and metadata %v", msg.ID, string(msg.Payload), string(msg.Metadata))

	return nil
}

func main() {

	// MySQL setup
	dsn := "user:password@tcp(localhost:3306)/outbox?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("failed to connect to mysql: %v", err)
	}
	defer db.Close()

	// RabbitMQ setup
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}
	defer channel.Close()

	// Declare queue
	q, err := channel.QueueDeclare(
		"entity", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		log.Fatalf("failed to declare a queue: %v", err)
	}

	// Outbox setup
	dbCtx := outbox.NewDBContext(db, outbox.SQLDialectMySQL)
	writer := outbox.NewWriter(dbCtx)
	reader := outbox.NewReader(dbCtx, &messagePublisher{
		conn:    conn,
		channel: channel,
		queue:   q.Name,
	}, outbox.WithInterval(1*time.Second))
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
			string(rabbitTraceIDHeaderKey):       uuid.New().String(), // Add any metadata you need to the message (eg. trace_id, correlation_id, etc)
			string(rabbitCorrelationIDHeaderKey): uuid.New().String(),
		}
		msgMetadataJSON, err := json.Marshal(msgMetadata)
		if err != nil {
			http.Error(w, "failed to marshal message metadata", http.StatusInternalServerError)
			return
		}
		msg := outbox.NewMessage(entityAsJSON, outbox.WithCreatedAt(entity.CreatedAt), outbox.WithMetadata(msgMetadataJSON))
		err = writer.Write(r.Context(), msg, func(ctx context.Context, execInTx outbox.ExecInTxFunc) error {
			_, err := execInTx(ctx,
				"INSERT INTO entity (id, created_at) VALUES (?, ?)",
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
