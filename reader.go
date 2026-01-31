package outbox

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MessagePublisher defines an interface for publishing messages to an external system.
type MessagePublisher interface {
	// Publish sends a message to an external system (e.g., a message broker).
	// This function may be called multiple times for the same message.
	// Consumers must be idempotent and handle duplicate messages,
	// though some brokers provide deduplication features.
	// Return nil on success.
	// Return error on failure. In this case:
	// - The message will be retried according to the configured retry and backoff settings
	// - or will be discarded if the maximum number of attempts is reached.
	Publish(ctx context.Context, msg *Message) error
}

// Reader periodically reads unpublished messages from the outbox table
// and attempts to publish them to an external system.
type Reader struct {
	dbCtx        *DBContext
	msgPublisher MessagePublisher

	interval        time.Duration
	readTimeout     time.Duration
	publishTimeout  time.Duration
	deleteTimeout   time.Duration
	updateTimeout   time.Duration
	maxMessages     int
	deleteBatchSize int
	maxAttempts     int32
	delayFunc       DelayFunc

	started         int32
	closed          int32
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	errCh           chan error
	discardedMsgsCh chan Message
}

// ReaderOption is a function that configures a Reader instance.
type ReaderOption func(*Reader)

// WithInterval sets the time between outbox reader processing attempts.
// Default is 10 seconds.
func WithInterval(interval time.Duration) ReaderOption {
	return func(r *Reader) {
		r.interval = interval
	}
}

// WithReadTimeout sets the timeout for reading messages from the outbox.
// Default is 5 seconds.
func WithReadTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.readTimeout = timeout
	}
}

// WithPublishTimeout sets the timeout for publishing messages to the external system.
// Default is 5 seconds.
func WithPublishTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.publishTimeout = timeout
	}
}

// WithDeleteTimeout sets the timeout for deleting messages from the outbox.
// Default is 5 seconds.
func WithDeleteTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.deleteTimeout = timeout
	}
}

// WithUpdateTimeout sets the timeout for updating a message in the outbox table.
// Default is 5 seconds.
func WithUpdateTimeout(timeout time.Duration) ReaderOption {
	return func(r *Reader) {
		r.updateTimeout = timeout
	}
}

// WithReadBatchSize sets the maximum number of messages to process in a single batch.
// Default is 100 messages. Must be positive.
func WithReadBatchSize(batchSize int) ReaderOption {
	return func(r *Reader) {
		if batchSize > 0 {
			r.maxMessages = batchSize
		}
	}
}

// WithErrorChannelSize sets the size of the error channel.
// Default is 128. Size must be positive.
func WithErrorChannelSize(size int) ReaderOption {
	return func(r *Reader) {
		if size > 0 {
			r.errCh = make(chan error, size)
		}
	}
}

// WithMaxAttempts sets the maximum number of attempts to publish a message.
// Message is discarded if max attempts is reached. Users can use `DiscardedMessages`
// function to get a channel and be notified about any message discarded.
// Default is math.MaxInt32. Must be positive.
func WithMaxAttempts(maxAttempts int32) ReaderOption {
	return func(r *Reader) {
		if maxAttempts > 0 {
			r.maxAttempts = maxAttempts
		}
	}
}

// WithDiscardedMessagesChannelSize sets the size of the discarded messages channel.
// Default is 128. Size must be positive.
func WithDiscardedMessagesChannelSize(size int) ReaderOption {
	return func(r *Reader) {
		if size > 0 {
			r.discardedMsgsCh = make(chan Message, size)
		}
	}
}

// WithDeleteBatchSize sets the number of successfully published messages to accumulate
// before executing a batch delete operation from the outbox table.
//
// The reader processes messages sequentially: for each message, it attempts to publish
// and then accumulates successfully published messages for deletion. When the batch
// reaches the specified size, all messages in the batch are deleted in a single
// database operation.
//
// Performance considerations:
//   - Larger batch sizes reduce database round trips but increase memory usage
//   - Smaller batch sizes provide faster cleanup but more frequent database operations
//   - A batch size of 1 deletes each message immediately after successful publication
//
// Behavior:
//   - Only successfully published messages are added to the delete batch
//   - Failed publications do not affect the batch; those messages remain in the outbox
//   - At the end of each processing cycle, any remaining messages in the batch are
//     deleted regardless of batch size to prevent reprocessing
//   - If fewer messages exist than the batch size, they are still deleted in one operation
//
// Default is 20. Size must be positive.
func WithDeleteBatchSize(size int) ReaderOption {
	return func(r *Reader) {
		if size > 0 {
			r.deleteBatchSize = size
		}
	}
}

// WithExponentialDelay	sets the delay between attempts to publish a message to be exponential.
// The delay is 2^n where n is the current attempt number.
//
// For example, with initialDelay of 200 milliseconds and maxDelay of 1 hour:
//
// Delay after attempt 0: 200ms
// Delay after attempt 1: 400ms
// Delay after attempt 2: 800ms
// Delay after attempt 3: 1.6s
// Delay after attempt 4: 3.2s
// Delay after attempt 5: 6.4s
// Delay after attempt 6: 12.8s
// Delay after attempt 7: 25.6s
// Delay after attempt 8: 51.2s
// Delay after attempt 9: 1m42.4s
// Delay after attempt 10: 3m24.8s
// Delay after attempt 11: 6m49.6s
// Delay after attempt 12: 13m39.2s
// Delay after attempt 13: 27m18.4s
// Delay after attempt 14: 54m36.8s
// Delay after attempt 15: 1h0m0s
// Delay after attempt 16: 1h0m0s
// ...
func WithExponentialDelay(initialDelay time.Duration, maxDelay time.Duration) ReaderOption {
	return WithDelay(Exponential(initialDelay, maxDelay))
}

// WithFixedDelay sets the delay between attempts to publish a message to be fixed.
// The delay is the same for all attempts.
//
// For example, with delay of 200 miliseconds:
//
// Delay after attempt 0: 200ms
// Delay after attempt 1: 200ms
// ...
func WithFixedDelay(delay time.Duration) ReaderOption {
	return WithDelay(Fixed(delay))
}

// WithDelay sets the delay function to apply between attempts to publish a message.
// Default is ExponentialDelay(200ms, 1h).
func WithDelay(delayFunc DelayFunc) ReaderOption {
	return func(r *Reader) {
		r.delayFunc = delayFunc
	}
}

// NewReader creates a new outbox Reader with the given database context,
// message publisher, and options.
func NewReader(dbCtx *DBContext, msgPublisher MessagePublisher, opts ...ReaderOption) *Reader {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Reader{
		dbCtx:           dbCtx,
		msgPublisher:    msgPublisher,
		ctx:             ctx,
		cancel:          cancel,
		interval:        10 * time.Second,
		readTimeout:     5 * time.Second,
		publishTimeout:  5 * time.Second,
		deleteTimeout:   5 * time.Second,
		updateTimeout:   5 * time.Second,
		maxMessages:     100,
		deleteBatchSize: 20,
		maxAttempts:     math.MaxInt32,
		delayFunc:       Exponential(200*time.Millisecond, 1*time.Hour),
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.errCh == nil {
		r.errCh = make(chan error, 128)
	}

	if r.discardedMsgsCh == nil {
		r.discardedMsgsCh = make(chan Message, 128)
	}

	return r
}

// Start begins the background processing of outbox messages.
// It periodically reads unpublished messages and attempts to publish them.
// If Start is called multiple times, only the first call has an effect.
func (r *Reader) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}

	r.wg.Add(1)
	go func() {
		ticker := time.NewTicker(r.interval)

		defer r.wg.Done()
		defer close(r.errCh)
		defer close(r.discardedMsgsCh)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.publishMessages()
			case <-r.ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully shuts down the outbox reader processing.
// It prevents new reader cycles from starting and waits for any ongoing
// message publishing to complete. The provided context controls how long to wait
// for graceful shutdown before giving up.
//
// If the context expires before processing completes, Stop returns the context's
// error. If shutdown completes successfully, it returns nil.
// Calling Stop multiple times is safe and only the first call has an effect.
func (r *Reader) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil
	}

	r.cancel() // signal stop

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishError indicates an error during message publication.
// It includes the message that failed to be published and the original error.
type PublishError struct {
	Message Message
	Err     error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("publishing message %s: %v", e.Message.ID, e.Err)
}
func (e *PublishError) Unwrap() error { return e.Err }

// UpdateError indicates an error when updating a message in the outbox.
// It includes the message that failed to be updated and the original error.
type UpdateError struct {
	Message Message
	Err     error
}

func (e *UpdateError) Error() string {
	return fmt.Sprintf("updating message %s: %v", e.Message.ID, e.Err)
}
func (e *UpdateError) Unwrap() error { return e.Err }

// DeleteError indicates an error during the batch deletion of messages.
// It includes the messages that failed to be deleted and the original error.
type DeleteError struct {
	Messages []Message
	Err      error
}

func (e *DeleteError) Error() string {
	return fmt.Sprintf("deleting %d messages: %v", len(e.Messages), e.Err)
}
func (e *DeleteError) Unwrap() error { return e.Err }

// ReadError indicates an error when reading messages from the outbox.
type ReadError struct {
	Err error
}

func (e *ReadError) Error() string { return fmt.Sprintf("reading outbox messages: %v", e.Err) }

func (e *ReadError) Unwrap() error { return e.Err }

// Errors returns a channel that receives errors from the outbox reader.
// The channel is buffered to prevent blocking the reader. If the buffer becomes
// full, subsequent errors will be dropped to maintain reader throughput.
// The channel is closed when the reader is stopped.
//
// The returned error will be one of the following types, which can be checked
// using a type switch:
//   - *PublishError: Failed to publish a message. Contains the message.
//   - *UpdateError:  Failed to update a message after a failed publish attempt.
//     Contains the message.
//   - *DeleteError:  Failed to delete a batch of messages. Contains the messages.
//   - *ReadError:    Failed to read messages from the outbox.
//
// Example of error handling:
//
//	for err := range r.Errors() {
//		switch e := err.(type) {
//		case *outbox.PublishError:
//			log.Printf("Failed to publish message | ID: %s | Error: %v",
//				e.Message.ID, e.Err)
//
//		case *outbox.UpdateError:
//			log.Printf("Failed to update message | ID: %s | Error: %v",
//				e.Message.ID, e.Err)
//
//		case *outbox.DeleteError:
//			log.Printf("Batch message deletion failed | Count: %d | Error: %v",
//				len(e.Messages), e.Err)
//			for _, msg := range e.Messages {
//				log.Printf("Failed to delete message | ID: %s", msg.ID)
//			}
//
//		case *outbox.ReadError:
//			log.Printf("Failed to read outbox messages | Error: %v", e.Err)
//
//		default:
//			log.Printf("Unexpected error occurred | Error: %v", e)
//		}
//	}
func (r *Reader) Errors() <-chan error {
	return r.errCh
}

// DiscardedMessages returns a channel that receives messages that were discarded
// because they reached the maximum number of attempts.
// The channel is closed when the reader is stopped.
//
// Consumers should drain this channel promptly to avoid missing messages.
func (r *Reader) DiscardedMessages() <-chan Message {
	return r.discardedMsgsCh
}

func (r *Reader) sendError(err error) {
	select {
	case r.errCh <- err:
	default:
		// Channel buffer full, drop the error to prevent blocking
	}
}

func (r *Reader) sendDiscardedMessage(msg *Message) {
	select {
	case r.discardedMsgsCh <- *msg:
	default:
		// Channel buffer full, drop the message to prevent blocking
	}
}

func (r *Reader) publishMessages() {
	msgs, err := r.readOutboxMessages()
	if err != nil {
		r.sendError(&ReadError{Err: err})
		return
	}

	msgsToDelete := make([]*Message, 0, r.deleteBatchSize)

	for _, msg := range msgs {
		if r.handleMessage(msg) {
			msgsToDelete = append(msgsToDelete, msg)
		}

		if r.flushIfFull(msgsToDelete) {
			msgsToDelete = make([]*Message, 0, r.deleteBatchSize)
		}
	}

	// delete remaining messages as next tick would read them again otherwise
	err = r.deleteMessages(msgsToDelete)
	if err != nil {
		r.sendError(&DeleteError{Messages: copyMessages(msgsToDelete), Err: err})
	}
}

func copyMessages(msgs []*Message) []Message {
	copied := make([]Message, len(msgs))
	for i, msg := range msgs {
		copied[i] = *msg
	}
	return copied
}

func (r *Reader) flushIfFull(msgsToDelete []*Message) bool {
	if len(msgsToDelete) < r.deleteBatchSize {
		return false
	}
	err := r.deleteMessages(msgsToDelete)
	if err != nil {
		r.sendError(&DeleteError{Messages: copyMessages(msgsToDelete), Err: err})
		return false
	}

	return true
}

func (r *Reader) handleMessage(msg *Message) bool {
	// db clock used in the query is ahead of the reader clock, skip the message
	if msg.ScheduledAt.After(time.Now().UTC()) {
		return false
	}

	if msg.TimesAttempted >= r.maxAttempts {
		r.sendDiscardedMessage(msg)
		return true // mark for deletion
	}

	err := r.publishMessage(msg)
	if err != nil {
		r.sendError(&PublishError{Message: *msg, Err: err})

		err = r.scheduleNextAttempt(msg)
		if err != nil {
			r.sendError(&UpdateError{Message: *msg, Err: err})
		}
		return false
	}

	return true
}

func (r *Reader) scheduleNextAttempt(msg *Message) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.updateTimeout)
	defer cancel()

	delay := r.delayFunc(int(msg.TimesAttempted))
	nextScheduledAt := time.Now().UTC().Add(delay)

	// nolint:gosec
	query := fmt.Sprintf("UPDATE outbox SET times_attempted = times_attempted + 1, scheduled_at = %s WHERE id = %s",
		r.dbCtx.getSQLPlaceholder(1), r.dbCtx.getSQLPlaceholder(2))
	_, err := r.dbCtx.db.ExecContext(ctx, query, nextScheduledAt, r.dbCtx.formatMessageIDForDB(msg))
	if err != nil {
		return fmt.Errorf("scheduling next attempt for message %s: %w", msg.ID, err)
	}

	return nil
}

func (r *Reader) publishMessage(msg *Message) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.publishTimeout)
	defer cancel()

	return r.msgPublisher.Publish(ctx, msg)
}

func (r *Reader) deleteMessages(msgsToDelete []*Message) error {
	if len(msgsToDelete) == 0 {
		return nil
	}

	// Do not use parent r.ctx, when reader is stopped we want to wait for the deletion to complete
	ctx, cancel := context.WithTimeout(context.Background(), r.deleteTimeout)
	defer cancel()

	placeholders := make([]string, 0, len(msgsToDelete))
	ids := make([]any, 0, len(msgsToDelete))
	for idx, msg := range msgsToDelete {
		placeholders = append(placeholders, r.dbCtx.getSQLPlaceholder(idx+1))
		ids = append(ids, r.dbCtx.formatMessageIDForDB(msg))
	}
	// nolint:gosec
	query := fmt.Sprintf("DELETE FROM outbox WHERE id IN (%s)", strings.Join(placeholders, ", "))
	_, err := r.dbCtx.db.ExecContext(ctx, query, ids...)

	return err
}

func (r *Reader) readOutboxMessages() ([]*Message, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.readTimeout)
	defer cancel()

	// nolint:gosec
	query := r.dbCtx.buildSelectMessagesQuery()
	rows, err := r.dbCtx.db.QueryContext(ctx, query, r.maxMessages)
	if err != nil {
		return nil, fmt.Errorf("querying outbox messages: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var messages []*Message
	for rows.Next() {
		msg := &Message{}
		if err := rows.Scan(&msg.ID, &msg.Payload, &msg.CreatedAt, &msg.ScheduledAt, &msg.Metadata, &msg.TimesAttempted); err != nil {
			return nil, fmt.Errorf("scanning outbox message: %w", err)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating outbox messages: %w", err)
	}
	return messages, nil
}
