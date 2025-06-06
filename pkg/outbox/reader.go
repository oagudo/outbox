package outbox

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oagudo/outbox/internal/delay"
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

// OpKind represents the type of operation that failed.
type OpKind uint8

const (
	OpRead    OpKind = iota // reading messages from the outbox
	OpUpdate                // updating a message in the outbox table
	OpPublish               // publishing messages to the external system
	OpDelete                // deleting messages from the outbox
)

// ReaderError represents an error that occurred during a reader operation.
type ReaderError struct {
	Op  OpKind
	Err error
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

	delayStrategy DelayStrategy
	maxDelay      time.Duration
	delay         time.Duration
	delayFunc     delay.DelayFunc

	started         int32
	closed          int32
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	errCh           chan ReaderError
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
			r.errCh = make(chan ReaderError, size)
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

// DelayStrategy represents the strategy for the delay between attempts to publish a message.
type DelayStrategy uint8

const (
	// DelayStrategyFixed means that the delay between attempts to publish a message is fixed.
	DelayStrategyFixed DelayStrategy = iota

	// DelayStrategyExponential means that the delay between attempts to publish a message is
	// exponential 2^n where n is the current attempt number.
	//
	// For example, with delay 200 miliseconds and maxDelay 1 hour:
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
	DelayStrategyExponential
)

// WithDelayStrategy sets the delay strategy for the outbox reader.
// Default is DelayStrategyExponential.
func WithDelayStrategy(delayStrategy DelayStrategy) ReaderOption {
	return func(r *Reader) {
		r.delayStrategy = delayStrategy
	}
}

// WithDelay sets the delay between attempts to publish a message.
// For FixedDelayStrategy, delay is the fixed delay between attempts.
// For ExponentialDelayStrategy, delay is the initial delay for the exponential delay function.
// Default is 200 milliseconds.
func WithDelay(delay time.Duration) ReaderOption {
	return func(r *Reader) {
		if delay >= 0 {
			r.delay = delay
		}
	}
}

// WithMaxDelay sets the maximum delay between attempts to publish a message.
// Only applicable if DelayStrategy is DelayStrategyExponential.
// Default is 1 hour.
func WithMaxDelay(maxDelay time.Duration) ReaderOption {
	return func(r *Reader) {
		if maxDelay >= 0 {
			r.maxDelay = maxDelay
		}
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
		delayStrategy:   DelayStrategyExponential,
		delay:           200 * time.Millisecond,
		maxDelay:        1 * time.Hour,
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.errCh == nil {
		r.errCh = make(chan ReaderError, 128)
	}

	if r.discardedMsgsCh == nil {
		r.discardedMsgsCh = make(chan Message, 128)
	}

	if r.delayStrategy == DelayStrategyExponential {
		r.delayFunc = delay.ExponentialDelay(r.delay, r.maxDelay)
	} else {
		r.delayFunc = delay.FixedDelay(r.delay)
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

// Errors returns a channel that receives errors from the outbox reader.
// The channel is buffered to prevent blocking the reader. If the buffer becomes
// full, subsequent errors will be dropped to maintain reader throughput.
// The channel is closed when the reader is stopped.
//
// Consumers should drain this channel promptly to avoid missing errors.
func (r *Reader) Errors() <-chan ReaderError {
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

func (r *Reader) sendError(err ReaderError) {
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
		r.sendError(ReaderError{Op: OpRead, Err: err})
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
	err = r.deleteMessagesInBatch(msgsToDelete)
	if err != nil {
		r.sendError(ReaderError{Op: OpDelete, Err: err})
	}
}

func (r *Reader) flushIfFull(batch []*Message) bool {
	if len(batch) < r.deleteBatchSize {
		return false
	}
	err := r.deleteMessagesInBatch(batch)
	if err == nil {
		return true
	}

	r.sendError(ReaderError{Op: OpDelete, Err: err})
	return false
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
	if err == nil {
		return true
	}

	r.sendError(ReaderError{Op: OpPublish, Err: err})

	err = r.scheduleNextAttempt(msg)
	if err != nil {
		r.sendError(ReaderError{Op: OpUpdate, Err: err})
	}

	return false
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
		return fmt.Errorf("failed to schedule next attempt for message %s: %w", msg.ID, err)
	}

	return nil
}

func (r *Reader) publishMessage(msg *Message) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.publishTimeout)
	defer cancel()

	return r.msgPublisher.Publish(ctx, msg)
}

func (r *Reader) deleteMessagesInBatch(batch []*Message) error {
	if len(batch) == 0 {
		return nil
	}

	// Do not use parent r.ctx, when reader is stopped we want to wait for the deletion to complete
	ctx, cancel := context.WithTimeout(context.Background(), r.deleteTimeout)
	defer cancel()

	placeholders := make([]string, 0, len(batch))
	ids := make([]any, 0, len(batch))
	for idx, msg := range batch {
		placeholders = append(placeholders, r.dbCtx.getSQLPlaceholder(idx+1))
		ids = append(ids, r.dbCtx.formatMessageIDForDB(msg))
	}
	// nolint:gosec
	query := fmt.Sprintf("DELETE FROM outbox WHERE id IN (%s)", strings.Join(placeholders, ", "))
	_, err := r.dbCtx.db.ExecContext(ctx, query, ids...)
	if err != nil {
		return fmt.Errorf("failed to delete messages from outbox: %w", err)
	}

	return nil
}

func (r *Reader) readOutboxMessages() ([]*Message, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.readTimeout)
	defer cancel()

	// nolint:gosec
	query := r.buildSelectMessagesQuery()
	rows, err := r.dbCtx.db.QueryContext(ctx, query, r.maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to read outbox messages: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var messages []*Message
	for rows.Next() {
		msg := &Message{}
		if err := rows.Scan(&msg.ID, &msg.Payload, &msg.CreatedAt, &msg.ScheduledAt, &msg.Metadata, &msg.TimesAttempted); err != nil {
			return nil, fmt.Errorf("failed to scan outbox message: %w", err)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unexpected error while scanning outbox messages: %w", err)
	}
	return messages, nil
}

func (r *Reader) buildSelectMessagesQuery() string {
	limitPlaceholder := r.dbCtx.getSQLPlaceholder(1)

	switch r.dbCtx.dialect {
	case SQLDialectOracle:
		return fmt.Sprintf(`SELECT id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox 
			WHERE scheduled_at <= %s 
			ORDER BY created_at ASC FETCH FIRST %s ROWS ONLY`, r.dbCtx.getCurrentTimestampInUTC(), limitPlaceholder)

	case SQLDialectSQLServer:
		return fmt.Sprintf(`SELECT TOP (%s) id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox 
			WHERE scheduled_at <= %s 
			ORDER BY created_at ASC`, limitPlaceholder, r.dbCtx.getCurrentTimestampInUTC())

	default:
		return fmt.Sprintf(`SELECT id, payload, created_at, scheduled_at, metadata, times_attempted 
			FROM outbox 
			WHERE scheduled_at <= %s
			ORDER BY created_at ASC LIMIT %s`, r.dbCtx.getCurrentTimestampInUTC(), limitPlaceholder)
	}
}
