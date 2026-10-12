package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

const (
	// maxMessagesPerReceive is the SQS maximum for one ReceiveMessage call.
	maxMessagesPerReceive = 10
	// sqsCallTimeout bounds delete, visibility and dead-letter calls.
	sqsCallTimeout = 5 * time.Second
	// receiveErrorBackoff spaces receive retries while SQS is unreachable.
	receiveErrorBackoff = 2 * time.Second
	// maxDeadLetterErrorLength keeps the error attribute well under SQS limits.
	maxDeadLetterErrorLength = 512
)

// Consumer handles wager-transactions.fifo through the shared submit use case.
//
// A message is deleted only after the SQL transaction that recorded its
// outcome (and its inbox entry) committed. Business rejections are outcomes,
// so they are deleted too. Transient failures leave the message in the queue
// with an exponential visibility backoff; after the queue's maxReceiveCount
// SQS moves it to the dead-letter queue. Permanent failures (invalid
// messages, conflicts, unknown wallets) are dead-lettered at once.
//
// Messages of one group (one wallet) are handled in order, groups in
// parallel. On Stop the consumer starts no new receive, lets the long poll in
// flight return (at most cfg.WaitTime) and hands back what it brought,
// finishes in-flight messages within the deadline and makes the remaining
// ones visible again. The poll is not cancelled: a long poll abandoned by its
// client can still receive messages server-side, which would then stay
// invisible for the whole visibility timeout.
type Consumer struct {
	client  *sqs.Client
	broker  *Broker
	queues  Queues
	service *app.Service
	cfg     config.Consumer
	clock   func() time.Time
	logger  *slog.Logger
	metrics *observability.Metrics

	stopping   chan struct{}
	stopOnce   sync.Once
	work       context.Context
	cancelWork context.CancelFunc
	done       chan struct{}
}

// NewConsumer returns a consumer that is not started yet.
func NewConsumer(broker *Broker, service *app.Service, cfg config.Consumer, clock func() time.Time, logger *slog.Logger, metrics *observability.Metrics) *Consumer {
	work, cancelWork := context.WithCancel(context.Background())
	return &Consumer{
		client: broker.Client(), broker: broker, service: service, cfg: cfg, clock: clock,
		logger: logger.With("component", "consumer"), metrics: metrics,
		stopping: make(chan struct{}), work: work, cancelWork: cancelWork, done: make(chan struct{}),
	}
}

// Start checks that a message is always handled before its visibility
// timeout expires, then receives in the background. The broker must be
// connected.
func (c *Consumer) Start() error {
	c.queues = c.broker.Queues()
	if c.cfg.ProcessingTimeout >= c.queues.VisibilityTimeout {
		return fmt.Errorf("CONSUMER_PROCESSING_TIMEOUT (%v) must be below the queue visibility timeout (%v)", c.cfg.ProcessingTimeout, c.queues.VisibilityTimeout)
	}
	c.logger.Info("consumer started", "queue", c.queues.InboundURL,
		"visibilityTimeout", c.queues.VisibilityTimeout.String(), "maxReceiveCount", c.queues.MaxReceiveCount)
	go c.run()
	return nil
}

// Stop waits until the receive in flight returned and the in-flight messages
// were handled or released. If ctx expires first, it cancels them (their SQL
// transactions roll back and they are released for redelivery).
func (c *Consumer) Stop(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stopping) })
	defer c.logger.Info("consumer stopped")
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.cancelWork()
		<-c.done
		return fmt.Errorf("consumer cancelled in-flight messages: %w", ctx.Err())
	}
}

func (c *Consumer) run() {
	defer close(c.done)
	for !c.isStopping() {
		out, err := c.client.ReceiveMessage(c.work, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queues.InboundURL),
			MaxNumberOfMessages: maxMessagesPerReceive,
			WaitTimeSeconds:     int32(c.cfg.WaitTime / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
			},
			MessageAttributeNames: []string{correlationAttribute},
		})
		if err != nil {
			if c.work.Err() != nil {
				return
			}
			c.metrics.Retries.WithLabelValues("sqs_receive").Inc()
			c.logger.Warn("receive failed; retrying", "error", err)
			select {
			case <-c.stopping:
			case <-time.After(receiveErrorBackoff):
			}
			continue
		}
		if c.isStopping() {
			c.release(out.Messages)
			return
		}
		c.handleBatch(out.Messages)
	}
}

// handleBatch keeps FIFO order inside each message group and runs groups
// concurrently, at most cfg.Concurrency at a time.
func (c *Consumer) handleBatch(messages []types.Message) {
	var (
		groups  = groupMessages(messages)
		slots   = make(chan struct{}, c.cfg.Concurrency)
		running sync.WaitGroup
	)
	for _, group := range groups {
		slots <- struct{}{}
		running.Add(1)
		go func() {
			defer running.Done()
			defer func() { <-slots }()
			for i, m := range group {
				if c.isStopping() {
					c.release(group[i:])
					return
				}
				c.handle(m)
			}
		}()
	}
	running.Wait()
}

func groupMessages(messages []types.Message) [][]types.Message {
	var (
		order  []string
		groups = make(map[string][]types.Message)
	)
	for _, m := range messages {
		group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, seen := groups[group]; !seen {
			order = append(order, group)
		}
		groups[group] = append(groups[group], m)
	}
	ordered := make([][]types.Message, len(order))
	for i, group := range order {
		ordered[i] = groups[group]
	}
	return ordered
}

func (c *Consumer) isStopping() bool {
	select {
	case <-c.stopping:
		return true
	default:
		return false
	}
}

func (c *Consumer) handle(m types.Message) {
	receivedAt := c.clock()
	ctx, cancel := context.WithTimeout(c.work, c.cfg.ProcessingTimeout)
	defer cancel()
	ctx = observability.WithLogAttrs(ctx, slog.String("sqsMessageId", aws.ToString(m.MessageId)))

	message, err := decodeMessage(aws.ToString(m.Body), m.MessageAttributes)
	if err != nil {
		c.deadLetter(ctx, m, "invalid_message", err)
		return
	}
	ctx = observability.WithLogAttrs(ctx,
		slog.String(observability.KeyMessageID, message.messageID),
		slog.String(observability.KeyCorrelationID, message.correlationID),
		slog.String(observability.KeyProviderID, message.request.ProviderID().String()))

	result, err := c.service.Submit(ctx, message.request, app.Origin{
		Channel:       app.ChannelSQS,
		CorrelationID: message.correlationID,
		Message: &app.InboundMessage{
			Consumer:   c.cfg.Name,
			MessageID:  message.messageID,
			Hash:       message.hash,
			ReceivedAt: receivedAt,
		},
	})
	switch {
	case err == nil && result.Transaction.Status() == domain.StatusFailed:
		// Recorded for audit, but surfaced to operators like any permanent error.
		c.deadLetter(ctx, m, "processing_failed", fmt.Errorf("transaction %s failed permanently", result.Transaction.ID()))
	case err == nil:
		c.complete(ctx, m, result)
	case c.work.Err() != nil:
		c.release([]types.Message{m})
	case isPermanent(err):
		c.deadLetter(ctx, m, deadLetterReason(err), err)
	default:
		c.retryLater(ctx, m, err)
	}
}

// isPermanent reports failures that no redelivery can fix.
func isPermanent(err error) bool {
	return errors.Is(err, app.ErrWalletNotFound) ||
		errors.Is(err, app.ErrMessageConflict) ||
		errors.Is(err, domain.ErrIdempotencyKeyReused) ||
		errors.Is(err, domain.ErrExternalTransactionConflict) ||
		errors.Is(err, domain.ErrInvalidValue)
}

func deadLetterReason(err error) string {
	switch {
	case errors.Is(err, app.ErrWalletNotFound):
		return "wallet_not_found"
	case errors.Is(err, app.ErrMessageConflict):
		return "message_id_reused"
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		return "idempotency_key_reused"
	case errors.Is(err, domain.ErrExternalTransactionConflict):
		return "external_transaction_conflict"
	default:
		return "invalid_message"
	}
}

// complete deletes a message whose outcome is committed. If the delete is
// lost, the redelivery is answered from the inbox without reprocessing.
func (c *Consumer) complete(ctx context.Context, m types.Message, result app.SubmitResult) {
	outcome := "processed"
	if result.Replayed {
		outcome = "duplicate"
	}
	c.metrics.SQSMessages.WithLabelValues(outcome).Inc()
	if err := c.deleteMessage(ctx, m); err != nil {
		c.logger.WarnContext(ctx, "delete failed after commit; the redelivery will be deduplicated", "error", err)
	}
}

// retryLater keeps the message invisible for an exponential backoff based on
// how often it was received. Once the queue's maxReceiveCount is reached, SQS
// moves it to the dead-letter queue on the next receive.
func (c *Consumer) retryLater(ctx context.Context, m types.Message, cause error) {
	receives, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	delay := c.cfg.RetryBaseDelay
	for i := 1; i < receives && delay < c.cfg.RetryMaxDelay; i++ {
		delay *= 2
	}
	delay = min(delay, c.cfg.RetryMaxDelay)
	c.metrics.SQSMessages.WithLabelValues("retried").Inc()
	c.metrics.Retries.WithLabelValues("sqs_message").Inc()
	c.logger.WarnContext(ctx, "message failed transiently; retrying later",
		"receives", receives, "maxReceives", c.queues.MaxReceiveCount, "retryIn", delay.String(), "error", cause)
	if err := c.changeVisibility(m, delay); err != nil {
		c.logger.WarnContext(ctx, "could not delay the message; it reappears after the visibility timeout", "error", err)
	}
}

// deadLetter copies the message to the dead-letter queue with the reason and
// deletes it. If the copy fails the message is retried later instead.
func (c *Consumer) deadLetter(ctx context.Context, m types.Message, reason string, cause error) {
	detail := cause.Error()
	if len(detail) > maxDeadLetterErrorLength {
		detail = detail[:maxDeadLetterErrorLength]
	}
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = reason
	}
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sqsCallTimeout)
	defer cancel()
	_, err := c.client.SendMessage(callCtx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DeadLetterURL),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: m.MessageId,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"deadLetterReason": stringAttribute(reason),
			"deadLetterError":  stringAttribute(detail),
			"sourceMessageId":  stringAttribute(aws.ToString(m.MessageId)),
		},
	})
	if err != nil {
		c.retryLater(ctx, m, fmt.Errorf("dead-letter %s: %w", reason, err))
		return
	}
	c.metrics.SQSMessages.WithLabelValues("dead_lettered").Inc()
	c.metrics.DeadLettered.WithLabelValues(reason).Inc()
	c.logger.ErrorContext(ctx, "message dead-lettered", "reason", reason, "error", cause)
	if err := c.deleteMessage(ctx, m); err != nil {
		c.logger.WarnContext(ctx, "delete failed after dead-lettering; the redelivery is dead-lettered again", "error", err)
	}
}

// release makes unhandled messages visible again for another consumer.
func (c *Consumer) release(messages []types.Message) {
	for _, m := range messages {
		c.metrics.SQSMessages.WithLabelValues("released").Inc()
		if err := c.changeVisibility(m, 0); err != nil {
			c.logger.Warn("could not release message; it reappears after the visibility timeout",
				"sqsMessageId", aws.ToString(m.MessageId), "error", err)
		}
	}
}

func (c *Consumer) deleteMessage(ctx context.Context, m types.Message) error {
	callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sqsCallTimeout)
	defer cancel()
	_, err := c.client.DeleteMessage(callCtx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queues.InboundURL),
		ReceiptHandle: m.ReceiptHandle,
	})
	return err
}

func (c *Consumer) changeVisibility(m types.Message, delay time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), sqsCallTimeout)
	defer cancel()
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queues.InboundURL),
		ReceiptHandle:     m.ReceiptHandle,
		VisibilityTimeout: int32(delay / time.Second),
	})
	return err
}
