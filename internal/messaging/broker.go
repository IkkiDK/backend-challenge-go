package messaging

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/IkkiDK/backend-challenge-go/internal/config"
	"github.com/IkkiDK/backend-challenge-go/internal/outbox"
)

// Broker is the SQS connection shared by the consumer, the outbox relay and
// the readiness check. Queue URLs are resolved by Connect at startup, before
// any of them runs.
type Broker struct {
	client *sqs.Client
	cfg    config.SQS

	mu     sync.RWMutex
	queues Queues
}

// NewBroker returns a broker that is not connected yet.
func NewBroker(client *sqs.Client, cfg config.SQS) *Broker {
	return &Broker{client: client, cfg: cfg}
}

// Connect resolves the queues, proving the credentials and policies allow it.
func (b *Broker) Connect(ctx context.Context) error {
	queues, err := ResolveQueues(ctx, b.client, b.cfg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queues = queues
	return nil
}

// Queues returns the resolved queues.
func (b *Broker) Queues() Queues {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queues
}

// Client returns the SQS client.
func (b *Broker) Client() *sqs.Client {
	return b.client
}

// Check verifies that the inbound queue answers.
func (b *Broker) Check(ctx context.Context) error {
	_, err := b.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(b.Queues().InboundURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}

// Publish implements outbox.Publisher on the events FIFO queue.
//
// Routing contract: MessageGroupId is the wallet id, so the events of one
// wallet are delivered in order; MessageDeduplicationId is the event id, so a
// republication within the 5-minute SQS window is dropped by the broker.
// Consumers must still deduplicate by eventId beyond that window.
func (b *Broker) Publish(ctx context.Context, e outbox.Event) error {
	_, err := b.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(b.Queues().EventsURL),
		MessageBody:            aws.String(string(e.Payload)),
		MessageGroupId:         aws.String(e.OrderingKey),
		MessageDeduplicationId: aws.String(e.ID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventId":      stringAttribute(e.ID.String()),
			"eventType":    stringAttribute(e.EventType),
			"eventVersion": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(e.EventVersion))},
			"aggregateId":  stringAttribute(e.AggregateID),
		},
	})
	return err
}
