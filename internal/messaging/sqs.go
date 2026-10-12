// Package messaging connects the service to Amazon SQS: it consumes
// wager-transactions.fifo and publishes outbox events to wallet-events.fifo.
// Access is granted by broker credentials and IAM policies; the consumer still
// applies every domain validation to what it receives.
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

// NewClient builds an SQS client from the standard AWS configuration chain
// (environment variables, or a shared credentials file and profile).
func NewClient(ctx context.Context, cfg config.SQS) (*sqs.Client, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return sqs.NewFromConfig(awsConfig, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// Queues holds the resolved queue URLs and the inbound redrive settings.
type Queues struct {
	InboundURL        string
	DeadLetterURL     string
	EventsURL         string
	VisibilityTimeout time.Duration
	// MaxReceiveCount is the number of receives after which SQS itself moves
	// a message to the dead-letter queue.
	MaxReceiveCount int
}

// ResolveQueues looks up the queues, which also proves the credentials can
// reach them, and reads the inbound visibility timeout and redrive policy.
func ResolveQueues(ctx context.Context, client *sqs.Client, cfg config.SQS) (Queues, error) {
	var (
		queues Queues
		err    error
	)
	for _, q := range []struct {
		name string
		url  *string
	}{
		{cfg.InboundQueue, &queues.InboundURL},
		{cfg.DeadLetterQueue, &queues.DeadLetterURL},
		{cfg.EventsQueue, &queues.EventsURL},
	} {
		out, lookupErr := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(q.name)})
		if lookupErr != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", q.name, lookupErr)
		}
		*q.url = aws.ToString(out.QueueUrl)
	}

	attributes, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queues.InboundURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameVisibilityTimeout, types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		return Queues{}, fmt.Errorf("read attributes of %s: %w", cfg.InboundQueue, err)
	}
	seconds, err := strconv.Atoi(attributes.Attributes[string(types.QueueAttributeNameVisibilityTimeout)])
	if err != nil {
		return Queues{}, fmt.Errorf("visibility timeout of %s: %w", cfg.InboundQueue, err)
	}
	queues.VisibilityTimeout = time.Duration(seconds) * time.Second

	var redrive struct {
		MaxReceiveCount json.Number `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(attributes.Attributes[string(types.QueueAttributeNameRedrivePolicy)]), &redrive); err != nil {
		return Queues{}, fmt.Errorf("%s has no redrive policy to a dead-letter queue: %w", cfg.InboundQueue, err)
	}
	maxReceives, err := redrive.MaxReceiveCount.Int64()
	if err != nil || maxReceives < 1 {
		return Queues{}, fmt.Errorf("invalid maxReceiveCount %q on %s", redrive.MaxReceiveCount, cfg.InboundQueue)
	}
	queues.MaxReceiveCount = int(maxReceives)
	return queues, nil
}

func stringAttribute(value string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(value)}
}
