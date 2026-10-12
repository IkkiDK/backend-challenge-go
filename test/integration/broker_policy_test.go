//go:build integration

package integration

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

// requireBrokerError checks that the broker refused a call with an error code
// containing want.
func requireBrokerError(t *testing.T, err error, want string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || !strings.Contains(apiErr.ErrorCode(), want) {
		t.Fatalf("error = %v, want the broker to refuse it with %s", err, want)
	}
}

// Every identity may only perform the SQS actions its IAM policy grants; the
// broker refuses everything else, on every queue.
func TestBrokerPoliciesRefuseWhatAnIdentityWasNotGranted(t *testing.T) {
	root := sqsClient(t, "root")
	urls := map[string]*string{}
	for _, name := range []string{inboundQueue, deadLetterQueue, eventsQueue} {
		urls[name] = aws.String(queueURL(t, root, name))
	}
	send := func(client *sqs.Client, queue string) error {
		_, err := client.SendMessage(t.Context(), &sqs.SendMessageInput{
			QueueUrl: urls[queue], MessageBody: aws.String("{}"),
			MessageGroupId: aws.String("policy-test"), MessageDeduplicationId: aws.String(uuid.NewString()),
		})
		return err
	}
	receive := func(client *sqs.Client, queue string) error {
		_, err := client.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: urls[queue]})
		return err
	}
	var (
		producer = sqsClient(t, "wager-producer")
		reader   = sqsClient(t, "events-reader")
		service  = sqsClient(t, "wallet-service")
	)

	refused := map[string]func() error{
		"the producer reading the inbound queue":      func() error { return receive(producer, inboundQueue) },
		"the producer publishing events":              func() error { return send(producer, eventsQueue) },
		"the events reader submitting operations":     func() error { return send(reader, inboundQueue) },
		"the events reader reading dead letters":      func() error { return receive(reader, deadLetterQueue) },
		"the service submitting operations to itself": func() error { return send(service, inboundQueue) },
		"the service purging the inbound queue": func() error {
			_, err := service.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: urls[inboundQueue]})
			return err
		},
	}
	for name, attempt := range refused {
		t.Run(name, func(t *testing.T) { requireBrokerError(t, attempt(), "AccessDenied") })
	}

	t.Run("an access key the broker does not know", func(t *testing.T) {
		stranger := sqs.New(sqs.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String(env.sqsEndpoint),
			Credentials:  credentials.NewStaticCredentialsProvider("AKIAUNKNOWNIDENTITY0", "any-secret", ""),
		})
		requireBrokerError(t, send(stranger, inboundQueue), "UnrecognizedClient")
	})
}
