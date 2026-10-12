//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/google/uuid"
	"github.com/moby/moby/client"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/testcontainers/testcontainers-go"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
	"github.com/IkkiDK/backend-challenge-go/internal/messaging"
)

const consumerName = "wallet-service.wager-transactions"

func testConsumerConfig(processingTimeout time.Duration) config.Consumer {
	return config.Consumer{
		Enabled: true, Name: consumerName, Concurrency: 4, WaitTime: time.Second, ProcessingTimeout: processingTimeout,
		RetryBaseDelay: time.Second, RetryMaxDelay: time.Second,
	}
}

// startConsumer runs an in-process consumer over client until the test ends.
func startConsumer(t *testing.T, p inProcess, sqsAPI *sqs.Client, cfg config.Consumer) *messaging.Consumer {
	t.Helper()
	broker := messaging.NewBroker(sqsAPI, config.SQS{InboundQueue: inboundQueue, DeadLetterQueue: deadLetterQueue, EventsQueue: eventsQueue})
	if err := broker.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	consumer := messaging.NewConsumer(broker, p.service, cfg, app.SystemClock, discardLogger(), p.metrics)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = consumer.Stop(ctx)
	})
	return consumer
}

func sqsMessages(p inProcess, outcome string) float64 {
	return testutil.ToFloat64(p.metrics.SQSMessages.WithLabelValues(outcome))
}

// queueDepth counts visible and in-flight messages, as the MiniStack root.
func queueDepth(t *testing.T, queue string) int {
	t.Helper()
	root := sqsClient(t, "root")
	out, err := root.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL(t, root, queue)),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	visible, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	inFlight, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return visible + inFlight
}

// pause freezes a container (its connections hang, nothing is lost) and
// returns the function that resumes it; the test cleanup resumes it too.
func pause(t *testing.T, c *testcontainers.DockerContainer) func() {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docker.ContainerPause(t.Context(), c.GetContainerID(), client.ContainerPauseOptions{}); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	resume := func() {
		once.Do(func() {
			if _, err := docker.ContainerUnpause(context.Background(), c.GetContainerID(), client.ContainerUnpauseOptions{}); err != nil {
				t.Errorf("resume container: %v", err)
			}
		})
	}
	t.Cleanup(resume)
	return resume
}

func TestConsumerSettlesMessagesAndDeduplicatesRedeliveries(t *testing.T) {
	purge(t, inboundQueue)
	p := newInProcess(t, testPolicy)
	startConsumer(t, p, sqsClient(t, "wallet-service"), testConsumerConfig(5*time.Second))
	w := openWalletInProcess(t, p, "100.00")
	op := newOperation(w, "BET", "sqs-bet-"+w.ID, "25.00")

	body := wagerMessage("msg-"+op.ExternalTransactionID, op)
	sendWager(t, w.ID, body)
	sendWager(t, w.ID, body) // a redelivery of the same message
	sendWager(t, w.ID, wagerMessage("msg-2-"+op.ExternalTransactionID, op))

	eventually(t, 20*time.Second, "the three deliveries to be handled", func() bool {
		return sqsMessages(p, "processed")+sqsMessages(p, "duplicate") == 3
	})
	if sqsMessages(p, "processed") != 1 || sqsMessages(p, "duplicate") != 2 {
		t.Fatalf("processed %v, duplicates %v; want 1 and 2", sqsMessages(p, "processed"), sqsMessages(p, "duplicate"))
	}
	if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1`, op.ExternalTransactionID); n != 1 {
		t.Fatalf("transactions = %d, want 1", n)
	}
	if n := countRows(t, `SELECT count(*) FROM inbox_messages WHERE message_id IN ($1, $2)`, "msg-"+op.ExternalTransactionID, "msg-2-"+op.ExternalTransactionID); n != 2 {
		t.Fatalf("inbox records = %d, want one per message id", n)
	}
	requireLedgerMatchesBalance(t, w.ID, 7500)
	eventually(t, 10*time.Second, "the queue to be empty", func() bool { return queueDepth(t, inboundQueue) == 0 })
}

// deleteFailingClient connects as the service but fails every DeleteMessage,
// as if the process died right after committing; it hands over the receipt
// handles it was asked to delete.
func deleteFailingClient(t *testing.T, receipts chan<- string) *sqs.Client {
	keys := env.credentials["wallet-service"]
	failDelete := func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("CrashBeforeDelete",
			func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				if input, ok := in.Parameters.(*sqs.DeleteMessageInput); ok {
					receipts <- aws.ToString(input.ReceiptHandle)
					return middleware.InitializeOutput{}, middleware.Metadata{}, errors.New("simulated crash before DeleteMessage")
				}
				return next.HandleInitialize(ctx, in)
			}), middleware.Before)
	}
	return sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(env.sqsEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(keys.accessKeyID, keys.secretAccessKey, ""),
		APIOptions:   []func(*middleware.Stack) error{failDelete},
	})
}

func TestMessageCommittedButNotDeletedIsNotReprocessedOnRedelivery(t *testing.T) {
	purge(t, inboundQueue)
	first := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, first, "100.00")
	op := newOperation(w, "BET", "crash-bet-"+w.ID, "40.00")

	receipts := make(chan string, 1)
	crashing := startConsumer(t, first, deleteFailingClient(t, receipts), testConsumerConfig(5*time.Second))
	sendWager(t, w.ID, wagerMessage("msg-"+op.ExternalTransactionID, op))

	var receipt string
	select {
	case receipt = <-receipts:
	case <-time.After(20 * time.Second):
		t.Fatal("the consumer never committed the message")
	}
	if status, _ := transactionStatus(t, "provider-a", op.ExternalTransactionID); status != "PROCESSED" {
		t.Fatalf("status after commit = %q, want PROCESSED", status)
	}
	// The "crashed" consumer is gone; its message is still in the queue.
	if err := crashing.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if queueDepth(t, inboundQueue) != 1 {
		t.Fatal("the undeleted message must still be in the queue")
	}
	// Instead of waiting the 30-second visibility timeout, make it visible now.
	root := sqsClient(t, "root")
	if _, err := root.ChangeMessageVisibility(t.Context(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(queueURL(t, root, inboundQueue)), ReceiptHandle: aws.String(receipt), VisibilityTimeout: 0,
	}); err != nil {
		t.Fatal(err)
	}

	// Another consumer instance, with its own connections, receives the redelivery.
	second := newInProcess(t, testPolicy)
	startConsumer(t, second, sqsClient(t, "wallet-service"), testConsumerConfig(5*time.Second))
	eventually(t, 20*time.Second, "the redelivery to be deduplicated", func() bool { return sqsMessages(second, "duplicate") == 1 })
	if sqsMessages(second, "processed") != 0 {
		t.Fatal("the redelivery was processed again")
	}
	requireLedgerMatchesBalance(t, w.ID, 6000)
	eventually(t, 10*time.Second, "the queue to be empty", func() bool { return queueDepth(t, inboundQueue) == 0 })
}

func TestPermanentFailuresAreDeadLetteredAtOnce(t *testing.T) {
	purge(t, inboundQueue, deadLetterQueue)
	p := newInProcess(t, testPolicy)
	startConsumer(t, p, sqsClient(t, "wallet-service"), testConsumerConfig(5*time.Second))
	w := openWalletInProcess(t, p, "100.00")
	valid := newOperation(w, "BET", "dlq-valid-"+w.ID, "10.00")
	sendWager(t, w.ID, wagerMessage("msg-valid-"+w.ID, valid))
	eventually(t, 20*time.Second, "the valid message to be processed", func() bool { return sqsMessages(p, "processed") == 1 })

	opening := newOperation(w, "OPENING", "dlq-opening-"+w.ID, "10.00")
	badAmount := newOperation(w, "BET", "dlq-amount-"+w.ID, "10")
	unknownWallet := newOperation(w, "BET", "dlq-wallet-"+w.ID, "10.00")
	unknownWallet.WalletID = uuid.NewString()
	reusedKey := valid
	reusedKey.Money = brl("11.00")
	sameIDOtherContent := newOperation(w, "BET", "dlq-other-"+w.ID, "5.00")

	sendWager(t, w.ID, "{not json")
	sendWager(t, w.ID, wagerMessage("msg-opening-"+w.ID, opening))
	sendWager(t, w.ID, wagerMessage("msg-amount-"+w.ID, badAmount))
	sendWager(t, unknownWallet.WalletID, wagerMessage("msg-wallet-"+w.ID, unknownWallet))
	sendWager(t, w.ID, wagerMessage("msg-reused-key-"+w.ID, reusedKey))
	sendWager(t, w.ID, wagerMessage("msg-valid-"+w.ID, sameIDOtherContent))

	want := map[string]int{"invalid_message": 3, "wallet_not_found": 1, "idempotency_key_reused": 1, "message_id_reused": 1}
	eventually(t, 30*time.Second, "six messages to be dead-lettered", func() bool {
		return testutil.ToFloat64(p.metrics.SQSMessages.WithLabelValues("dead_lettered")) == 6
	})
	reasons := map[string]int{}
	for _, m := range drain(t, sqsClient(t, "root"), deadLetterQueue, 3*time.Second) {
		reasons[aws.ToString(m.attributes["deadLetterReason"].StringValue)]++
	}
	for reason, n := range want {
		if reasons[reason] != n {
			t.Errorf("dead-lettered for %s: %d, want %d (all: %v)", reason, reasons[reason], n, reasons)
		}
	}
	for _, op := range []operation{opening, badAmount, unknownWallet, sameIDOtherContent} {
		if status, _ := transactionStatus(t, "provider-a", op.ExternalTransactionID); status != "" {
			t.Errorf("%s was recorded as %s", op.ExternalTransactionID, status)
		}
	}
	requireLedgerMatchesBalance(t, w.ID, 9000)
}

func TestDatabaseOutageIsRetriedUntilItRecovers(t *testing.T) {
	purge(t, inboundQueue)
	p := newInProcess(t, testPolicy)
	startConsumer(t, p, sqsClient(t, "wallet-service"), testConsumerConfig(2*time.Second))
	w := openWalletInProcess(t, p, "100.00")
	op := newOperation(w, "BET", "outage-bet-"+w.ID, "10.00")

	resume := pause(t, env.postgres)
	sendWager(t, w.ID, wagerMessage("msg-"+op.ExternalTransactionID, op))
	eventually(t, 20*time.Second, "the consumer to back off", func() bool { return sqsMessages(p, "retried") >= 1 })
	resume()

	eventually(t, 30*time.Second, "the message to be processed after recovery", func() bool {
		status, _ := transactionStatus(t, "provider-a", op.ExternalTransactionID)
		return status == "PROCESSED"
	})
	requireLedgerMatchesBalance(t, w.ID, 9000)
}

func TestMessagesThatKeepFailingReachTheDeadLetterQueueThroughTheRedrivePolicy(t *testing.T) {
	purge(t, inboundQueue, deadLetterQueue)
	p := newInProcess(t, testPolicy)
	startConsumer(t, p, sqsClient(t, "wallet-service"), testConsumerConfig(time.Second))
	w := openWalletInProcess(t, p, "100.00")
	op := newOperation(w, "BET", "exhausted-bet-"+w.ID, "10.00")
	body := wagerMessage("msg-"+op.ExternalTransactionID, op)

	resume := pause(t, env.postgres)
	sendWager(t, w.ID, body)
	eventually(t, 60*time.Second, "SQS to move the message to the dead-letter queue", func() bool {
		return queueDepth(t, deadLetterQueue) == 1
	})
	resume()

	moved := drain(t, sqsClient(t, "root"), deadLetterQueue, 3*time.Second)
	if len(moved) != 1 || moved[0].body != body {
		t.Fatalf("dead-lettered = %+v, want the original message", moved)
	}
	if retried := sqsMessages(p, "retried"); retried < 4 {
		t.Fatalf("retries before dead-lettering = %v, want the queue's maxReceiveCount worth", retried)
	}
	if status, _ := transactionStatus(t, "provider-a", op.ExternalTransactionID); status != "" {
		t.Fatalf("an exhausted message was recorded as %s", status)
	}
}

func TestBrokerOutageDelaysEventsWithoutLosingThem(t *testing.T) {
	inst := startInstance(t, "broker-outage", nil)
	w := openWallet(t, inst.baseURL, "100.00")
	eventually(t, 20*time.Second, "the opening events to be published", func() bool {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND published_at IS NULL`, w.ID) == 0
	})

	resume := pause(t, env.ministack)
	status, result := submit(t, inst.baseURL, accessToken(t, "provider-a"), newOperation(w, "BET", "broker-outage-"+w.ID, "10.00"))
	if status != http.StatusCreated || result.Status != "PROCESSED" {
		t.Fatalf("bet during the broker outage: %d %+v", status, result)
	}
	eventually(t, 10*time.Second, "readiness to report SQS down", func() bool {
		return call(t, http.MethodGet, inst.baseURL+"/health/ready", "", nil, nil).status == http.StatusServiceUnavailable
	})
	if n := countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND published_at IS NULL`, w.ID); n != 2 {
		t.Fatalf("unpublished events during the outage = %d, want the bet's 2", n)
	}
	resume()

	eventually(t, 30*time.Second, "the delayed events to be published", func() bool {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND published_at IS NULL`, w.ID) == 0
	})
	requireLedgerMatchesBalance(t, w.ID, 9000)
}
