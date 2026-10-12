//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
	"github.com/IkkiDK/backend-challenge-go/internal/postgres"
)

// Test identities provisioned by deploy/keycloak/wallet-realm.json.
var clientSecrets = map[string]string{
	"provider-a":             "provider-a-local-secret",
	"provider-b":             "provider-b-local-secret",
	"provider-a-short-lived": "provider-a-short-lived-local-secret",
	"wallet-backoffice":      "wallet-backoffice-local-secret",
	"foreign-service":        "foreign-service-local-secret",
}

// Queue names provisioned by deploy/ministack/ready.d/10-provision.sh.
const (
	inboundQueue    = "wager-transactions.fifo"
	deadLetterQueue = "wager-transactions-dlq.fifo"
	eventsQueue     = "wallet-events.fifo"
)

var testPolicy = domain.ReferenceRetryPolicy{BaseDelay: 200 * time.Millisecond, MaxDelay: time.Second, MaxAttempts: 4}

// eventually polls condition until it holds or timeout expires.
func eventually(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// --- identity provider ---

// accessToken returns a client_credentials token for client.
func accessToken(t *testing.T, client string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {clientSecrets[client]}}
	resp, err := http.PostForm(env.tokenURL, form)
	if err != nil {
		t.Fatalf("token request: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d, %v", client, resp.StatusCode, err)
	}
	return body.AccessToken
}

// --- HTTP API ---

type apiResponse struct {
	status int
	body   []byte
}

func (r apiResponse) decode(t *testing.T, target any) {
	t.Helper()
	if err := json.Unmarshal(r.body, target); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

func call(t *testing.T, method, endpoint, token string, headers map[string]string, body any) apiResponse {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, endpoint, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return apiResponse{status: resp.StatusCode, body: raw}
}

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func brl(amount string) moneyJSON { return moneyJSON{Amount: amount, Currency: "BRL"} }

type walletJSON struct {
	ID       string    `json:"id"`
	PlayerID string    `json:"playerId"`
	Balance  moneyJSON `json:"balance"`
	Version  int64     `json:"version"`
}

// openWallet opens a wallet for a new player through the API.
func openWallet(t *testing.T, baseURL, initialBalance string) walletJSON {
	t.Helper()
	resp := call(t, http.MethodPost, baseURL+"/wallets", accessToken(t, "wallet-backoffice"), nil, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": brl(initialBalance),
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", resp.status, resp.body)
	}
	var wallet walletJSON
	resp.decode(t, &wallet)
	return wallet
}

// operation is the body of POST /wagering/transactions.
type operation struct {
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	PlayerID              string    `json:"playerId"`
	WalletID              string    `json:"walletId"`
	RoundID               string    `json:"roundId"`
	GameID                string    `json:"gameId"`
	Kind                  string    `json:"kind"`
	Money                 moneyJSON `json:"money"`
	Reference             string    `json:"referenceExternalTransactionId,omitempty"`
}

func newOperation(w walletJSON, kind, externalID, amount string) operation {
	return operation{
		ProviderID: "provider-a", ExternalTransactionID: externalID, PlayerID: w.PlayerID, WalletID: w.ID,
		RoundID: "round-1", GameID: "fortune-chimp", Kind: kind, Money: brl(amount),
	}
}

func (o operation) key() string { return o.ProviderID + ":" + o.ExternalTransactionID }

type submitJSON struct {
	TransactionID    string     `json:"transactionId"`
	Status           string     `json:"status"`
	Balance          *moneyJSON `json:"balance"`
	FailureCode      string     `json:"failureCode"`
	FailureClass     string     `json:"failureClass"`
	IdempotentReplay bool       `json:"idempotentReplay"`
}

// submit posts op with its default idempotency key and the provider's token.
func submit(t *testing.T, baseURL, token string, op operation) (int, submitJSON) {
	t.Helper()
	resp := call(t, http.MethodPost, baseURL+"/wagering/transactions", token, map[string]string{"Idempotency-Key": op.key()}, op)
	var result submitJSON
	if resp.status < http.StatusBadRequest || resp.status == http.StatusUnprocessableEntity || resp.status == http.StatusInternalServerError {
		resp.decode(t, &result)
	}
	return resp.status, result
}

// --- database assertions (as the schema owner) ---

type walletRow struct {
	balanceMinor int64
	version      int64
}

func walletState(t *testing.T, walletID string) walletRow {
	t.Helper()
	var row walletRow
	if err := env.owner.QueryRow(t.Context(), `SELECT balance_minor, version FROM wallets WHERE id = $1`, walletID).
		Scan(&row.balanceMinor, &row.version); err != nil {
		t.Fatalf("wallet %s: %v", walletID, err)
	}
	return row
}

func countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := env.owner.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func ledgerEntries(t *testing.T, walletID string) int {
	t.Helper()
	return countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
}

func transactionStatus(t *testing.T, provider, externalID string) (status, failureCode string) {
	t.Helper()
	var code *string
	err := env.owner.QueryRow(t.Context(), `
		SELECT status, failure_code FROM wagering_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
		provider, externalID).Scan(&status, &code)
	if err != nil {
		return "", ""
	}
	if code != nil {
		failureCode = *code
	}
	return status, failureCode
}

// requireLedgerMatchesBalance checks the stored balance against the sum of
// the wallet's credits minus its debits, and the version against its entries.
func requireLedgerMatchesBalance(t *testing.T, walletID string, wantBalanceMinor int64) {
	t.Helper()
	var stored, calculated int64
	err := env.owner.QueryRow(t.Context(), `
		SELECT w.balance_minor,
			COALESCE(SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount_minor ELSE -l.amount_minor END), 0)::bigint
		FROM wallets w LEFT JOIN wallet_ledger_entries l ON l.wallet_id = w.id
		WHERE w.id = $1 GROUP BY w.balance_minor`, walletID).Scan(&stored, &calculated)
	if err != nil {
		t.Fatal(err)
	}
	if stored != calculated || stored != wantBalanceMinor {
		t.Fatalf("wallet %s: stored %d, ledger %d, want %d", walletID, stored, calculated, wantBalanceMinor)
	}
}

// --- in-process service ---

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), postgres.Config{
		URL: dsn, MaxConns: 16, LockTimeout: 5 * time.Second,
		StatementTimeout: 10 * time.Second, IdleInTransactionTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type inProcess struct {
	service *app.Service
	store   *postgres.Store
	metrics *observability.Metrics
}

// newInProcess builds the application against the shared database with the
// restricted application role.
func newInProcess(t *testing.T, policy domain.ReferenceRetryPolicy) inProcess {
	t.Helper()
	metrics := observability.NewMetrics()
	store := postgres.NewStore(newPool(t, env.appDSN), metrics)
	service, err := app.NewService(store, policy, app.SystemClock, discardLogger(), metrics)
	if err != nil {
		t.Fatal(err)
	}
	return inProcess{service: service, store: store, metrics: metrics}
}

// httpOrigin is the origin of an in-process request standing in for HTTP.
func httpOrigin() app.Origin {
	return app.Origin{Channel: app.ChannelHTTP, CorrelationID: "test-" + uuid.NewString()}
}

func mustRequest(t *testing.T, op operation) domain.TransactionRequest {
	t.Helper()
	req, err := domain.NewTransactionRequest(domain.TransactionRequestInput{
		ProviderID: op.ProviderID, ExternalTransactionID: op.ExternalTransactionID, IdempotencyKey: op.key(),
		PlayerID: op.PlayerID, WalletID: op.WalletID, RoundID: op.RoundID, GameID: op.GameID, Kind: op.Kind,
		Amount: op.Money.Amount, Currency: op.Money.Currency, ReferenceExternalTransactionID: op.Reference,
	})
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// openWalletInProcess opens a wallet with the in-process service.
func openWalletInProcess(t *testing.T, p inProcess, initialBalance string) walletJSON {
	t.Helper()
	req, err := domain.NewWalletOpeningRequest(domain.WalletOpeningInput{
		PlayerID: uuid.NewString(), Amount: initialBalance, Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	w, err := p.service.OpenWallet(t.Context(), req, "test-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return walletJSON{ID: w.ID().String(), PlayerID: w.PlayerID().String(), Version: w.Version()}
}

// --- SQS ---

// sqsClient connects as one of the provisioned IAM identities, or as the
// MiniStack root for administrative calls when profile is "root".
func sqsClient(t *testing.T, profile string) *sqs.Client {
	t.Helper()
	keys := awsKeys{accessKeyID: rootAccessKey, secretAccessKey: rootSecretKey}
	if profile != "root" {
		keys = env.credentials[profile]
	}
	return sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(env.sqsEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(keys.accessKeyID, keys.secretAccessKey, ""),
	})
}

func queueURL(t *testing.T, client *sqs.Client, name string) string {
	t.Helper()
	out, err := client.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("queue %s: %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// purge empties queues so a test only sees its own messages.
func purge(t *testing.T, names ...string) {
	t.Helper()
	root := sqsClient(t, "root")
	for _, name := range names {
		if _, err := root.PurgeQueue(t.Context(), &sqs.PurgeQueueInput{QueueUrl: aws.String(queueURL(t, root, name))}); err != nil {
			t.Fatalf("purge %s: %v", name, err)
		}
	}
}

// wagerMessage builds a WagerTransactionRequested envelope for op.
func wagerMessage(messageID string, op operation) string {
	data := map[string]any{
		"providerId": op.ProviderID, "externalTransactionId": op.ExternalTransactionID, "idempotencyKey": op.key(),
		"playerId": op.PlayerID, "walletId": op.WalletID, "roundId": op.RoundID, "gameId": op.GameID,
		"kind": op.Kind, "money": op.Money,
	}
	if op.Reference != "" {
		data["referenceExternalTransactionId"] = op.Reference
	}
	raw, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	})
	return string(raw)
}

// sendWager publishes body as the provider gateway. Each send gets its own
// deduplication id, so SQS delivers repeated sends and the service must
// deduplicate them itself.
func sendWager(t *testing.T, groupID, body string) {
	t.Helper()
	producer := sqsClient(t, "wager-producer")
	_, err := producer.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL(t, producer, inboundQueue)),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	if err != nil {
		t.Fatalf("send wager message: %v", err)
	}
}

type receivedMessage struct {
	body       string
	attributes map[string]types.MessageAttributeValue
}

// drain receives and deletes every visible message of queue as client.
func drain(t *testing.T, client *sqs.Client, queue string, wait time.Duration) []receivedMessage {
	t.Helper()
	url := queueURL(t, client, queue)
	var messages []receivedMessage
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatalf("receive from %s: %v", queue, err)
		}
		for _, m := range out.Messages {
			messages = append(messages, receivedMessage{body: aws.ToString(m.Body), attributes: m.MessageAttributes})
			if _, err := client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatalf("delete from %s: %v", queue, err)
			}
		}
	}
	return messages
}

// --- service processes ---

var (
	buildOnce   sync.Once
	walletPath  string
	buildOutput []byte
	buildErr    error
)

// walletBinary builds cmd/wallet once per run, with the race detector when
// the tests themselves run with -race.
func walletBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wallet-integration-*")
		if err != nil {
			buildErr = err
			return
		}
		walletPath = filepath.Join(dir, "wallet")
		if runtime.GOOS == "windows" {
			walletPath += ".exe"
		}
		args := []string{"build", "-o", walletPath}
		if raceEnabled {
			args = append(args, "-race")
		}
		cmd := exec.Command("go", append(args, "./cmd/wallet")...)
		cmd.Dir = repoPath()
		buildOutput, buildErr = cmd.CombinedOutput()
	})
	if buildErr != nil {
		t.Fatalf("build wallet: %v\n%s", buildErr, buildOutput)
	}
	return walletPath
}

// instance is a service process with its own connections and memory.
type instance struct {
	name    string
	baseURL string
	cmd     *exec.Cmd
	logs    *syncBuffer
	exited  chan struct{}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// serviceEnvironment is the configuration of a test service, with timings
// shortened so retries and expirations happen within seconds.
func serviceEnvironment(name string, port int) map[string]string {
	keys := env.credentials["wallet-service"]
	return map[string]string{
		"INSTANCE_ID":                 name,
		"LOG_LEVEL":                   "debug",
		"HTTP_ADDR":                   fmt.Sprintf("127.0.0.1:%d", port),
		"DATABASE_URL":                env.appDSN,
		"OIDC_ISSUER":                 env.issuer,
		"OIDC_JWKS_URL":               env.jwksURL,
		"AWS_REGION":                  "us-east-1",
		"AWS_ENDPOINT_URL":            env.sqsEndpoint,
		"AWS_ACCESS_KEY_ID":           keys.accessKeyID,
		"AWS_SECRET_ACCESS_KEY":       keys.secretAccessKey,
		"AWS_PROFILE":                 "",
		"AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
		"AWS_CONFIG_FILE":             os.DevNull,
		"SHUTDOWN_TIMEOUT":            "10s",
		"REFERENCE_POLL_INTERVAL":     "100ms",
		"REFERENCE_RETRY_BASE_DELAY":  testPolicy.BaseDelay.String(),
		"REFERENCE_RETRY_MAX_DELAY":   testPolicy.MaxDelay.String(),
		"REFERENCE_MAX_ATTEMPTS":      fmt.Sprint(testPolicy.MaxAttempts),
		"OUTBOX_POLL_INTERVAL":        "100ms",
		"OUTBOX_LEASE":                "5s",
		"OUTBOX_PUBLISH_TIMEOUT":      "2s",
		"OUTBOX_RETRY_BASE_DELAY":     "200ms",
		"OUTBOX_RETRY_MAX_DELAY":      "2s",
		"CONSUMER_RETRY_BASE_DELAY":   "1s",
		"CONSUMER_RETRY_MAX_DELAY":    "2s",
		"CONSUMER_WAIT_TIME":          "1s",
	}
}

// abandonedPollWindow outlasts CONSUMER_WAIT_TIME. A killed process cannot
// close its long poll: the broker keeps it open server-side until it times
// out, and a message arriving meanwhile would be received by nobody and stay
// invisible for the whole visibility timeout.
const abandonedPollWindow = 2 * time.Second

var pollWindowWaits sync.Map

// waitOutAbandonedPolls makes the test end, after all its processes are
// killed, by waiting once for their abandoned polls to expire, so the next
// test's messages reach live consumers.
func waitOutAbandonedPolls(t *testing.T) {
	if _, registered := pollWindowWaits.LoadOrStore(t, true); !registered {
		t.Cleanup(func() { time.Sleep(abandonedPollWindow) })
	}
}

// startInstance runs a service process and waits until it is ready. The
// process is killed when the test ends; its logs are printed if it failed.
func startInstance(t *testing.T, name string, overrides map[string]string) *instance {
	t.Helper()
	// Registered first, so it runs after the cleanups that kill the processes.
	waitOutAbandonedPolls(t)
	port := freePort(t)
	variables := serviceEnvironment(name, port)
	for key, value := range overrides {
		variables[key] = value
	}
	cmd := exec.Command(walletBinary(t))
	cmd.Env = os.Environ()
	for key, value := range variables {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	inst := &instance{name: name, baseURL: fmt.Sprintf("http://127.0.0.1:%d", port), cmd: cmd, logs: &syncBuffer{}, exited: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = inst.logs, inst.logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	go func() {
		_ = cmd.Wait()
		close(inst.exited)
	}()
	t.Cleanup(func() {
		inst.kill()
		// A process built with -race reports races on its own stderr.
		if strings.Contains(inst.logs.String(), "WARNING: DATA RACE") {
			t.Errorf("%s reported a data race", name)
		}
		if t.Failed() {
			t.Logf("logs of %s:\n%s", name, inst.logs)
		}
		// INTEGRATION_LOG_DIR keeps every instance's logs for later inspection.
		if dir := os.Getenv("INTEGRATION_LOG_DIR"); dir != "" {
			file := filepath.Join(dir, t.Name()+"-"+name+".log")
			if err := os.MkdirAll(filepath.Dir(file), 0o755); err == nil {
				_ = os.WriteFile(file, []byte(inst.logs.String()), 0o644)
			}
		}
	})

	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-inst.exited:
			t.Fatalf("%s exited during startup:\n%s", name, inst.logs)
		default:
		}
		if resp, err := http.Get(inst.baseURL + "/health/ready"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return inst
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready in time:\n%s", name, inst.logs)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// kill ends the process abruptly (SIGKILL / TerminateProcess), like a crash:
// no shutdown hook runs and its connections simply drop.
func (i *instance) kill() {
	select {
	case <-i.exited:
		return
	default:
	}
	_ = i.cmd.Process.Kill()
	<-i.exited
}

func (i *instance) logContains(text string) bool {
	return strings.Contains(i.logs.String(), text)
}
