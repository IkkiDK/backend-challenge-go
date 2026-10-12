// Command loadtest drives a running stack (docker compose up) with wagering
// traffic spread over every service instance, then reports throughput,
// latency percentiles, outcomes, idempotent replays, concurrency conflicts,
// the outbox lag of every event the run produced and a reconciliation of every
// wallet it used. See LOADTEST.md for the methodology and recorded results.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	provider          = "provider-a"
	backofficeClient  = "wallet-backoffice"
	localSecretSuffix = "-local-secret"
	currency          = "BRL"
	initialBalance    = "1000000.00"
	maxBetUnits       = 20
	winPercent        = 25
	lossPercent       = 10
	requestTimeout    = 15 * time.Second
	tokenRefreshAhead = 30 * time.Second
	drainPoll         = 500 * time.Millisecond
)

type config struct {
	targets        []string
	issuer         string
	databaseURL    string
	duration       time.Duration
	concurrency    int
	rate           float64
	wallets        int
	hotWallets     int
	duplicateRatio float64
	seed           uint64
	drainTimeout   time.Duration
	report         string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		cfg     config
		targets string
	)
	flag.StringVar(&targets, "targets", "http://localhost:8081,http://localhost:8082,http://localhost:8083", "comma-separated service base URLs; requests rotate over them")
	flag.StringVar(&cfg.issuer, "issuer", "http://localhost:8180/realms/wallet", "Keycloak realm URL")
	flag.StringVar(&cfg.databaseURL, "database-url", "postgres://wallet_app:wallet_app_local@localhost:55432/wallet?sslmode=disable", "PostgreSQL URL used to measure the outbox lag")
	flag.DurationVar(&cfg.duration, "duration", time.Minute, "measured load duration")
	flag.IntVar(&cfg.concurrency, "concurrency", 32, "concurrent clients")
	flag.Float64Var(&cfg.rate, "rate", 0, "operations per second (open loop); 0 sends each client's next request as soon as the previous one returns (closed loop)")
	flag.IntVar(&cfg.wallets, "wallets", 200, "wallets opened for the run")
	flag.IntVar(&cfg.hotWallets, "hot-wallets", 0, "if positive, send all traffic to this many wallets to measure lock contention")
	flag.Float64Var(&cfg.duplicateRatio, "duplicate-ratio", 0.1, "fraction of requests that resend an earlier operation with the same idempotency key")
	flag.Uint64Var(&cfg.seed, "seed", 1, "random seed for the operation mix")
	flag.DurationVar(&cfg.drainTimeout, "drain-timeout", 2*time.Minute, "how long to wait, after the load, for the outbox to publish the run's events")
	flag.StringVar(&cfg.report, "report", "", "also write the Markdown report to this file")
	flag.Parse()
	cfg.targets = strings.Split(targets, ",")
	switch {
	case cfg.concurrency < 1 || cfg.wallets < 1 || cfg.rate < 0 || cfg.drainTimeout < 0:
		return errors.New("concurrency and wallets must be positive, rate and drain-timeout not negative")
	case cfg.duplicateRatio < 0 || cfg.duplicateRatio >= 1:
		return errors.New("duplicate-ratio must be within [0, 1)")
	case cfg.hotWallets > cfg.wallets:
		return errors.New("hot-wallets cannot exceed wallets")
	}

	ctx := context.Background()
	db, err := pgx.Connect(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer db.Close(ctx)

	client := &http.Client{Timeout: requestTimeout, Transport: &http.Transport{MaxIdleConnsPerHost: cfg.concurrency}}
	tokenURL := cfg.issuer + "/protocol/openid-connect/token"
	backoffice := &tokenSource{client: client, tokenURL: tokenURL, clientID: backofficeClient}
	providerTokens := &tokenSource{client: client, tokenURL: tokenURL, clientID: provider}

	fmt.Printf("opening %d wallets...\n", cfg.wallets)
	wallets, err := openWallets(client, cfg.targets, backoffice, cfg.wallets)
	if err != nil {
		return err
	}
	before, err := scrape(client, cfg.targets)
	if err != nil {
		return err
	}

	fmt.Printf("running for %v against %d instances...\n", cfg.duration, len(cfg.targets))
	started := time.Now()
	load := drive(client, cfg, wallets, providerTokens)

	fmt.Println("waiting for the outbox to publish every event of the run...")
	drain, err := waitForDrain(ctx, db, started, cfg.drainTimeout)
	if err != nil {
		return err
	}
	lag, err := outboxLag(ctx, db, started)
	if err != nil {
		return err
	}
	after, err := scrape(client, cfg.targets)
	if err != nil {
		return err
	}
	inconsistent, err := reconcile(client, cfg.targets, backoffice, wallets)
	if err != nil {
		return err
	}

	report := render(cfg, load, after.minus(before), lag, drain, len(wallets), inconsistent)
	fmt.Print(report)
	if cfg.report != "" {
		return os.WriteFile(cfg.report, []byte(report), 0o644)
	}
	return nil
}

// tokenSource caches a client_credentials token and refreshes it before expiry.
type tokenSource struct {
	client   *http.Client
	tokenURL string
	clientID string

	mu      sync.Mutex
	token   string
	expires time.Time
}

func (s *tokenSource) get() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Until(s.expires) > tokenRefreshAhead {
		return s.token, nil
	}
	resp, err := s.client.PostForm(s.tokenURL, url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {s.clientID},
		"client_secret": {s.clientID + localSecretSuffix},
	})
	if err != nil {
		return "", fmt.Errorf("token for %s: %w", s.clientID, err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		return "", fmt.Errorf("token for %s: status %d", s.clientID, resp.StatusCode)
	}
	s.token, s.expires = body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn)*time.Second)
	return s.token, nil
}

type wallet struct {
	id     string
	player string
}

func openWallets(client *http.Client, targets []string, tokens *tokenSource, n int) ([]wallet, error) {
	wallets := make([]wallet, 0, n)
	for i := range n {
		token, err := tokens.get()
		if err != nil {
			return nil, err
		}
		player := uuid.NewString()
		status, body, err := post(client, targets[i%len(targets)]+"/wallets", token, "", map[string]any{
			"playerId": player, "initialBalance": map[string]string{"amount": initialBalance, "currency": currency},
		})
		if err != nil || status != http.StatusCreated {
			return nil, fmt.Errorf("open wallet: status %d %s %v", status, body, err)
		}
		var opened struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(body, &opened); err != nil {
			return nil, err
		}
		wallets = append(wallets, wallet{id: opened.ID, player: player})
	}
	return wallets, nil
}

type operation struct {
	key  string
	body map[string]any
}

type outcomes struct {
	latencies []time.Duration
	statuses  map[int]int
	replays   int
	errors    int
}

func (o *outcomes) merge(other outcomes) {
	o.latencies = append(o.latencies, other.latencies...)
	o.replays += other.replays
	o.errors += other.errors
	for status, n := range other.statuses {
		o.statuses[status] += n
	}
}

type loadResult struct {
	elapsed time.Duration
	outcomes
}

// drive sends wagering operations until the duration ends. Most are new BET,
// WIN or LOSS operations; duplicateRatio of them resend an earlier operation
// unchanged. In the open loop, latency counts from the moment a request was
// due, so a saturated service shows up as latency instead of a lower send rate.
func drive(client *http.Client, cfg config, wallets []wallet, tokens *tokenSource) loadResult {
	pool := wallets
	if cfg.hotWallets > 0 {
		pool = wallets[:cfg.hotWallets]
	}
	started := time.Now()
	deadline := started.Add(cfg.duration)
	due := schedule(cfg, started, deadline)
	perClient := make([]outcomes, cfg.concurrency)
	var wg sync.WaitGroup
	for id := range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.seed, uint64(id)))
			out := outcomes{statuses: map[int]int{}}
			var sent []operation
			for i := 0; ; i++ {
				dueAt, ok := next(due, deadline)
				if !ok {
					break
				}
				var op operation
				if len(sent) > 0 && rng.Float64() < cfg.duplicateRatio {
					op = sent[rng.IntN(len(sent))]
				} else {
					op = newOperation(rng, pool[rng.IntN(len(pool))])
					sent = append(sent, op)
				}
				token, err := tokens.get()
				if err != nil {
					out.errors++
					continue
				}
				target := cfg.targets[(id+i)%len(cfg.targets)]
				status, body, err := post(client, target+"/wagering/transactions", token, op.key, op.body)
				out.latencies = append(out.latencies, time.Since(dueAt))
				if err != nil {
					out.errors++
					continue
				}
				out.statuses[status]++
				if bytes.Contains(body, []byte(`"idempotentReplay":true`)) {
					out.replays++
				}
			}
			perClient[id] = out
		}()
	}
	wg.Wait()
	result := loadResult{elapsed: time.Since(started), outcomes: outcomes{statuses: map[int]int{}}}
	for _, out := range perClient {
		result.merge(out)
	}
	return result
}

// schedule emits the due time of every request in the open loop and returns
// nil in the closed loop.
func schedule(cfg config, started, deadline time.Time) <-chan time.Time {
	if cfg.rate == 0 {
		return nil
	}
	interval := time.Duration(float64(time.Second) / cfg.rate)
	due := make(chan time.Time)
	go func() {
		defer close(due)
		for at := started; at.Before(deadline); at = at.Add(interval) {
			time.Sleep(time.Until(at))
			due <- at
		}
	}()
	return due
}

func next(due <-chan time.Time, deadline time.Time) (time.Time, bool) {
	if due == nil {
		now := time.Now()
		return now, now.Before(deadline)
	}
	at, ok := <-due
	return at, ok
}

func newOperation(rng *rand.Rand, w wallet) operation {
	externalID := uuid.NewString()
	kind, amount := "BET", fmt.Sprintf("%d.%02d", 1+rng.IntN(maxBetUnits), rng.IntN(100))
	switch roll := rng.IntN(100); {
	case roll < winPercent:
		kind = "WIN"
	case roll < winPercent+lossPercent:
		kind, amount = "LOSS", "0.00"
	}
	return operation{
		key: provider + ":" + externalID,
		body: map[string]any{
			"providerId": provider, "externalTransactionId": externalID, "playerId": w.player, "walletId": w.id,
			"roundId": "round-" + externalID, "gameId": "load-test", "kind": kind,
			"money": map[string]string{"amount": amount, "currency": currency},
		},
	}
}

func post(client *http.Client, endpoint, token, idempotencyKey string, body any) (int, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	return resp.StatusCode, payload, err
}

// metrics holds the wallet_* samples of every instance, summed per series.
type metrics map[string]float64

func scrape(client *http.Client, targets []string) (metrics, error) {
	total := metrics{}
	for _, target := range targets {
		if err := scrapeInto(client, target+"/metrics", total); err != nil {
			return nil, err
		}
	}
	return total, nil
}

func scrapeInto(client *http.Client, endpoint string, total metrics) error {
	resp, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "wallet_") {
			continue
		}
		space := strings.LastIndexByte(line, ' ')
		value, err := strconv.ParseFloat(line[space+1:], 64)
		if err != nil {
			return fmt.Errorf("parse metric %q: %w", line, err)
		}
		total[line[:space]] += value
	}
	return scanner.Err()
}

func (m metrics) minus(earlier metrics) metrics {
	delta := metrics{}
	for series, value := range m {
		delta[series] = value - earlier[series]
	}
	return delta
}

// sum adds the series of a metric whose labels contain every given label.
func (m metrics) sum(name string, labels ...string) float64 {
	var total float64
	for series, value := range m {
		if series != name && !strings.HasPrefix(series, name+"{") {
			continue
		}
		if !slices.ContainsFunc(labels, func(label string) bool { return !strings.Contains(series, label) }) {
			total += value
		}
	}
	return total
}

// drainResult tells how the outbox caught up with the run's events once the
// load stopped.
type drainResult struct {
	elapsed time.Duration
	// pending events were still unpublished when the drain ended; published
	// counts those published while draining.
	pending, published int64
}

// waitForDrain waits until every event that occurred since started is
// published, or until timeout.
func waitForDrain(ctx context.Context, db *pgx.Conn, started time.Time, timeout time.Duration) (drainResult, error) {
	stopped := time.Now()
	_, publishedAtStop, err := outboxProgress(ctx, db, started)
	if err != nil {
		return drainResult{}, err
	}
	for {
		pending, published, err := outboxProgress(ctx, db, started)
		if err != nil {
			return drainResult{}, err
		}
		if pending == 0 || time.Since(stopped) >= timeout {
			return drainResult{elapsed: time.Since(stopped), pending: pending, published: published - publishedAtStop}, nil
		}
		time.Sleep(drainPoll)
	}
}

// outboxProgress counts the run's events that wait for publication and those
// already published.
func outboxProgress(ctx context.Context, db *pgx.Conn, started time.Time) (pending, published int64, err error) {
	err = db.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE published_at IS NULL), count(*) FILTER (WHERE published_at IS NOT NULL)
		FROM outbox_events WHERE occurred_at >= $1`, started).Scan(&pending, &published)
	if err != nil {
		return 0, 0, fmt.Errorf("count outbox events: %w", err)
	}
	return pending, published, nil
}

type lagStats struct {
	events, published  int64
	p50, p95, p99, max time.Duration
}

// outboxLag measures, for every published event of the run, the time from
// its occurrence (inside the committing transaction) to its publication.
func outboxLag(ctx context.Context, db *pgx.Conn, started time.Time) (lagStats, error) {
	var (
		stats              lagStats
		p50, p95, p99, top *float64
	)
	err := db.QueryRow(ctx, `
		WITH lag AS (
			SELECT extract(epoch FROM published_at - occurred_at)::float8 AS seconds
			FROM outbox_events WHERE occurred_at >= $1)
		SELECT count(*), count(seconds),
			percentile_cont(0.50) WITHIN GROUP (ORDER BY seconds),
			percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds),
			percentile_cont(0.99) WITHIN GROUP (ORDER BY seconds),
			max(seconds)
		FROM lag`, started).Scan(&stats.events, &stats.published, &p50, &p95, &p99, &top)
	if err != nil {
		return lagStats{}, fmt.Errorf("measure outbox lag: %w", err)
	}
	if stats.published == 0 {
		return stats, nil
	}
	seconds := func(s *float64) time.Duration { return time.Duration(*s * float64(time.Second)) }
	stats.p50, stats.p95, stats.p99, stats.max = seconds(p50), seconds(p95), seconds(p99), seconds(top)
	return stats, nil
}

// reconcile checks every wallet's stored balance against its ledger.
func reconcile(client *http.Client, targets []string, tokens *tokenSource, wallets []wallet) (int, error) {
	inconsistent := 0
	for i, w := range wallets {
		token, err := tokens.get()
		if err != nil {
			return 0, err
		}
		status, body, err := post(client, targets[i%len(targets)]+"/wallets/"+w.id+"/reconciliation", token, "", map[string]any{})
		if err != nil || status != http.StatusOK {
			return 0, fmt.Errorf("reconcile %s: status %d %s %v", w.id, status, body, err)
		}
		if !bytes.Contains(body, []byte(`"consistent":true`)) {
			inconsistent++
		}
	}
	return inconsistent, nil
}

// percentile uses the nearest-rank method on sorted samples.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(rank, 0)]
}

func render(cfg config, load loadResult, delta metrics, lag lagStats, drain drainResult, wallets, inconsistent int) string {
	latencies := slices.Clone(load.latencies)
	slices.Sort(latencies)
	ms := func(d time.Duration) string {
		return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64) + " ms"
	}
	loop := fmt.Sprintf("closed loop, %d clients", cfg.concurrency)
	if cfg.rate > 0 {
		loop = fmt.Sprintf("open loop at %.0f ops/s, %d clients", cfg.rate, cfg.concurrency)
	}
	scope := fmt.Sprintf("%d wallets", wallets)
	if cfg.hotWallets > 0 {
		scope = fmt.Sprintf("%d hot wallets", cfg.hotWallets)
	}
	lockWaits := delta.sum("wallet_lock_wait_seconds_count")
	avgLockWait := 0.0
	if lockWaits > 0 {
		avgLockWait = delta.sum("wallet_lock_wait_seconds_sum") / lockWaits * 1000
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n### %s, %s, %v, %d instances\n\n", loop, scope, cfg.duration, len(cfg.targets))
	fmt.Fprintf(&b, "Client: %s/%s, %d CPUs, %s; duplicate ratio %.2f, seed %d.\n\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), cfg.duplicateRatio, cfg.seed)
	fmt.Fprintf(&b, "| Measure | Value |\n| --- | --- |\n")
	fmt.Fprintf(&b, "| Requests | %d in %.1f s (%.0f req/s) |\n", len(latencies), load.elapsed.Seconds(), float64(len(latencies))/load.elapsed.Seconds())
	fmt.Fprintf(&b, "| Latency p50 / p95 / p99 / max | %s / %s / %s / %s |\n",
		ms(percentile(latencies, 0.50)), ms(percentile(latencies, 0.95)), ms(percentile(latencies, 0.99)), ms(percentile(latencies, 1)))
	fmt.Fprintf(&b, "| Responses by status | %s |\n", statusSummary(load.statuses))
	fmt.Fprintf(&b, "| Transport errors | %d |\n", load.errors)
	fmt.Fprintf(&b, "| Idempotent replays (client / service) | %d / %.0f |\n", load.replays, delta.sum("wallet_idempotent_replays_total"))
	fmt.Fprintf(&b, "| Idempotency conflicts (409) | %.0f |\n", delta.sum("wallet_idempotency_conflicts_total"))
	fmt.Fprintf(&b, "| Concurrency conflicts retried (deadlock, serialization, stale version) | %.0f |\n", delta.sum("wallet_concurrency_conflicts_total"))
	fmt.Fprintf(&b, "| Wallet lock wait, average | %.2f ms |\n", avgLockWait)
	fmt.Fprintf(&b, "| Events recorded / published | %d / %d |\n", lag.events, lag.published)
	fmt.Fprintf(&b, "| Outbox lag of published events, p50 / p95 / p99 / max | %s / %s / %s / %s |\n", ms(lag.p50), ms(lag.p95), ms(lag.p99), ms(lag.max))
	if drain.pending == 0 {
		fmt.Fprintf(&b, "| Outbox drained after the load stopped | %.1f s |\n", drain.elapsed.Seconds())
	} else {
		fmt.Fprintf(&b, "| Outbox not drained | %d events still pending after %.0f s of draining, at %.0f events/s |\n",
			drain.pending, drain.elapsed.Seconds(), float64(drain.published)/drain.elapsed.Seconds())
	}
	fmt.Fprintf(&b, "| Wallets reconciled / inconsistent | %d / %d |\n", wallets, inconsistent)
	return b.String()
}

func statusSummary(statuses map[int]int) string {
	codes := slices.Sorted(maps.Keys(statuses))
	parts := make([]string, len(codes))
	for i, code := range codes {
		parts[i] = fmt.Sprintf("%d: %d", code, statuses[code])
	}
	return strings.Join(parts, ", ")
}
