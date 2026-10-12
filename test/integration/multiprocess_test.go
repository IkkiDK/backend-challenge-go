//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// startInstances runs n independent service processes.
func startInstances(t *testing.T, n int) []*instance {
	t.Helper()
	instances := make([]*instance, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instances[i] = startInstance(t, fmt.Sprintf("wallet-%d", i+1), nil)
		}()
	}
	wg.Wait()
	for _, inst := range instances {
		if inst == nil {
			t.FailNow()
		}
	}
	return instances
}

// metric sums the samples of name whose labels contain every given pair, as
// exposed on an instance's /metrics endpoint.
func metric(t *testing.T, inst *instance, name string, labels ...string) float64 {
	t.Helper()
	resp := call(t, http.MethodGet, inst.baseURL+"/metrics", "", nil, nil)
	var total float64
	scanner := bufio.NewScanner(bytes.NewReader(resp.body))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		matches := true
		for _, label := range labels {
			matches = matches && strings.Contains(line, label)
		}
		if !matches {
			continue
		}
		value, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		if err != nil {
			t.Fatalf("metric line %q: %v", line, err)
		}
		total += value
	}
	return total
}

// atOnce runs every function in its own goroutine, releasing them together.
func atOnce(fns ...func()) {
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	for _, fn := range fns {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			fn()
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
}

func TestTwoBetsRacingForTheSameBalanceOnDifferentInstances(t *testing.T) {
	instances := startInstances(t, 3)
	token := accessToken(t, "provider-a")
	w := openWallet(t, instances[0].baseURL, "100.00")
	betA := newOperation(w, "BET", "race-a-"+w.ID, "80.00")
	betB := newOperation(w, "BET", "race-b-"+w.ID, "80.00")

	var (
		mu       sync.Mutex
		statuses = map[string]int{}
		results  = map[string]submitJSON{}
	)
	record := func(name string, status int, result submitJSON) {
		mu.Lock()
		defer mu.Unlock()
		statuses[name], results[name] = status, result
	}
	atOnce(
		func() { status, result := submit(t, instances[0].baseURL, token, betA); record("a", status, result) },
		func() { status, result := submit(t, instances[1].baseURL, token, betB); record("b", status, result) },
	)

	processed, rejected := "a", "b"
	if statuses["a"] != http.StatusCreated {
		processed, rejected = "b", "a"
	}
	if statuses[processed] != http.StatusCreated || results[processed].Balance.Amount != "20.00" {
		t.Fatalf("winner: %d %+v, want 201 with balance 20.00", statuses[processed], results[processed])
	}
	if statuses[rejected] != http.StatusUnprocessableEntity || results[rejected].FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("loser: %d %+v, want 422 INSUFFICIENT_FUNDS", statuses[rejected], results[rejected])
	}
	requireLedgerMatchesBalance(t, w.ID, 2000)
	if n := countRows(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("debit entries = %d, want 1", n)
	}

	// Resending both, to the third instance, changes nothing.
	for name, op := range map[string]operation{"a": betA, "b": betB} {
		status, result := submit(t, instances[2].baseURL, token, op)
		wantStatus := http.StatusUnprocessableEntity
		if name == processed {
			wantStatus = http.StatusOK
		}
		if status != wantStatus || !result.IdempotentReplay || result.TransactionID != results[name].TransactionID {
			t.Fatalf("resend of %s: %d %+v, want %d replaying %s", name, status, result, wantStatus, results[name].TransactionID)
		}
	}
	requireLedgerMatchesBalance(t, w.ID, 2000)
}

func TestTheSameBetFiftyTimesInParallelDebitsOnce(t *testing.T) {
	purge(t, inboundQueue)
	instances := startInstances(t, 3)
	token := accessToken(t, "provider-a")
	w := openWallet(t, instances[0].baseURL, "100.00")
	bet := newOperation(w, "BET", "fifty-"+w.ID, "25.00")

	const httpCopies, sqsCopies = 50, 5
	var (
		mu      sync.Mutex
		created int
		replays int
		other   []int
	)
	calls := make([]func(), 0, httpCopies)
	for i := range httpCopies {
		calls = append(calls, func() {
			status, result := submit(t, instances[i%len(instances)].baseURL, token, bet)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case status == http.StatusCreated && !result.IdempotentReplay:
				created++
			case status == http.StatusOK && result.IdempotentReplay && result.Balance.Amount == "75.00":
				replays++
			default:
				other = append(other, status)
			}
		})
	}
	atOnce(calls...)
	for i := range sqsCopies {
		sendWager(t, w.ID, wagerMessage(fmt.Sprintf("msg-fifty-%d-%s", i, w.ID), bet))
	}

	if created != 1 || replays != httpCopies-1 || len(other) > 0 {
		t.Fatalf("created %d, replays %d, other %v; want 1 and %d", created, replays, other, httpCopies-1)
	}
	eventually(t, 30*time.Second, "the SQS copies to be answered as duplicates", func() bool {
		var duplicates float64
		for _, inst := range instances {
			duplicates += metric(t, inst, "wallet_idempotent_replays_total", `channel="sqs"`)
		}
		return duplicates == sqsCopies
	})
	var httpReplays float64
	for _, inst := range instances {
		httpReplays += metric(t, inst, "wallet_idempotent_replays_total", `channel="http"`)
	}
	if httpReplays != httpCopies-1 {
		t.Fatalf("instances counted %v HTTP replays, want %d", httpReplays, httpCopies-1)
	}
	if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.ID); n != 1 {
		t.Fatalf("BET transactions = %d, want 1", n)
	}
	requireLedgerMatchesBalance(t, w.ID, 7500)
}

func TestDistinctWalletsAreProcessedInParallelAcrossInstances(t *testing.T) {
	instances := startInstances(t, 3)
	token := accessToken(t, "provider-a")
	const walletCount, betsPerWallet = 20, 5
	wallets := make([]walletJSON, walletCount)
	for i := range wallets {
		wallets[i] = openWallet(t, instances[i%len(instances)].baseURL, "100.00")
	}

	var (
		mu       sync.Mutex
		failures []string
		calls    []func()
	)
	for i, w := range wallets {
		for bet := range betsPerWallet {
			calls = append(calls, func() {
				op := newOperation(w, "BET", fmt.Sprintf("parallel-%d-%s", bet, w.ID), "10.00")
				if status, result := submit(t, instances[(i+bet)%len(instances)].baseURL, token, op); status != http.StatusCreated {
					mu.Lock()
					defer mu.Unlock()
					failures = append(failures, fmt.Sprintf("%s: %d %+v", op.ExternalTransactionID, status, result))
				}
			})
		}
	}
	started := time.Now()
	atOnce(calls...)
	elapsed := time.Since(started)
	if len(failures) > 0 {
		t.Fatalf("%d bets failed: %v", len(failures), failures)
	}
	for _, w := range wallets {
		requireLedgerMatchesBalance(t, w.ID, 5000)
		if got := walletState(t, w.ID); got.version != 1+betsPerWallet {
			t.Fatalf("wallet %s at version %d, want %d", w.ID, got.version, 1+betsPerWallet)
		}
	}
	t.Logf("%d bets on %d wallets across %d instances in %v", walletCount*betsPerWallet, walletCount, len(instances), elapsed)
}

func TestTheSameOperationOverHTTPAndSQSAtOnce(t *testing.T) {
	purge(t, inboundQueue)
	instances := startInstances(t, 3)
	token := accessToken(t, "provider-a")
	w := openWallet(t, instances[0].baseURL, "100.00")
	bet := newOperation(w, "BET", "cross-channel-"+w.ID, "30.00")

	var status int
	atOnce(
		func() { status, _ = submit(t, instances[1].baseURL, token, bet) },
		func() { sendWager(t, w.ID, wagerMessage("msg-cross-"+w.ID, bet)) },
	)
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("HTTP status %d", status)
	}
	eventually(t, 30*time.Second, "the SQS copy to be consumed", func() bool {
		return countRows(t, `SELECT count(*) FROM inbox_messages WHERE message_id = $1`, "msg-cross-"+w.ID) == 1
	})
	if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.ID); n != 1 {
		t.Fatalf("BET transactions = %d, want 1", n)
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}

func TestRestartPreservesIdempotencyAndResumesPendingReferences(t *testing.T) {
	token := accessToken(t, "provider-a")
	first := startInstance(t, "before-restart", nil)
	w := openWallet(t, first.baseURL, "100.00")
	bet := newOperation(w, "BET", "restart-bet-"+w.ID, "10.00")
	if status, _ := submit(t, first.baseURL, token, bet); status != http.StatusCreated {
		t.Fatalf("bet: %d", status)
	}
	lateBet := newOperation(w, "BET", "restart-late-bet-"+w.ID, "20.00")
	refund := newOperation(w, "REFUND", "restart-refund-"+w.ID, "20.00")
	refund.Reference = lateBet.ExternalTransactionID
	if status, result := submit(t, first.baseURL, token, refund); status != http.StatusAccepted || result.Status != "PENDING_REFERENCE" {
		t.Fatalf("early refund: %d %+v", status, result)
	}

	first.kill() // a crash: nothing in memory survives
	second := startInstance(t, "after-restart", nil)

	status, replay := submit(t, second.baseURL, token, bet)
	if status != http.StatusOK || !replay.IdempotentReplay || replay.Balance.Amount != "90.00" {
		t.Fatalf("replay after restart: %d %+v, want the original outcome", status, replay)
	}
	if status, _ := submit(t, second.baseURL, token, lateBet); status != http.StatusCreated {
		t.Fatalf("late bet: %d", status)
	}
	eventually(t, 15*time.Second, "the new instance to resume the pending refund", func() bool {
		s, _ := transactionStatus(t, "provider-a", refund.ExternalTransactionID)
		return s == "PROCESSED"
	})
	requireLedgerMatchesBalance(t, w.ID, 9000)
	reconciliation := call(t, http.MethodPost, second.baseURL+"/wallets/"+w.ID+"/reconciliation", accessToken(t, "wallet-backoffice"), nil, nil)
	if reconciliation.status != http.StatusOK || !strings.Contains(string(reconciliation.body), `"consistent":true`) {
		t.Fatalf("reconciliation after restart: %d %s", reconciliation.status, reconciliation.body)
	}
}

func TestKillingAnInstanceMidTrafficLosesAndDuplicatesNothing(t *testing.T) {
	instances := startInstances(t, 3)
	token := accessToken(t, "provider-a")
	const walletCount, betsPerWallet = 3, 20
	wallets := make([]walletJSON, walletCount)
	for i := range wallets {
		wallets[i] = openWallet(t, instances[0].baseURL, "1000.00")
	}

	var ops []operation
	for _, w := range wallets {
		for bet := range betsPerWallet {
			ops = append(ops, newOperation(w, "BET", fmt.Sprintf("chaos-%d-%s", bet, w.ID), "5.00"))
		}
	}
	victim := instances[1]
	var killOnce sync.Once
	calls := make([]func(), len(ops))
	for i, op := range ops {
		calls[i] = func() {
			if i == len(ops)/3 {
				killOnce.Do(victim.kill)
			}
			// Clients retry with the same idempotency key until an instance answers.
			for attempt := 0; ; attempt++ {
				inst := instances[rand.IntN(len(instances))]
				status, err := trySubmit(inst.baseURL, token, op)
				if err == nil && status == http.StatusCreated || status == http.StatusOK {
					return
				}
				if attempt > 50 {
					t.Errorf("%s never succeeded (last status %d, %v)", op.ExternalTransactionID, status, err)
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	atOnce(calls...)

	for _, w := range wallets {
		if n := countRows(t, `SELECT count(*) FROM wagering_transactions WHERE wallet_id = $1 AND kind = 'BET'`, w.ID); n != betsPerWallet {
			t.Fatalf("wallet %s has %d BETs, want exactly %d", w.ID, n, betsPerWallet)
		}
		requireLedgerMatchesBalance(t, w.ID, 100000-betsPerWallet*500)
	}
	// Events the killed instance had claimed are taken over once their lease expires.
	eventually(t, 30*time.Second, "every event to be published", func() bool {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
}

// trySubmit is submit without failing the test on connection errors, for
// clients that retry against another instance.
func trySubmit(baseURL, token string, op operation) (int, error) {
	body := strings.NewReader(fmt.Sprintf(`{"providerId":%q,"externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":%q,"gameId":%q,"kind":%q,"money":{"amount":%q,"currency":%q}}`,
		op.ProviderID, op.ExternalTransactionID, op.PlayerID, op.WalletID, op.RoundID, op.GameID, op.Kind, op.Money.Amount, op.Money.Currency))
	req, err := http.NewRequest(http.MethodPost, baseURL+"/wagering/transactions", body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", op.key())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}
