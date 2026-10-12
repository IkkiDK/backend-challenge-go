//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stopDeadline is the shutdown budget given to a consumer whose message waits
// on a lock that outlives it.
const stopDeadline = 300 * time.Millisecond

func TestStoppingTheConsumerReleasesWhatItHasNotFinished(t *testing.T) {
	purge(t, inboundQueue)
	var (
		ctx   = t.Context()
		first = newInProcess(t, testPolicy)
		w     = openWalletInProcess(t, first, "100.00")
	)

	// A slow writer holds the wallet's row lock, so the consumer gets stuck on
	// the first message of the wallet's group.
	owner, err := env.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = owner.Rollback(ctx) }()
	if _, err := owner.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID); err != nil {
		t.Fatal(err)
	}
	var ownerPID int
	if err := owner.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&ownerPID); err != nil {
		t.Fatal(err)
	}

	bets := []operation{
		newOperation(w, "BET", "shutdown-1-"+w.ID, "10.00"),
		newOperation(w, "BET", "shutdown-2-"+w.ID, "20.00"),
	}
	for _, bet := range bets {
		sendWager(t, w.ID, wagerMessage("msg-"+bet.ExternalTransactionID, bet))
	}
	cfg := testConsumerConfig(10 * time.Second)
	cfg.Concurrency = 1
	consumer := startConsumer(t, first, sqsClient(t, "wallet-service"), cfg)
	eventually(t, 5*time.Second, "the consumer to wait for the wallet lock", func() bool {
		return countRows(t, `SELECT count(*) FROM pg_stat_activity
			WHERE usename = 'wallet_app' AND wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(pid))`, ownerPID) == 1
	})

	stopCtx, cancel := context.WithTimeout(ctx, stopDeadline)
	defer cancel()
	if err := consumer.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop with a message in flight = %v, want the expired deadline reported", err)
	}
	// The message in flight was cancelled and released, and so was the rest of
	// its group, which was received in the same batch. Nothing was retried.
	if released, retried := sqsMessages(first, "released"), sqsMessages(first, "retried"); released != 2 || retried != 0 {
		t.Fatalf("released %v, retried %v; want both messages released and none retried", released, retried)
	}

	if err := owner.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, bet := range bets {
		if status, _ := transactionStatus(t, "provider-a", bet.ExternalTransactionID); status != "" {
			t.Fatalf("%s was recorded as %s by a cancelled unit of work", bet.ExternalTransactionID, status)
		}
	}
	if n := countRows(t, `SELECT count(*) FROM inbox_messages WHERE message_id IN ($1, $2)`,
		"msg-"+bets[0].ExternalTransactionID, "msg-"+bets[1].ExternalTransactionID); n != 0 {
		t.Fatalf("inbox records = %d, want none for cancelled messages", n)
	}
	requireLedgerMatchesBalance(t, w.ID, 10000)

	// Released messages are visible again at once, so another consumer settles
	// them long before the 30-second visibility timeout would have returned them.
	second := newInProcess(t, testPolicy)
	startConsumer(t, second, sqsClient(t, "wallet-service"), testConsumerConfig(10*time.Second))
	eventually(t, 10*time.Second, "another consumer to settle both released messages", func() bool {
		for _, bet := range bets {
			if status, _ := transactionStatus(t, "provider-a", bet.ExternalTransactionID); status != "PROCESSED" {
				return false
			}
		}
		return true
	})
	// The group kept its order: the bets produced wallet versions 2 and 3.
	for i, bet := range bets {
		version := countRows(t, `SELECT l.wallet_version FROM wallet_ledger_entries l
			JOIN wagering_transactions t ON t.id = l.transaction_id
			WHERE t.provider_id = 'provider-a' AND t.external_transaction_id = $1`, bet.ExternalTransactionID)
		if version != i+2 {
			t.Fatalf("%s produced wallet version %d, want %d", bet.ExternalTransactionID, version, i+2)
		}
	}
	requireLedgerMatchesBalance(t, w.ID, 7000)
}
