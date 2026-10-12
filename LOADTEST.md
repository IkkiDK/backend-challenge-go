# Load test

The challenge lists load tests as an optional extra, with a reproducible command, the environment, the methodology, throughput, p50/p95/p99, errors, conflicts and outbox lag. This document records all of them for four scenarios. The generator is [`cmd/loadtest`](cmd/loadtest/main.go).

## Environment

| Item | Value |
| --- | --- |
| Host | Intel Core i9-13900K (24 cores, 32 threads), 64 GB RAM, Windows 11 Pro 10.0.26200 |
| Containers | Docker Desktop 28.4.0; its Linux VM has 32 CPUs and 31 GiB |
| Stack | `docker compose up` from this repository: PostgreSQL 18.6, Keycloak 26.8.0, MiniStack 1.5.26 and three service instances with the defaults of [`.env.example`](.env.example) (20 database connections per instance, `OUTBOX_BATCH_SIZE=50`, `OUTBOX_POLL_INTERVAL=250ms`, `CONSUMER_CONCURRENCY=8`) |
| Client | `cmd/loadtest` on the same host (Go 1.27.2), through Docker Desktop's port forwarding |

The host is a desktop shared with other applications, not an isolated benchmark machine. An earlier run of the same scenarios, before migration `000006` (which only changes how the relay claims events), measured 3,397 requests per second with a p99 of 26.6 ms at saturation and a p99 of 4.3 ms at 40 operations per second. The spread between runs comes from the host, so read the figures as an order of magnitude and compare runs on the same machine.

## Methodology

- **A fresh stack per scenario.** `docker compose down -v` and `up --wait`, then a 10-second warm-up at 20 operations per second, so every scenario starts with an empty database and an empty broker.
- **Workload.** Each request is a BET (65 %), a WIN (25 %) or a LOSS (10 %) on a random wallet; 10 % of the requests resend an earlier operation with the same `Idempotency-Key`, so replays are part of the load. Requests rotate over the three instances.
- **Closed loop** (saturation, contention): each client sends its next request as soon as the previous one returns. It finds the most the service sustains.
- **Open loop** (steady, moderate): requests are due at a fixed rate, and each latency counts from the moment its request was due, not from when a client got to send it. A slow service therefore shows up as latency instead of quietly lowering the send rate, which avoids coordinated omission.
- **Latency** is measured by the client, end to end, through the port forwarding of Docker Desktop.
- **Errors** are transport errors and the status codes received.
- **Conflicts** come from the service metrics, as the difference before and after the run: idempotency conflicts (`409`), retried concurrency conflicts (deadlocks, serialization failures, stale versions) and the average wait for a wallet's row lock.
- **Outbox lag** is measured per event, in PostgreSQL: `published_at − occurred_at` for every event of the run, with percentiles computed in SQL. After the load stops, the tool waits up to two minutes for the outbox to publish everything and reports either the time it took or what was left and the rate it drained at.
- **Money check.** Every wallet the run used is reconciled at the end: its stored balance against the sum of its ledger.
- **Container CPU** is sampled once, in the middle of each measured run, with `docker stats`.

## Reproduce

```sh
docker compose down -v && docker compose up --build -d --wait
go run ./cmd/loadtest -duration 60s -concurrency 32 -wallets 200                   # saturation
go run ./cmd/loadtest -duration 30s -concurrency 32 -wallets 200 -hot-wallets 4    # contention
go run ./cmd/loadtest -duration 120s -rate 40 -concurrency 16 -wallets 200         # steady load
go run ./cmd/loadtest -duration 60s -rate 400 -concurrency 64 -wallets 200         # moderate load
go run ./cmd/loadtest -h                                                           # every flag
```

Run each scenario on a fresh stack, as above, for comparable outbox figures: the emulator slows down as its deduplication cache fills (see the analysis).

## Results

### Saturation

Thirty-two clients send as fast as the service answers. This measures the most the three instances sustain; the outbox cannot keep up with it on the emulator (see the analysis).

Command settings: closed loop, 32 clients, 200 wallets, 1m0s, 3 instances.

Client: windows/amd64, 32 CPUs, go1.27.2; duplicate ratio 0.10, seed 1.

| Measure | Value |
| --- | --- |
| Requests | 180437 in 60.0 s (3007 req/s) |
| Latency p50 / p95 / p99 / max | 9.0 ms / 17.0 ms / 61.8 ms / 198.5 ms |
| Responses by status | 200: 17976, 201: 162461 |
| Transport errors | 0 |
| Idempotent replays (client / service) | 17976 / 17976 |
| Idempotency conflicts (409) | 0 |
| Concurrency conflicts retried (deadlock, serialization, stale version) | 0 |
| Wallet lock wait, average | 0.83 ms |
| Events recorded / published | 308571 / 30850 |
| Outbox lag of published events, p50 / p95 / p99 / max | 66044.4 ms / 160621.8 ms / 171803.4 ms / 175375.0 ms |
| Outbox not drained | 277721 events still pending after 120 s of draining, at 145 events/s |
| Wallets reconciled / inconsistent | 200 / 0 |

Container CPU mid-run (100 % is one core): keycloak 0.45%, ministack 103.57%, postgres 1217.10%, wallet-1 169.58%, wallet-2 167.37%, wallet-3 161.94%.

### Contention

The same closed loop on four wallets only, so every operation queues on one of four row locks.

Command settings: closed loop, 32 clients, 4 hot wallets, 30s, 3 instances.

Client: windows/amd64, 32 CPUs, go1.27.2; duplicate ratio 0.10, seed 1.

| Measure | Value |
| --- | --- |
| Requests | 31501 in 30.0 s (1049 req/s) |
| Latency p50 / p95 / p99 / max | 14.5 ms / 116.6 ms / 216.8 ms / 706.7 ms |
| Responses by status | 200: 3153, 201: 28348 |
| Transport errors | 0 |
| Idempotent replays (client / service) | 3153 / 3153 |
| Idempotency conflicts (409) | 0 |
| Concurrency conflicts retried (deadlock, serialization, stale version) | 0 |
| Wallet lock wait, average | 27.10 ms |
| Events recorded / published | 53761 / 3077 |
| Outbox lag of published events, p50 / p95 / p99 / max | 76813.9 ms / 140777.5 ms / 147377.7 ms / 148952.6 ms |
| Outbox not drained | 50684 events still pending after 120 s of draining, at 21 events/s |
| Wallets reconciled / inconsistent | 200 / 0 |

Container CPU mid-run (100 % is one core): keycloak 2.61%, ministack 0.48%, postgres 309.23%, wallet-1 0.40%, wallet-2 0.27%, wallet-3 0.29%.

### Steady load

A fixed 40 operations per second, within what the emulator publishes, to measure latency and the outbox lag of a service that keeps up.

Command settings: open loop at 40 ops/s, 16 clients, 200 wallets, 2m0s, 3 instances.

Client: windows/amd64, 32 CPUs, go1.27.2; duplicate ratio 0.10, seed 1.

| Measure | Value |
| --- | --- |
| Requests | 4800 in 120.0 s (40 req/s) |
| Latency p50 / p95 / p99 / max | 5.3 ms / 6.8 ms / 7.6 ms / 17.9 ms |
| Responses by status | 200: 445, 201: 4355 |
| Transport errors | 0 |
| Idempotent replays (client / service) | 445 / 445 |
| Idempotency conflicts (409) | 0 |
| Concurrency conflicts retried (deadlock, serialization, stale version) | 0 |
| Wallet lock wait, average | 0.20 ms |
| Events recorded / published | 8268 / 8268 |
| Outbox lag of published events, p50 / p95 / p99 / max | 114.7 ms / 239.7 ms / 264.1 ms / 306.3 ms |
| Outbox drained after the load stopped | 0.5 s |
| Wallets reconciled / inconsistent | 200 / 0 |

Container CPU mid-run (100 % is one core): keycloak 2.14%, ministack 16.70%, postgres 9.00%, wallet-1 3.58%, wallet-2 5.32%, wallet-3 5.58%.

### Moderate load

A fixed 400 operations per second: latency without saturation, and an outbox producing faster than the emulator accepts.

Command settings: open loop at 400 ops/s, 64 clients, 200 wallets, 1m0s, 3 instances.

Client: windows/amd64, 32 CPUs, go1.27.2; duplicate ratio 0.10, seed 1.

| Measure | Value |
| --- | --- |
| Requests | 24000 in 60.0 s (400 req/s) |
| Latency p50 / p95 / p99 / max | 5.7 ms / 10.3 ms / 15.8 ms / 52.6 ms |
| Responses by status | 200: 2333, 201: 21667 |
| Transport errors | 0 |
| Idempotent replays (client / service) | 2333 / 2333 |
| Idempotency conflicts (409) | 0 |
| Concurrency conflicts retried (deadlock, serialization, stale version) | 0 |
| Wallet lock wait, average | 0.24 ms |
| Events recorded / published | 41181 / 33340 |
| Outbox lag of published events, p50 / p95 / p99 / max | 34756.4 ms / 118311.5 ms / 128577.3 ms / 134873.4 ms |
| Outbox not drained | 7841 events still pending after 120 s of draining, at 137 events/s |
| Wallets reconciled / inconsistent | 200 / 0 |

Container CPU mid-run (100 % is one core): keycloak 0.18%, ministack 106.74%, postgres 262.11%, wallet-1 48.87%, wallet-2 31.65%, wallet-3 30.51%.

## Analysis

**The write path.** Three instances sustain about 3,000 requests per second, with a p99 under 65 ms at saturation and under 16 ms at 400 operations per second. There were no errors, no concurrency conflicts and no idempotency conflicts in any scenario, every replay was answered as a replay (the client's count equals the service's metric), and every wallet reconciled with its ledger. PostgreSQL is where the work goes: each operation is one SQL transaction that locks the wallet row, inserts the transaction, updates the balance, appends the ledger entry and two outbox events, and runs the triggers that check them.

**Contention.** With four wallets, each takes about 260 operations per second. Operations of one wallet queue on its row lock (27 ms of average wait), still without a single retry or deadlock: the pessimistic lock serializes instead of failing. A real player's wallet sees a few operations per second, so this scenario is a stress test of the locking, not a traffic forecast.

**The outbox and the emulator.** The outbox only lags where the broker cannot take more. MiniStack is a single-threaded Python process, and its FIFO `SendMessage` rebuilds the deduplication cache on every call, so its cost grows with the messages sent in the last five minutes. It saturates one core at roughly 140 to 220 messages per second, which the saturation and moderate scenarios show (MiniStack at about 105 % CPU). At 400 operations per second the service produces about 690 events per second, so a backlog builds and keeps draining after the load stops: two minutes later, 7,841 of 41,181 events were still waiting. None is lost: each is published, in its wallet's order, as the broker catches up. On AWS the limits are different: an SQS FIFO queue accepts 300 `SendMessage` calls per second per action without high-throughput mode, 3,000 messages per second when sent in batches of ten, and more in high-throughput mode, so the next step for this relay at scale is `SendMessageBatch`.

**Outbox claims behind a few hot wallets.** In the contention scenario the backlog drains at about 20 events per second while PostgreSQL stays busy and the instances idle. With only four wallets holding a deep backlog, each claim reads the whole backlog trying to fill its batch of 50. This is the limitation described in [ARCHITECTURE.md § Transactional outbox](ARCHITECTURE.md#transactional-outbox), with its remedies. The load test also found the opposite problem, now fixed: claims walked the whole published history, and migration `000006` gave them a partial index of pending events.

**Lag at low load.** When the service keeps up (40 operations per second), an event waits for the relay's next poll: p50 about 115 ms and p99 about 264 ms for a 250 ms `OUTBOX_POLL_INTERVAL`. A shorter interval, or a `LISTEN/NOTIFY` wake-up on commit, would bring that down to milliseconds.
