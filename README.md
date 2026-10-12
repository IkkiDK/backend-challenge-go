# Wallet service: distributed bet processing in Go

A Go service that moves player wallets for game providers (BET, WIN, LOSS, REFUND, ROLLBACK) over an HTTP API and an SQS consumer with the same guarantees:

- exact money,
- an append-only ledger,
- persistent idempotency,
- per-wallet concurrency control enforced by PostgreSQL,
- a transactional inbox and outbox,
- OAuth 2.0 client-credentials authentication against Keycloak.

It runs as several independent instances that share only PostgreSQL, SQS and the identity provider.

The design decisions, invariants, state machine, trade-offs and limitations are in **[ARCHITECTURE.md](ARCHITECTURE.md)**. This README covers running, calling and testing the service.

## Contents

- [Stack](#stack)
- [Quick start](#quick-start)
- [Calling the API](#calling-the-api)
- [Sending operations through SQS](#sending-operations-through-sqs)
- [HTTP contract](#http-contract)
- [Configuration](#configuration)
- [Database migrations](#database-migrations)
- [Queues and broker identities](#queues-and-broker-identities)
- [Tests](#tests)
- [Load test](#load-test)
- [Observability](#observability)
- [Project layout](#project-layout)

## Stack

| Concern | Choice |
| --- | --- |
| Language | Go 1.27.2 (`go.mod`, `Dockerfile`) |
| Composition | Uber Fx, imported only by the `internal/bootstrap` composition root and by `cmd/wallet`, which runs it |
| HTTP | `net/http` with Go 1.22+ routing patterns |
| Persistence | PostgreSQL 18, `pgx/v5` with explicit SQL, `golang-migrate` migrations |
| Messaging | Amazon SQS FIFO through [MiniStack](https://github.com/ministackorg/ministack) with IAM enforcement (`AUTH=true`) |
| Identity provider | Keycloak 26, OAuth 2.0 `client_credentials`, JWTs validated locally against the JWKS |
| Observability | JSON logs (`log/slog`), Prometheus metrics, liveness and readiness endpoints |
| Tests | `testing`, testcontainers-go (real PostgreSQL, Keycloak, MiniStack), goleak |

> **Why MiniStack and not LocalStack?** Since release 2026.03, LocalStack needs an account auth token even for the free tier. MiniStack is the MIT-licensed alternative the challenge allows. It speaks the SQS JSON protocol used by aws-sdk-go-v2 and supports FIFO queues and dead-letter redrive. Started with `AUTH=true`, it maps every access key to its IAM user and enforces that user's policies; `test/integration/broker_policy_test.go` proves the refusals. Unlike AWS, it does not verify the request signature, so locally the access key id alone identifies a caller.

## Quick start

Prerequisites: Docker with Compose v2. Go 1.27.2 is only needed to run the tests, the load test or the service on the host. The examples below use `bash`, `curl`, `jq` and `uuidgen` (on Debian and Ubuntu, `uuidgen` comes with `uuid-runtime`). They carry `#` comments: in zsh, the macOS default, run `setopt interactivecomments` first.

```sh
docker compose up --build
```

This starts the following:

| Service | What it is | Host port |
| --- | --- | --- |
| `postgres` | PostgreSQL 18; an init script creates the restricted `wallet_app` role | 55432 |
| `keycloak` | Keycloak, realm `wallet` imported from `deploy/keycloak/wallet-realm.json` | 8180 |
| `ministack` | SQS with IAM enforcement; queues and identities provisioned by `deploy/ministack/ready.d/10-provision.sh` | 4566 |
| `migrate` | One-shot `migrate up` as the schema owner | none |
| `wallet-1`, `wallet-2`, `wallet-3` | Three independent instances. Each serves the API, consumes `wager-transactions.fifo`, relays the outbox and resolves pending references | 8081, 8082, 8083 |

The stack is ready when each instance logs `http server listening`; it takes about 30 seconds on a warm machine. Then:

```sh
curl -s localhost:8081/health/ready   # {"checks":{"postgres":"UP","sqs":"UP"},"status":"UP"}
```

The default ports can be changed in `.env` (see [`.env.example`](.env.example)). `docker compose down -v` removes everything, data included.

### Test identities

Keycloak issues tokens with the `client_credentials` grant. All secrets are local development values.

| Client id | Secret | Token grants |
| --- | --- | --- |
| `provider-a` | `provider-a-local-secret` | `wagering:write`, `wagering:read`, claim `provider_id=provider-a` |
| `provider-b` | `provider-b-local-secret` | `wagering:write`, `wagering:read`, claim `provider_id=provider-b` |
| `provider-a-short-lived` | `provider-a-short-lived-local-secret` | Same as `provider-a`, but tokens expire after 3 seconds (expiry tests) |
| `wallet-backoffice` | `wallet-backoffice-local-secret` | `wallets:write`, `wallets:read`, `wallets:reconcile`, `wagering:read-all` (internal service) |
| `foreign-service` | `foreign-service-local-secret` | Powerful scopes, but no `wallet-api` audience: always rejected |

```sh
token() {
  curl -s localhost:8180/realms/wallet/protocol/openid-connect/token \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$1-local-secret" | jq -r .access_token
}
INTERNAL=$(token wallet-backoffice)
PROVIDER=$(token provider-a)
```

Tokens live 5 minutes. Get new ones if a call answers `401`.

## Calling the API

```sh
API=localhost:8081

# Open a wallet with 100.00 BRL (internal service only).
WALLET=$(curl -s -X POST $API/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$(uuidgen | tr A-Z a-z)\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
echo "$WALLET"   # {"id":"…","playerId":"…","balance":{"amount":"100.00","currency":"BRL"},"version":1,…}
WALLET_ID=$(echo "$WALLET" | jq -r .id); PLAYER_ID=$(echo "$WALLET" | jq -r .playerId)

# A provider operation: the Idempotency-Key header is mandatory.
# Usage: bet <externalTransactionId> <kind> <amount> [<referenceExternalTransactionId>]
bet() {
  curl -s -w ' [%{http_code}]\n' -X POST $API/wagering/transactions \
    -H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$1" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$1\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",
         \"roundId\":\"round-1\",\"gameId\":\"fortune-chimp\",\"kind\":\"$2\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}
         ${4:+,\"referenceExternalTransactionId\":\"$4\"}}"
}

bet tx-1 BET 25.00            # 201 {"transactionId":"…","status":"PROCESSED","balance":{"amount":"75.00",…},"idempotentReplay":false}
bet tx-1 BET 25.00            # 200 same outcome, "idempotentReplay":true
bet tx-1 BET 30.00            # 409 IDEMPOTENCY_KEY_REUSED
bet tx-2 BET 500.00           # 422 "status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS"
bet tx-3 LOSS 0.00            # 201 PROCESSED, balance unchanged, wallet version unchanged
bet tx-4 REFUND 10.00 tx-5    # 202 "status":"PENDING_REFERENCE" (tx-5 has not arrived yet)
bet tx-5 BET 10.00            # 201; tx-4 settles at its next scheduled attempt (1 s, 2 s, 4 s … up to 30 s)

# Reads
curl -s -H "Authorization: Bearer $PROVIDER" $API/providers/provider-a/wagering/transactions/tx-4 | jq
curl -s -H "Authorization: Bearer $INTERNAL" $API/wallets/$WALLET_ID | jq
curl -s -H "Authorization: Bearer $INTERNAL" "$API/wallets/$WALLET_ID/ledger?limit=2" | jq   # then ?cursor=<nextCursor>
curl -s -X POST -H "Authorization: Bearer $INTERNAL" $API/wallets/$WALLET_ID/reconciliation | jq
curl -s $API/metrics | grep -E '^wallet_(transaction_results|idempotent_replays|outbox_published)'
```

Any instance answers any request. Try sending the same operation to `8081`, `8082` and `8083`.

## Sending operations through SQS

Providers that use the queue publish to `wager-transactions.fifo` through the provider gateway, which holds the `wager-producer` identity: IAM allows it to send to that queue and nothing else. The gateway authenticates each provider before it enqueues, so the consumer takes the provider from `data.providerId` and re-validates everything else ([ARCHITECTURE.md § Authentication and authorization](ARCHITECTURE.md#authentication-and-authorization) explains this trust boundary). The consumer shares the HTTP use case. The idempotency key travels as `data.idempotencyKey`, and the envelope `messageId` is deduplicated by the inbox.

The example sends with a random `MessageDeduplicationId`, so resending the same file reaches the consumer and shows the inbox answering the duplicate. Real producers send the envelope `messageId` there, so SQS itself drops their retries within 5 minutes.

```sh
cat > message.json <<EOF
{"messageId":"msg-1","type":"WagerTransactionRequested","occurredAt":"2026-10-12T12:00:00.000Z",
 "data":{"providerId":"provider-a","externalTransactionId":"sqs-1","idempotencyKey":"provider-a:sqs-1",
         "playerId":"$PLAYER_ID","walletId":"$WALLET_ID","roundId":"round-2","gameId":"fortune-chimp",
         "kind":"BET","money":{"amount":"5.00","currency":"BRL"}}}
EOF
docker compose exec -T ministack sh -c 'AWS_SHARED_CREDENTIALS_FILE=/shared/aws/credentials AWS_DEFAULT_REGION=us-east-1 \
  aws --profile wager-producer --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$0" --message-deduplication-id "$(cat /proc/sys/kernel/random/uuid)" \
  --message-body "$(cat)"' "$WALLET_ID" < message.json

sleep 2   # give a consumer time to receive it
curl -s -H "Authorization: Bearer $PROVIDER" $API/providers/provider-a/wagering/transactions/sqs-1 | jq .status
```

A `404` problem document instead means no consumer has handled the message yet.

Downstream consumers read the published events with the `events-reader` identity:

```sh
docker compose exec ministack sh -c 'AWS_SHARED_CREDENTIALS_FILE=/shared/aws/credentials AWS_DEFAULT_REGION=us-east-1 \
  aws --profile events-reader --endpoint-url http://localhost:4566 sqs receive-message \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo --max-number-of-messages 10 \
  --message-attribute-names All --query "Messages[].Body"'
```

The message contract, `MessageGroupId`, `MessageDeduplicationId`, retries and DLQ handling are described in [ARCHITECTURE.md § SQS consumer](ARCHITECTURE.md#sqs-consumer).

## HTTP contract

All business endpoints need `Authorization: Bearer <access token>`. Health and metrics are public.

| Method and path | Scope |
| --- | --- |
| `POST /wallets` | `wallets:write` |
| `GET /wallets/{walletId}` | `wallets:read` |
| `GET /wallets/{walletId}/ledger?limit=…&cursor=…` | `wallets:read` |
| `POST /wallets/{walletId}/reconciliation` | `wallets:reconcile` |
| `POST /wagering/transactions` | `wagering:write`, and the body's `providerId` must equal the token's `provider_id` |
| `GET /wagering/transactions/{transactionId}` | `wagering:read` (own provider) or `wagering:read-all` |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | `wagering:read` for its own `providerId`, or `wagering:read-all` |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | public |

Every response carries `X-Correlation-ID`: the client's value when it sends a safe one, otherwise a new UUID.

The ledger is paged in wallet-version order. `limit` runs from 1 to 200 (default 50); `nextCursor`, present while more entries follow, is passed back as `cursor`. Any other `limit` or a malformed cursor answers `400 VALIDATION_FAILED` naming the parameter.

### Outcomes of `POST /wagering/transactions`

The outcome is always told apart by the HTTP status. A replay answers with the status of the stored outcome and `"idempotentReplay": true`: `200` for `PROCESSED` (only the request that processed it gets `201`), `202` while `PENDING_REFERENCE`, `422` for `REJECTED` and `500` for `FAILED`, always with the balance observed by the original processing. A result balance is always in the wallet's currency, also when a `CURRENCY_MISMATCH` request named another one.

| Status | Meaning | Body |
| --- | --- | --- |
| `201 Created` | Processed now | `{"transactionId","status":"PROCESSED","balance","idempotentReplay":false}` |
| `200 OK` | Replay of a processed operation | Same body, `"idempotentReplay":true` |
| `202 Accepted` | Waiting for its reference (`PENDING_REFERENCE`); track it with the GET endpoints | `{"transactionId","status":"PENDING_REFERENCE","idempotentReplay"}` |
| `422 Unprocessable Entity` | Rejected by a business rule; the rejection is recorded and replays the same way | `{"transactionId","status":"REJECTED","failureCode","failureClass","balance","idempotentReplay"}`; no `balance` for `WALLET_PLAYER_MISMATCH`, whose caller named a player who does not own the wallet |
| `500 Internal Server Error` with a transaction body | Recorded as a permanent processing failure (`FAILED`); retrying cannot succeed | `{"transactionId","status":"FAILED","failureCode":"PROCESSING_FAILED",…}` |

### Error responses

Errors are [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem documents (`application/problem+json`): `{"title","status","code","detail","correlationId","errors":[{"field","code","message"}]}`. Nothing is recorded for any of them, so the same request can be corrected and resent with the same `Idempotency-Key`.

| Status | `code` | When |
| --- | --- | --- |
| `400` | `MALFORMED_REQUEST` | The body is not one JSON object with known fields only (an amount sent as a JSON number fails here) |
| `400` | `VALIDATION_FAILED` | Invalid fields; `errors[]` lists each one (codes below) |
| `401` | `UNAUTHENTICATED` | Missing, malformed, expired or untrusted token (`WWW-Authenticate: Bearer`, with `error="invalid_token"` when a token was sent) |
| `403` | `FORBIDDEN` | Missing scope (`WWW-Authenticate: Bearer error="insufficient_scope"`), or another provider's data or identity |
| `404` | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` | Unknown, or not visible to the caller |
| `409` | `IDEMPOTENCY_KEY_REUSED` | The key was used for a different payload |
| `409` | `EXTERNAL_TRANSACTION_CONFLICT` | The `(providerId, externalTransactionId)` was recorded under another key |
| `409` | `WALLET_ALREADY_EXISTS` | The player already has a wallet in that currency |
| `415` | `UNSUPPORTED_MEDIA_TYPE` | `Content-Type` is not `application/json` |
| `503` | `SERVICE_UNAVAILABLE` | Transient dependency failure (with `Retry-After`); resend with the same key |

Field `errors[].code` values: `REQUIRED`, `INVALID_FORMAT`, `TOO_LONG`, `INVALID_AMOUNT`, `UNSUPPORTED_CURRENCY`, `AMOUNT_OUT_OF_RANGE`, `NEGATIVE_AMOUNT`, `AMOUNT_MUST_BE_POSITIVE`, `AMOUNT_MUST_BE_ZERO`, `UNSUPPORTED_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `SELF_REFERENCE`.

### Failure codes of rejected and failed transactions

`failureClass` tells a provider whether it can fix the input and submit a new operation (`CORRECTABLE`), or whether the outcome is final (`DEFINITIVE`).

| `failureCode` | Class | Meaning |
| --- | --- | --- |
| `WALLET_PLAYER_MISMATCH` | CORRECTABLE | The wallet belongs to another player |
| `CURRENCY_MISMATCH` | CORRECTABLE | The amount's currency differs from the wallet's |
| `REFERENCE_KIND_NOT_ALLOWED` | CORRECTABLE | The reference has a kind this operation cannot target (for example, a REFUND of a WIN) |
| `REFERENCE_MISMATCH` | CORRECTABLE | The reference has another wallet, player, currency or round |
| `REFERENCE_AMOUNT_MISMATCH` | CORRECTABLE | A reversal must have the referenced amount (partial reversals are not supported) |
| `INSUFFICIENT_FUNDS` | DEFINITIVE | A BET above the balance |
| `INSUFFICIENT_FUNDS_FOR_REVERSAL` | DEFINITIVE | A ROLLBACK of a WIN or REFUND would need more than the balance |
| `BALANCE_LIMIT_EXCEEDED` | DEFINITIVE | A credit would overflow the 64-bit balance |
| `REFERENCE_NOT_FOUND` | DEFINITIVE | The reference never arrived within the retry budget |
| `REFERENCE_NOT_PROCESSED` | DEFINITIVE | The reference was rejected or failed, or was still unresolved when the budget ran out |
| `REFERENCE_ALREADY_REVERSED` | DEFINITIVE | The reference was already refunded or rolled back |
| `PROCESSING_FAILED` | DEFINITIVE | `FAILED`: the operation violated a database invariant and was recorded for audit |

## Configuration

Every setting is an environment variable. [`.env.example`](.env.example) lists them with their defaults; `internal/config` validates them at startup and reports every problem at once. The essential ones:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | PostgreSQL as the restricted `wallet_app` role |
| `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_AUDIENCE` | Expected `iss` and `aud` of tokens, and where the signing keys are |
| `AWS_REGION`, `AWS_ENDPOINT_URL`, and AWS credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, or `AWS_SHARED_CREDENTIALS_FILE` plus `AWS_PROFILE`) | SQS access |
| `CONSUMER_ENABLED`, `OUTBOX_ENABLED`, `REFERENCE_WORKER_ENABLED` | Turn the background components on or off per instance |
| `REFERENCE_RETRY_BASE_DELAY`, `REFERENCE_RETRY_MAX_DELAY`, `REFERENCE_MAX_ATTEMPTS` | How long a reversal waits for its reference (by default 12 attempts, about 4 minutes) |
| `HTTP_REQUEST_TIMEOUT`, `HTTP_READ_HEADER_TIMEOUT`, `HTTP_READ_TIMEOUT`, `HTTP_WRITE_TIMEOUT`, `HTTP_IDLE_TIMEOUT` | Bounds on handling a request and on every phase of a connection, so slow or idle clients cannot hold connections |
| `SHUTDOWN_TIMEOUT` | Time given to drain HTTP requests, messages and workers on SIGTERM |

To run an instance on the host against the compose dependencies:

```sh
docker compose up -d --wait postgres keycloak ministack   # MiniStack is healthy once its queues and keys exist
docker compose run --rm migrate up
docker compose exec -T ministack cat /shared/aws/credentials > .aws-credentials
set -a; . ./.env.example; set +a
HTTP_ADDR=:8090 go run ./cmd/wallet
```

## Database migrations

Versioned SQL files live in [`migrations/`](migrations), each `NNNNNN_name.up.sql` with its `.down.sql`, and are embedded into the `migrate` binary. They run as the schema owner (`MIGRATIONS_DATABASE_URL`), never as the service role.

```sh
# with compose
docker compose run --rm migrate up         # apply all pending migrations (also run by `docker compose up`)
docker compose run --rm migrate down 1     # revert the last migration
docker compose run --rm migrate version    # current version
docker compose run --rm migrate force 3    # mark a version clean after repairing a failed migration

# from the host
export MIGRATIONS_DATABASE_URL=postgres://wallet_owner:wallet_owner_local@localhost:55432/wallet?sslmode=disable
go run ./cmd/migrate down 6 && go run ./cmd/migrate up
```

## Queues and broker identities

Nothing needs to be created by hand. When MiniStack is ready, `deploy/ministack/ready.d/10-provision.sh` creates the following:

| Queue | Attributes |
| --- | --- |
| `wager-transactions.fifo` | FIFO, visibility timeout 30 s, long polling 20 s, retention 4 days, redrive to the DLQ after `maxReceiveCount=5` |
| `wager-transactions-dlq.fifo` | FIFO, retention 14 days |
| `wallet-events.fifo` | FIFO, retention 14 days: the destination of the outbox events |

It also creates three IAM users with inline least-privilege policies:

- `wallet-service` consumes the inbound queue, sends to the DLQ and publishes events.
- `wager-producer` may only send to the inbound queue.
- `events-reader` may only consume the events queue.

`TestBrokerPoliciesRefuseWhatAnIdentityWasNotGranted` checks that the broker refuses each identity what its policy does not grant, and refuses unknown access keys.

Their generated access keys are written to the `sqs-credentials` volume (`/shared/aws/credentials`), which the instances mount read-only. MiniStack keeps no state across restarts, so restarting it provisions new keys and the instances must be restarted too: `docker compose restart ministack wallet-1 wallet-2 wallet-3`.

## Tests

```sh
go vet ./... && go vet -tags=integration ./...
gofmt -l .                       # prints nothing when formatted
go test ./...                    # unit tests: no Docker needed
go test -race ./...              # needs cgo and a C compiler
```

The race detector needs cgo. Where no C compiler is installed (for example, Windows), run it in a container:

```sh
docker run --rm -v "$PWD:/src" -w /src golang:1.27.2-bookworm go test -race ./...
```

### Integration tests (`-tags=integration`)

The integration tests need only Docker. A single `TestMain` starts real PostgreSQL, Keycloak and MiniStack containers through testcontainers-go, from the same provisioning files as compose. It applies the migrations, reads the provisioned broker credentials and builds the service binary. Tests run serially: some pause dependencies or kill service processes. The full run takes about 2 minutes.

```sh
go test -tags=integration -count=1 ./test/integration/...
go test -race -tags=integration -count=1 ./test/integration/...   # the service processes are built with -race too

# -race from a container (Docker Desktop):
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:/src" -w /src \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal golang:1.27.2-bookworm \
  go test -race -tags=integration -count=1 ./test/integration/...

INTEGRATION_LOG_DIR=/tmp/wallet-logs go test -tags=integration -count=1 -run TestKilling ./test/integration/...  # keep every instance's logs
```

What the suite proves, by file:

| File | Scenarios |
| --- | --- |
| `schema_test.go` | Migrations up/down/up on a fresh database; the ledger is append-only for the service role (privileges) and for the owner (triggers); a balance cannot change without its ledger entry or go negative; each ledger guard (uniqueness per transaction and per version, direction, chain) refuses its case with its exact SQLSTATE; a second opening and a second successful reversal are refused by the database |
| `app_test.go` | Opening atomicity (wallet, OPENING, ledger, events) and zero opening; replay returns the original balance; idempotency conflicts and provider-scoped keys; a reversal before its reference is resolved later, or expires with `REFERENCE_NOT_FOUND`; a REFUND racing a ROLLBACK of the same BET leaves one `PROCESSED` and one `REFERENCE_ALREADY_REVERSED`; a bet on another wallet completes while a bet waits on a locked wallet; a corrupted balance makes the next movement `FAILED`, and reconciliation reports the divergence |
| `messaging_test.go` | The consumer settles and deduplicates redeliveries; a crash after commit and before delete is redelivered to another consumer and not reprocessed; permanent failures go to the DLQ at once; a PostgreSQL outage is retried with visibility backoff until recovery; a failing message holds back the rest of its group; messages that keep failing reach the DLQ through the redrive policy; an SQS outage delays events without losing them |
| `http_test.go` | The HTTP error contract: missing fields and `Idempotency-Key`, amounts as JSON numbers, unknown fields, trailing documents and wrong media types are refused without recording anything |
| `http_contract_test.go` | `409` for a second wallet of a player and for idempotency conflicts, `404` for an unknown wallet, `503` with `Retry-After` while PostgreSQL is paused and the same key succeeding after it; ledger pagination, invalid cursors and limits |
| `outbox_test.go` | Two competing publishers publish every event exactly once, in wallet order, including events committed while no publisher ran; an event published but not confirmed (crashed publisher) is republished with the same `eventId` after its lease expires |
| `outbox_fencing_test.go` | A relay whose lease passed to another relay can neither confirm the event nor shorten the new lease |
| `outbox_claim_test.go` | A claim reads pending events only, however long the published history |
| `reversal_http_test.go` | Over HTTP: a second REFUND or a ROLLBACK of a refunded BET answers `422 REFERENCE_ALREADY_REVERSED`, a ROLLBACK of a WIN whose funds were spent answers `INSUFFICIENT_FUNDS_FOR_REVERSAL`, a REFUND of a rejected BET answers `REFERENCE_NOT_PROCESSED`; nothing moves and only a rejection event is recorded |
| `shutdown_test.go` | Stopping a consumer whose message waits on a wallet lock rolls its unit of work back and releases both messages at once; another consumer settles them in order |
| `clock_test.go` | Instances whose clocks disagree, and a wall clock stepping back during handling, still settle operations |
| `currency_test.go` | A currency mismatch reports the wallet's balance in the wallet's currency, also on replay and on read |
| `name_resolution_test.go` | The service role cannot create temporary tables, and a temporary table cannot shadow a table the triggers read |
| `broker_policy_test.go` | The broker refuses each SQS identity what its policy does not grant, and refuses unknown access keys |
| `multiprocess_test.go` | **Three independent service processes**: two 80.00 bets on 100.00 sent to different instances (one PROCESSED, one INSUFFICIENT_FUNDS, balance 20.00, one debit; resends change nothing); the same bet 50 times in parallel plus 5 SQS copies (one debit, 49 HTTP and 5 SQS replays counted by the instances' metrics); 20 wallets in parallel; the same operation over HTTP and SQS at once; restart preserves idempotency and resumes pending references; killing an instance mid-traffic loses and duplicates nothing |
| `auth_test.go` | Missing, garbage, forged-claim, foreign-audience and expired tokens are refused without effects; providers only reach their own transactions, also in replays; wallet operations are internal; a player mismatch does not reveal the wallet's balance |
| `fx_test.go` | The Fx application starts, serves, publishes and stops without leaking goroutines |

The unit tests also cover the composition and the adapters: `internal/bootstrap` validates the Fx dependency graph for every combination of enabled components and checks that the dependency checks start before any worker; `internal/outbox` proves that a batch is published concurrently and confirmed in one write; `internal/httpapi` proves that stalled and idle connections are closed; `internal/domain` pins the JSON contract of every published event.

### Spec §13 checklist

| Requirement | Tests |
| --- | --- |
| Money parsing, operations, scale, limits, invalid input, currency mismatch | `internal/money`: `TestParseRejectsNonCanonicalAmounts`, `TestParseReportsOverflow`, `TestArithmeticOverflowBoundaries`, `TestCurrencyMismatch` |
| Wallet invariants, state transitions, the five kinds, zero values, payload conflict, opening | `internal/domain`: `TestDebitAndCreditKeepTheLedgerInvariant`, `TestStateMachineAllowsOnlyDocumentedTransitions`, `TestBet` … `TestRollback`, `TestZeroValuePolicyPerKind`, `TestCheckReplay`, `TestOpenWalletWithPositiveBalanceCreditsAnOpeningTransaction` |
| Migrations, constraints, ledger immutability, financial atomicity | `schema_test.go`, `TestOpeningCommitsWalletTransactionLedgerAndEventsTogether` |
| Inbox, redelivery, concurrent outbox, retry, DLQ, restart | `messaging_test.go`, `outbox_test.go`, `outbox_fencing_test.go`, `TestRestartPreservesIdempotencyAndResumesPendingReferences` |
| Fx composition, start, stop and release of the workers | `fx_test.go`, `shutdown_test.go`, `internal/bootstrap` |
| Real IdP; missing, invalid and expired credentials; provider isolation; no effects or data exposure | `auth_test.go` |
| 1. The same bet 50 times in parallel | `TestTheSameBetFiftyTimesInParallelDebitsOnce` |
| 2. Two 80.00 bets on 100.00 | `TestTwoBetsRacingForTheSameBalanceOnDifferentInstances` |
| 3. Distinct wallets at once | `TestDistinctWalletsAreProcessedInParallelAcrossInstances`, `TestIndependentWalletsDoNotWaitForEachOther` |
| 4. Three independent instances | every test in `multiprocess_test.go` |
| 5. Consumer stopped after commit, before delete | `TestMessageCommittedButNotDeletedIsNotReprocessedOnRedelivery` |
| 6. Two publishers on one outbox, publication recovery | `TestCompetingPublishersPublishEveryEventOnceInWalletOrder`, `TestEventPublishedButNotConfirmedIsRepublishedWithTheSameID` |
| 7. A reversal before its reference | `TestReversalArrivingBeforeItsReferenceIsResolvedLater`, `TestReversalWhoseReferenceNeverArrivesExpires` |
| 8. Restart preserves idempotency, pending references and balances | `TestRestartPreservesIdempotencyAndResumesPendingReferences`, `TestKillingAnInstanceMidTrafficLosesAndDuplicatesNothing` |
| Stored balance against the ledger; HTTP and SQS for the same operation | `requireLedgerMatchesBalance` closes almost every test; `TestTheSameOperationOverHTTPAndSQSAtOnce` |
| `go test -race` | CI runs the unit and integration suites with `-race`, service processes included |

### Manual failure simulations with compose

```sh
docker compose kill wallet-2              # crash an instance; the others carry on, its outbox leases expire
docker compose pause postgres             # requests answer 503 and messages back off
docker compose unpause postgres           # everything resumes
docker compose pause ministack            # events wait in the outbox; readiness reports sqs DOWN
docker compose stop -t 40 wallet-1        # graceful SIGTERM: HTTP drains, the consumer releases its messages
```

## Load test

[`cmd/loadtest`](cmd/loadtest/main.go) drives the compose stack with wagering traffic over the three instances. It reports throughput, p50/p95/p99, errors, conflicts, the outbox lag of every event, and a reconciliation of every wallet it used. [LOADTEST.md](LOADTEST.md) records the environment, the methodology, the commands and four scenarios. In short, on a shared desktop: about 3,000 requests per second at saturation with a p99 of 62 ms, and a p99 of 16 ms at 400 operations per second, with no errors, no conflicts and every wallet consistent. The outbox keeps up until the local broker emulator saturates.

```sh
docker compose up --build -d --wait
go run ./cmd/loadtest -duration 60s -concurrency 32 -wallets 200
```

## Observability

- **Logs:** JSON on stdout, in UTC. Every record carries `service` and `instance`, plus whichever of `correlationId`, `messageId`, `transactionId`, `walletId` and `providerId` are known. Tokens, secrets and request bodies are never logged.
- **Metrics:** Prometheus text format on `GET /metrics`:

  | Metric | What it counts or measures |
  | --- | --- |
  | `wallet_transaction_results_total{channel,kind,status}` | Settled operations |
  | `wallet_idempotent_replays_total{channel}` | Duplicates |
  | `wallet_idempotency_conflicts_total{reason}` | Reused keys or ids |
  | `wallet_retries_total{component}` | Retries |
  | `wallet_pending_reference_resolutions_total{outcome}` | Reference worker attempts: rescheduled while the reference is missing, processed, rejected, failed |
  | `wallet_sqs_messages_total{outcome}` and `wallet_dead_lettered_messages_total{reason}` | Consumed messages, and permanent failures the consumer copied to the DLQ (messages the redrive policy moves are counted by SQS, not by the service) |
  | `wallet_concurrency_conflicts_total{reason}` and `wallet_lock_wait_seconds` | Concurrency conflicts |
  | `wallet_outbox_lag_seconds`, `wallet_outbox_pending_events`, `wallet_outbox_published_total`, `wallet_outbox_publish_failures_total` | Outbox delay, backlog, publications and failed publications (each retried with backoff) |
  | `wallet_processing_duration_seconds{channel}` | Processing latency |
  | `wallet_reconciliations_total{result}` and `wallet_reconciliation_divergences_total` | Reconciliation runs and divergences |
  | `wallet_http_requests_total`, `wallet_http_request_duration_seconds` | HTTP traffic |

- **Health:** `GET /health/live` reports the process only. `GET /health/ready` checks PostgreSQL and SQS, and fails while the instance drains on shutdown.

## Project layout

```text
cmd/wallet            service entry point (Fx application)
cmd/migrate           migration CLI
cmd/loadtest          load generator and report (see LOADTEST.md)
migrations/           versioned SQL migrations (embedded)
internal/money        Money value object
internal/domain       wallet, wagering transaction, ledger, events and settlement rules (no framework, transport or driver)
internal/app          use cases and persistence ports, shared by HTTP and SQS
internal/postgres     pgx implementation of the ports, units of work, error classification
internal/httpapi      handlers, authentication middleware, problem responses, health
internal/auth         access-token verification and scopes
internal/messaging    SQS broker, consumer and event publication
internal/outbox       outbox relay
internal/worker       background loop runner
internal/config       environment configuration
internal/observability  JSON logger and Prometheus metrics
internal/bootstrap    Fx composition root
deploy/               PostgreSQL init, Keycloak realm, MiniStack provisioning
test/integration      container-based integration suite
```
