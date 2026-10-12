// Package config loads and validates the service configuration from
// environment variables. Every variable is listed in .env.example.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/auth"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/outbox"
	"github.com/IkkiDK/backend-challenge-go/internal/postgres"
)

// maxLongPollWait is the SQS limit of WaitTimeSeconds.
const maxLongPollWait = 20 * time.Second

// Config is the complete service configuration.
type Config struct {
	InstanceID      string
	LogLevel        slog.Level
	StartTimeout    time.Duration
	ShutdownTimeout time.Duration

	HTTP       HTTP
	Database   postgres.Config
	Auth       auth.Config
	SQS        SQS
	Consumer   Consumer
	Outbox     Outbox
	References References
}

// HTTP configures the API server.
type HTTP struct {
	Addr              string
	RequestTimeout    time.Duration
	ReadHeaderTimeout time.Duration
}

// SQS locates the broker and the queues. Credentials come from the standard
// AWS chain (environment variables or a shared credentials file and profile).
type SQS struct {
	Region          string
	Endpoint        string
	InboundQueue    string
	DeadLetterQueue string
	EventsQueue     string
}

// Consumer configures the wager-transactions consumer.
type Consumer struct {
	Enabled bool
	// Name identifies the consumer in the inbox.
	Name string
	// Concurrency is how many message groups (wallets) are handled at once.
	Concurrency int
	// WaitTime is the long-polling wait of each receive, which also bounds
	// how long shutdown waits for the receive in flight.
	WaitTime time.Duration
	// ProcessingTimeout bounds the handling of one message; it must stay
	// below the queue's visibility timeout.
	ProcessingTimeout time.Duration
	// RetryBaseDelay and RetryMaxDelay bound the visibility backoff applied
	// to messages that failed transiently.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// Outbox configures the outbox relay.
type Outbox struct {
	Enabled      bool
	PollInterval time.Duration
	Relay        outbox.Config
}

// References configures the pending-reference worker.
type References struct {
	Enabled      bool
	PollInterval time.Duration
	Policy       domain.ReferenceRetryPolicy
}

// Load reads the configuration through getenv (os.Getenv in production) and
// reports every missing or invalid variable at once.
func Load(getenv func(string) string) (Config, error) {
	l := loader{getenv: getenv}
	hostname, _ := os.Hostname()
	instanceID := l.string("INSTANCE_ID", hostname)

	cfg := Config{
		InstanceID:      instanceID,
		LogLevel:        l.logLevel("LOG_LEVEL", slog.LevelInfo),
		StartTimeout:    l.duration("START_TIMEOUT", 60*time.Second),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 25*time.Second),
		HTTP: HTTP{
			Addr:              l.string("HTTP_ADDR", ":8080"),
			RequestTimeout:    l.duration("HTTP_REQUEST_TIMEOUT", 10*time.Second),
			ReadHeaderTimeout: l.duration("HTTP_READ_HEADER_TIMEOUT", 5*time.Second),
		},
		Database: postgres.Config{
			URL:                      l.required("DATABASE_URL"),
			MaxConns:                 int32(l.int("DATABASE_MAX_CONNS", 20)),
			LockTimeout:              l.duration("DATABASE_LOCK_TIMEOUT", 5*time.Second),
			StatementTimeout:         l.duration("DATABASE_STATEMENT_TIMEOUT", 15*time.Second),
			IdleInTransactionTimeout: l.duration("DATABASE_IDLE_IN_TRANSACTION_TIMEOUT", 30*time.Second),
		},
		Auth: auth.Config{
			Issuer:   l.required("OIDC_ISSUER"),
			JWKSURL:  l.required("OIDC_JWKS_URL"),
			Audience: l.string("OIDC_AUDIENCE", "wallet-api"),
		},
		SQS: SQS{
			Region:          l.string("AWS_REGION", "us-east-1"),
			Endpoint:        l.string("AWS_ENDPOINT_URL", ""),
			InboundQueue:    l.string("SQS_INBOUND_QUEUE", "wager-transactions.fifo"),
			DeadLetterQueue: l.string("SQS_DEAD_LETTER_QUEUE", "wager-transactions-dlq.fifo"),
			EventsQueue:     l.string("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		},
		Consumer: Consumer{
			Enabled:           l.bool("CONSUMER_ENABLED", true),
			Name:              l.string("CONSUMER_NAME", "wallet-service.wager-transactions"),
			Concurrency:       l.int("CONSUMER_CONCURRENCY", 8),
			WaitTime:          l.duration("CONSUMER_WAIT_TIME", 10*time.Second),
			ProcessingTimeout: l.duration("CONSUMER_PROCESSING_TIMEOUT", 10*time.Second),
			RetryBaseDelay:    l.duration("CONSUMER_RETRY_BASE_DELAY", 2*time.Second),
			RetryMaxDelay:     l.duration("CONSUMER_RETRY_MAX_DELAY", 60*time.Second),
		},
		Outbox: Outbox{
			Enabled:      l.bool("OUTBOX_ENABLED", true),
			PollInterval: l.duration("OUTBOX_POLL_INTERVAL", 250*time.Millisecond),
			Relay: outbox.Config{
				Owner:          instanceID,
				BatchSize:      l.int("OUTBOX_BATCH_SIZE", 50),
				Lease:          l.duration("OUTBOX_LEASE", 30*time.Second),
				PublishTimeout: l.duration("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second),
				RetryBaseDelay: l.duration("OUTBOX_RETRY_BASE_DELAY", time.Second),
				RetryMaxDelay:  l.duration("OUTBOX_RETRY_MAX_DELAY", 5*time.Minute),
			},
		},
		References: References{
			Enabled:      l.bool("REFERENCE_WORKER_ENABLED", true),
			PollInterval: l.duration("REFERENCE_POLL_INTERVAL", 500*time.Millisecond),
			Policy: domain.ReferenceRetryPolicy{
				BaseDelay:   l.duration("REFERENCE_RETRY_BASE_DELAY", time.Second),
				MaxDelay:    l.duration("REFERENCE_RETRY_MAX_DELAY", 30*time.Second),
				MaxAttempts: l.int("REFERENCE_MAX_ATTEMPTS", 12),
			},
		},
	}
	l.check(cfg.InstanceID != "", "INSTANCE_ID is required when the hostname is unknown")
	l.check(cfg.Database.MaxConns > 0, "DATABASE_MAX_CONNS must be positive")
	l.check(cfg.Consumer.Concurrency > 0, "CONSUMER_CONCURRENCY must be positive")
	l.check(cfg.Consumer.ProcessingTimeout > 0, "CONSUMER_PROCESSING_TIMEOUT must be positive")
	l.check(cfg.Consumer.WaitTime >= 0 && cfg.Consumer.WaitTime <= maxLongPollWait && cfg.Consumer.WaitTime%time.Second == 0,
		"CONSUMER_WAIT_TIME must be whole seconds from 0s to 20s")
	l.check(cfg.Consumer.RetryBaseDelay > 0 && cfg.Consumer.RetryMaxDelay >= cfg.Consumer.RetryBaseDelay,
		"CONSUMER_RETRY_BASE_DELAY must be positive and not above CONSUMER_RETRY_MAX_DELAY")
	l.check(cfg.HTTP.RequestTimeout > 0 && cfg.ShutdownTimeout > 0 && cfg.StartTimeout > 0,
		"HTTP_REQUEST_TIMEOUT, START_TIMEOUT and SHUTDOWN_TIMEOUT must be positive")
	l.check(cfg.Outbox.PollInterval > 0 && cfg.References.PollInterval > 0,
		"OUTBOX_POLL_INTERVAL and REFERENCE_POLL_INTERVAL must be positive")
	l.keep(cfg.Outbox.Relay.Validate())
	l.keep(cfg.References.Policy.Validate())
	return cfg, errors.Join(l.errs...)
}

type loader struct {
	getenv func(string) string
	errs   []error
}

func (l *loader) check(ok bool, message string) {
	if !ok {
		l.errs = append(l.errs, errors.New(message))
	}
}

func (l *loader) keep(err error) {
	if err != nil {
		l.errs = append(l.errs, err)
	}
}

func (l *loader) string(name, fallback string) string {
	if value := l.getenv(name); value != "" {
		return value
	}
	return fallback
}

func (l *loader) required(name string) string {
	value := l.getenv(name)
	l.check(value != "", name+" is required")
	return value
}

func (l *loader) duration(name string, fallback time.Duration) time.Duration {
	raw := l.getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", name, err))
	}
	return value
}

func (l *loader) int(name string, fallback int) int {
	raw := l.getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", name, err))
	}
	return value
}

func (l *loader) bool(name string, fallback bool) bool {
	raw := l.getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", name, err))
	}
	return value
}

func (l *loader) logLevel(name string, fallback slog.Level) slog.Level {
	raw := l.getenv(name)
	if raw == "" {
		return fallback
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", name, err))
	}
	return level
}
