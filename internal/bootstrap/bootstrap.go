// Package bootstrap is the composition root: it wires configuration,
// connections, repositories, use cases, handlers and workers with Uber Fx.
// It is the only package that knows Fx; every component it composes exposes
// plain constructors and Start/Stop methods.
//
// Fx starts lifecycle hooks in registration order and stops them in reverse.
// Dependencies register their hooks first (startup verifies them before
// anything uses them, shutdown closes them last), then the internal workers,
// then the inputs: the SQS consumer and the HTTP server, which stop first.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/auth"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
	"github.com/IkkiDK/backend-challenge-go/internal/httpapi"
	"github.com/IkkiDK/backend-challenge-go/internal/messaging"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
	"github.com/IkkiDK/backend-challenge-go/internal/outbox"
	"github.com/IkkiDK/backend-challenge-go/internal/postgres"
	"github.com/IkkiDK/backend-challenge-go/internal/worker"
)

const (
	// dependencyRetryInterval spaces startup probes of unavailable dependencies.
	dependencyRetryInterval = time.Second
	// closeMargin is left after the components' shutdown deadline to close
	// connections.
	closeMargin = 5 * time.Second
	// maxWorkerBackoff caps the wait of a worker after repeated failures.
	maxWorkerBackoff = 30 * time.Second
	// backlogInterval is how often the outbox lag metrics are refreshed.
	backlogInterval = 5 * time.Second
)

// Options composes the service for cfg.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.StartTimeout(cfg.StartTimeout),
		fx.StopTimeout(cfg.ShutdownTimeout+closeMargin),
		fx.WithLogger(func(logger *slog.Logger) fxevent.Logger {
			fxLogger := &fxevent.SlogLogger{Logger: logger}
			fxLogger.UseLogLevel(slog.LevelDebug)
			return fxLogger
		}),
		observabilityModule,
		databaseModule,
		authModule,
		applicationModule,
		messagingModule,
		workersModule(cfg),
		httpModule,
	)
}

var observabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(os.Stdout, cfg.LogLevel, cfg.InstanceID)
		},
		observability.NewMetrics,
	),
)

var databaseModule = fx.Module("database",
	fx.Provide(newPool, postgres.NewStore),
)

func newPool(lc fx.Lifecycle, cfg config.Config, logger *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), cfg.Database)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return awaitDependency(ctx, logger, "postgres", pool.Ping)
		},
		OnStop: func(context.Context) error {
			pool.Close()
			logger.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

var authModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config, logger *slog.Logger) *auth.Verifier {
		verifier := auth.NewVerifier(cfg.Auth)
		lc.Append(fx.StartHook(func(ctx context.Context) error {
			return awaitDependency(ctx, logger, "identity provider", verifier.CheckKeys)
		}))
		return verifier
	}),
)

var applicationModule = fx.Module("application",
	fx.Provide(func(store *postgres.Store, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) (*app.Service, error) {
		return app.NewService(store, cfg.References.Policy, app.SystemClock, logger, metrics)
	}),
)

var messagingModule = fx.Module("messaging",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config, logger *slog.Logger) (*messaging.Broker, error) {
		client, err := messaging.NewClient(context.Background(), cfg.SQS)
		if err != nil {
			return nil, err
		}
		broker := messaging.NewBroker(client, cfg.SQS)
		lc.Append(fx.StartHook(func(ctx context.Context) error {
			return awaitDependency(ctx, logger, "sqs", broker.Connect)
		}))
		return broker, nil
	}),
)

// workersModule registers the enabled background components: internal
// workers first, the consumer last, so the consumer stops before them.
func workersModule(cfg config.Config) fx.Option {
	var options []fx.Option
	if cfg.References.Enabled {
		options = append(options, fx.Invoke(registerReferenceWorker))
	}
	if cfg.Outbox.Enabled {
		options = append(options, fx.Invoke(registerOutboxRelay))
	}
	if cfg.Consumer.Enabled {
		options = append(options, fx.Invoke(registerConsumer))
	}
	return fx.Module("workers", options...)
}

func registerReferenceWorker(lc fx.Lifecycle, service *app.Service, cfg config.Config, logger *slog.Logger) {
	appendLoop(lc, worker.NewLoop("pending-references", service.ResolveNextPendingReference,
		cfg.References.PollInterval, maxWorkerBackoff, logger))
}

func registerOutboxRelay(lc fx.Lifecycle, store *postgres.Store, broker *messaging.Broker, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) error {
	relay, err := outbox.NewRelay(store.Outbox(), broker, cfg.Outbox.Relay, app.SystemClock, logger, metrics)
	if err != nil {
		return err
	}
	appendLoop(lc, worker.NewLoop("outbox-relay", relay.PublishBatch, cfg.Outbox.PollInterval, maxWorkerBackoff, logger))
	appendLoop(lc, worker.NewLoop("outbox-backlog", func(ctx context.Context) (bool, error) {
		return false, relay.RecordBacklog(ctx)
	}, backlogInterval, maxWorkerBackoff, logger))
	return nil
}

func registerConsumer(lc fx.Lifecycle, broker *messaging.Broker, service *app.Service, cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) {
	consumer := messaging.NewConsumer(broker, service, cfg.Consumer, app.SystemClock, logger, metrics)
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { return consumer.Start() },
		OnStop:  consumer.Stop,
	})
}

func appendLoop(lc fx.Lifecycle, loop *worker.Loop) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			loop.Start()
			return nil
		},
		OnStop: loop.Stop,
	})
}

var httpModule = fx.Module("http",
	fx.Provide(newReadiness, newHTTPServer),
	fx.Invoke(func(lc fx.Lifecycle, server *httpapi.Server) {
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error { return server.Start() },
			OnStop:  server.Stop,
		})
	}),
)

func newReadiness(store *postgres.Store, broker *messaging.Broker) *httpapi.Readiness {
	return httpapi.NewReadiness(
		httpapi.Check{Name: "postgres", Probe: store.Ping},
		httpapi.Check{Name: "sqs", Probe: broker.Check},
	)
}

func newHTTPServer(cfg config.Config, service *app.Service, verifier *auth.Verifier, readiness *httpapi.Readiness, logger *slog.Logger, metrics *observability.Metrics) *httpapi.Server {
	handler := httpapi.NewHandler(httpapi.Dependencies{
		Service:        service,
		Verifier:       verifier,
		Readiness:      readiness,
		Logger:         logger,
		Metrics:        metrics,
		RequestTimeout: cfg.HTTP.RequestTimeout,
	})
	return httpapi.NewServer(cfg.HTTP.Addr, handler, cfg.HTTP.Timeouts, readiness, logger)
}

// awaitDependency retries probe until it succeeds or the start timeout
// carried by ctx expires, so the service starts once its dependencies are
// up and fails fast when they never come up.
func awaitDependency(ctx context.Context, logger *slog.Logger, name string, probe func(context.Context) error) error {
	for {
		err := probe(ctx)
		if err == nil {
			logger.Info("dependency ready", "dependency", name)
			return nil
		}
		logger.Warn("waiting for dependency", "dependency", name, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready before the start timeout: %w", name, err)
		case <-time.After(dependencyRetryInterval):
		}
	}
}
