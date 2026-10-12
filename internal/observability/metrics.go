package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every Prometheus collector of the service, registered in its
// own registry so tests and parallel instances never share global state.
type Metrics struct {
	Registry *prometheus.Registry

	// TransactionResults counts settled operations by channel, kind and status.
	TransactionResults *prometheus.CounterVec
	// IdempotentReplays counts duplicates answered from the stored outcome.
	IdempotentReplays *prometheus.CounterVec
	// IdempotencyConflicts counts keys or operations reused with other content.
	IdempotencyConflicts *prometheus.CounterVec
	// ProcessingDuration measures operation handling latency by channel.
	ProcessingDuration *prometheus.HistogramVec
	// ConcurrencyConflicts counts lost races: deadlocks, serialization
	// failures, stale versions and concurrent duplicate inserts.
	ConcurrencyConflicts *prometheus.CounterVec
	// WalletLockWait measures how long writers wait for a wallet row lock.
	WalletLockWait prometheus.Histogram
	// Retries counts transient-failure retries by component.
	Retries *prometheus.CounterVec
	// SQSMessages counts consumed messages by outcome.
	SQSMessages *prometheus.CounterVec
	// DeadLettered counts messages moved to the dead-letter queue by reason.
	DeadLettered *prometheus.CounterVec
	// OutboxPublished counts events published from the outbox.
	OutboxPublished prometheus.Counter
	// OutboxPublishFailures counts failed publication attempts.
	OutboxPublishFailures prometheus.Counter
	// OutboxLag is the age of the oldest unpublished event.
	OutboxLag prometheus.Gauge
	// OutboxPending is the number of unpublished events.
	OutboxPending prometheus.Gauge
	// PendingReferenceResolutions counts reference worker outcomes.
	PendingReferenceResolutions *prometheus.CounterVec
	// Reconciliations counts reconciliation runs by result.
	Reconciliations *prometheus.CounterVec
	// ReconciliationDivergences counts wallets whose balance diverged from the ledger.
	ReconciliationDivergences prometheus.Counter
	// HTTPRequests counts API requests by route, method and status code.
	HTTPRequests *prometheus.CounterVec
	// HTTPRequestDuration measures API latency by route and method.
	HTTPRequestDuration *prometheus.HistogramVec
}

// NewMetrics creates and registers the service metrics plus Go runtime and
// process collectors.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	m := &Metrics{
		Registry: registry,
		TransactionResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_transaction_results_total",
			Help: "Settled wagering operations by channel, kind and resulting status.",
		}, []string{"channel", "kind", "status"}),
		IdempotentReplays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_idempotent_replays_total",
			Help: "Duplicate requests or deliveries answered with the stored outcome.",
		}, []string{"channel"}),
		IdempotencyConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_idempotency_conflicts_total",
			Help: "Idempotency keys, operations or message ids reused with different content.",
		}, []string{"reason"}),
		ProcessingDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_processing_duration_seconds",
			Help:    "Latency of handling a wagering operation.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		ConcurrencyConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total",
			Help: "Lost concurrency races resolved by retrying or replaying.",
		}, []string{"reason"}),
		WalletLockWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "wallet_lock_wait_seconds",
			Help:    "Time spent acquiring a wallet row lock.",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_retries_total",
			Help: "Retries after transient failures, by component.",
		}, []string{"component"}),
		SQSMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_sqs_messages_total",
			Help: "Consumed SQS messages by outcome.",
		}, []string{"outcome"}),
		DeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_dead_lettered_messages_total",
			Help: "Messages sent to the dead-letter queue by reason.",
		}, []string{"reason"}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_outbox_published_total",
			Help: "Events published from the transactional outbox.",
		}),
		OutboxPublishFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_outbox_publish_failures_total",
			Help: "Failed outbox publication attempts.",
		}),
		OutboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_outbox_lag_seconds",
			Help: "Age of the oldest unpublished outbox event (0 when none).",
		}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "wallet_outbox_pending_events",
			Help: "Number of unpublished outbox events.",
		}),
		PendingReferenceResolutions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_pending_reference_resolutions_total",
			Help: "Reference worker attempts by outcome.",
		}, []string{"outcome"}),
		Reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_reconciliations_total",
			Help: "Wallet reconciliations by result.",
		}, []string{"result"}),
		ReconciliationDivergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total",
			Help: "Reconciliations whose stored balance differed from the ledger.",
		}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_http_requests_total",
			Help: "HTTP requests by route, method and status code.",
		}, []string{"route", "method", "code"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "wallet_http_request_duration_seconds",
			Help:    "HTTP request latency by route and method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
	}
	for _, c := range []prometheus.Collector{
		m.TransactionResults, m.IdempotentReplays, m.IdempotencyConflicts, m.ProcessingDuration,
		m.ConcurrencyConflicts, m.WalletLockWait, m.Retries, m.SQSMessages, m.DeadLettered,
		m.OutboxPublished, m.OutboxPublishFailures, m.OutboxLag, m.OutboxPending,
		m.PendingReferenceResolutions, m.Reconciliations, m.ReconciliationDivergences,
		m.HTTPRequests, m.HTTPRequestDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		registry.MustRegister(c)
	}
	return m
}
