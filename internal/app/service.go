package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

// Clock returns the current time.
type Clock func() time.Time

// SystemClock returns the current UTC time truncated to the microsecond
// precision PostgreSQL stores, so stored and published times are identical.
func SystemClock() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// Service implements the wallet use cases.
type Service struct {
	store   Store
	policy  domain.ReferenceRetryPolicy
	clock   Clock
	logger  *slog.Logger
	metrics *observability.Metrics
}

// NewService validates the reference retry policy and returns the service.
func NewService(store Store, policy domain.ReferenceRetryPolicy, clock Clock, logger *slog.Logger, metrics *observability.Metrics) (*Service, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &Service{store: store, policy: policy, clock: clock, logger: logger, metrics: metrics}, nil
}

// OpenWallet creates a wallet. A positive initial balance is credited by an
// internal OPENING transaction; the wallet, the transaction, its ledger entry
// and their events commit together.
func (s *Service) OpenWallet(ctx context.Context, req domain.WalletOpeningRequest, correlationID string) (*domain.Wallet, error) {
	var opened *domain.Wallet
	err := s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		opening, err := domain.OpenWallet(domain.OpenWalletParams{
			ID:                   domain.NewWalletID(),
			PlayerID:             req.PlayerID(),
			InitialBalance:       req.InitialBalance(),
			OpeningTransactionID: domain.NewTransactionID(),
			CorrelationID:        correlationID,
			Now:                  s.clock(),
		})
		if err != nil {
			return err
		}
		if err := tx.Wallets().Insert(ctx, opening.Wallet); err != nil {
			return err
		}
		events := opening.Wallet.PullEvents()
		if opening.Opening != nil {
			if err := tx.Transactions().Insert(ctx, opening.Opening); err != nil {
				return err
			}
			if err := tx.Ledger().Append(ctx, *opening.Entry); err != nil {
				return err
			}
			events = append(events, opening.Opening.PullEvents()...)
		}
		if err := appendEvents(ctx, tx, events, correlationID, ""); err != nil {
			return err
		}
		opened = opening.Wallet
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.logger.InfoContext(ctx, "wallet opened", observability.KeyWalletID, opened.ID().String())
	return opened, nil
}

// Wallet returns a wallet.
func (s *Service) Wallet(ctx context.Context, id domain.WalletID) (*domain.Wallet, error) {
	return s.store.Reader().Wallet(ctx, id)
}

// LedgerPage is one page of a wallet ledger.
type LedgerPage struct {
	Entries []domain.LedgerEntry
	// NextAfterVersion is the cursor of the following page, or 0 on the last page.
	NextAfterVersion int64
}

// Ledger returns up to limit entries of the wallet after the given wallet
// version, in the stable order of the versions they produced.
func (s *Service) Ledger(ctx context.Context, walletID domain.WalletID, afterVersion int64, limit int) (LedgerPage, error) {
	reader := s.store.Reader()
	if _, err := reader.Wallet(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	entries, err := reader.LedgerPage(ctx, walletID, afterVersion, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Entries: entries}
	if len(entries) > limit {
		page.Entries = entries[:limit]
		page.NextAfterVersion = page.Entries[limit-1].WalletVersion()
	}
	return page, nil
}

// ReadScope limits whose transactions a caller may read.
type ReadScope struct {
	provider domain.ProviderID
	all      bool
}

// AllProviders lets internal services read every transaction.
func AllProviders() ReadScope { return ReadScope{all: true} }

// OnlyProvider restricts a provider to its own transactions.
func OnlyProvider(provider domain.ProviderID) ReadScope { return ReadScope{provider: provider} }

func (s ReadScope) allows(t *domain.WagerTransaction) bool {
	return s.all || (!s.provider.IsZero() && t.ProviderID() == s.provider)
}

// Transaction returns a transaction visible in scope. Transactions of other
// providers are reported as not found, so their existence does not leak.
func (s *Service) Transaction(ctx context.Context, id domain.TransactionID, scope ReadScope) (*domain.WagerTransaction, error) {
	t, err := s.store.Reader().Transactions().ByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if !scope.allows(t) {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// TransactionByExternalID returns the provider's transaction with that
// external id. Callers authorize access to the provider beforehand.
func (s *Service) TransactionByExternalID(ctx context.Context, provider domain.ProviderID, externalID domain.ExternalTransactionID) (*domain.WagerTransaction, error) {
	t, err := s.store.Reader().Transactions().ByExternalID(ctx, provider, externalID)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// Reconciliation compares a stored balance with the one rebuilt from the ledger.
type Reconciliation struct {
	WalletID          domain.WalletID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	// Difference is the stored balance minus the calculated one.
	Difference     money.Money
	Consistent     bool
	CheckedEntries int64
}

// Reconcile rebuilds the wallet balance from its ledger (opening included)
// and compares it with the stored balance, both read from one snapshot. It
// never changes the balance; divergences are logged and counted.
func (s *Service) Reconcile(ctx context.Context, walletID domain.WalletID) (Reconciliation, error) {
	totals, err := s.store.Reader().LedgerTotals(ctx, walletID)
	if err != nil {
		return Reconciliation{}, err
	}
	calculated, err := totals.Credits.Sub(totals.Debits)
	if err != nil {
		return Reconciliation{}, fmt.Errorf("rebuild balance of wallet %s: %w", walletID, err)
	}
	difference, err := totals.StoredBalance.Sub(calculated)
	if err != nil {
		return Reconciliation{}, fmt.Errorf("compare balance of wallet %s: %w", walletID, err)
	}
	r := Reconciliation{
		WalletID:          walletID,
		StoredBalance:     totals.StoredBalance,
		CalculatedBalance: calculated,
		Difference:        difference,
		Consistent:        difference.IsZero(),
		CheckedEntries:    totals.Entries,
	}
	if r.Consistent {
		s.metrics.Reconciliations.WithLabelValues("consistent").Inc()
		return r, nil
	}
	s.metrics.Reconciliations.WithLabelValues("divergent").Inc()
	s.metrics.ReconciliationDivergences.Inc()
	s.logger.ErrorContext(ctx, "wallet balance diverges from its ledger",
		observability.KeyWalletID, walletID.String(),
		"difference", difference.String(),
		"checkedEntries", totals.Entries)
	return r, nil
}

// appendEvents serializes events into outbox records of the current unit of work.
func appendEvents(ctx context.Context, tx Tx, events []domain.Event, correlationID, causationID string) error {
	if len(events) == 0 {
		return nil
	}
	records := make([]OutboxRecord, len(events))
	for i, e := range events {
		payload, err := json.Marshal(domain.NewEnvelope(e, correlationID, causationID))
		if err != nil {
			return fmt.Errorf("serialize %s: %w", e.EventType(), err)
		}
		records[i] = OutboxRecord{
			EventID:       e.EventID(),
			AggregateType: e.AggregateType(),
			AggregateID:   e.AggregateID(),
			OrderingKey:   e.OrderingKey(),
			EventType:     e.EventType(),
			EventVersion:  e.EventVersion(),
			Payload:       payload,
			OccurredAt:    e.OccurredAt(),
		}
	}
	return tx.Outbox().Append(ctx, records)
}
