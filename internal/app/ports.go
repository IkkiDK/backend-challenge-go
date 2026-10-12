// Package app holds the use cases of the wallet service. HTTP handlers and the
// SQS consumer both call them, so both channels share the same validation,
// idempotency and settlement path. It depends on the domain and on the ports
// declared here, never on HTTP, SQS or a database driver.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// Store runs units of work and read-only queries against the database.
type Store interface {
	// InTx runs fn in one READ COMMITTED SQL transaction and commits when fn
	// returns nil. Every repository reached through tx shares that
	// transaction, which is the delimitation of a unit of work. Transactions
	// lost to a deadlock, a serialization failure or ErrStaleVersion are
	// retried from scratch, so fn must rebuild its state on every call.
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Reader answers queries outside of any unit of work.
	Reader() Reader
}

// Tx exposes the repositories bound to one SQL transaction.
type Tx interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerWriter
	Outbox() OutboxWriter
	Inbox() InboxRepository
}

// WalletRepository persists wallets.
type WalletRepository interface {
	// Insert stores a new wallet; ErrWalletAlreadyExists if the player
	// already has a wallet in that currency.
	Insert(ctx context.Context, w *domain.Wallet) error
	// Lock reads the wallet and holds its row lock until the transaction
	// ends, serializing every writer of that wallet (and only that wallet).
	// ErrWalletNotFound when it does not exist.
	Lock(ctx context.Context, id domain.WalletID) (*domain.Wallet, error)
	// UpdateBalance writes the balance and version of w, provided the stored
	// version is still previousVersion; ErrStaleVersion otherwise.
	UpdateBalance(ctx context.Context, w *domain.Wallet, previousVersion int64) error
}

// TransactionReader looks transactions up.
type TransactionReader interface {
	// ByID returns the transaction; ErrTransactionNotFound when absent.
	ByID(ctx context.Context, id domain.TransactionID) (*domain.WagerTransaction, error)
	// ByIdempotencyKey returns the provider's transaction recorded under key,
	// or nil.
	ByIdempotencyKey(ctx context.Context, provider domain.ProviderID, key domain.IdempotencyKey) (*domain.WagerTransaction, error)
	// ByExternalID returns the provider's transaction with that external id,
	// or nil.
	ByExternalID(ctx context.Context, provider domain.ProviderID, id domain.ExternalTransactionID) (*domain.WagerTransaction, error)
	// IsReversed reports whether a PROCESSED REFUND or ROLLBACK targets id.
	IsReversed(ctx context.Context, id domain.TransactionID) (bool, error)
}

// TransactionRepository persists wagering transactions.
type TransactionRepository interface {
	TransactionReader
	// Insert stores a new transaction; ErrDuplicate when its idempotency key
	// or external id was committed concurrently.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	// Update writes the state fields of a transaction that was not terminal.
	Update(ctx context.Context, t *domain.WagerTransaction) error
	// Lock reads the transaction and holds its row lock until the
	// transaction ends; ErrTransactionNotFound when absent.
	Lock(ctx context.Context, id domain.TransactionID) (*domain.WagerTransaction, error)
	// ClaimDuePendingReference locks the most overdue PENDING_REFERENCE
	// transaction not locked by anybody else, or returns nil when none is due.
	ClaimDuePendingReference(ctx context.Context, now time.Time) (*domain.WagerTransaction, error)
}

// LedgerWriter appends ledger entries.
type LedgerWriter interface {
	Append(ctx context.Context, entry domain.LedgerEntry) error
}

// OutboxWriter stores events to be published after commit.
type OutboxWriter interface {
	Append(ctx context.Context, records []OutboxRecord) error
}

// InboxReader finds consumed messages.
type InboxReader interface {
	// Find returns the record of a consumed message, or nil.
	Find(ctx context.Context, consumer, messageID string) (*InboxRecord, error)
}

// InboxRepository records consumed messages.
type InboxRepository interface {
	InboxReader
	// Insert records a consumed message; ErrDuplicate when it was recorded
	// concurrently.
	Insert(ctx context.Context, record InboxRecord) error
}

// Reader answers queries outside of any unit of work.
type Reader interface {
	Wallet(ctx context.Context, id domain.WalletID) (*domain.Wallet, error)
	Transactions() TransactionReader
	Inbox() InboxReader
	// LedgerPage returns up to limit entries of the wallet with a version
	// greater than afterVersion, oldest first.
	LedgerPage(ctx context.Context, walletID domain.WalletID, afterVersion int64, limit int) ([]domain.LedgerEntry, error)
	// LedgerTotals reads the stored balance and the ledger sums of the wallet
	// from one consistent snapshot; ErrWalletNotFound when absent.
	LedgerTotals(ctx context.Context, walletID domain.WalletID) (LedgerTotals, error)
}

// OutboxRecord is an event serialized for the transactional outbox.
type OutboxRecord struct {
	EventID       uuid.UUID
	AggregateType string
	AggregateID   string
	OrderingKey   string
	EventType     string
	EventVersion  int
	Payload       []byte
	OccurredAt    time.Time
}

// InboxRecord is a message consumed by a named consumer.
type InboxRecord struct {
	Consumer      string
	MessageID     string
	Hash          string
	TransactionID domain.TransactionID
	ReceivedAt    time.Time
	CompletedAt   time.Time
}

// LedgerTotals is a consistent view of a wallet balance and of its ledger.
type LedgerTotals struct {
	StoredBalance money.Money
	Credits       money.Money
	Debits        money.Money
	Entries       int64
}
