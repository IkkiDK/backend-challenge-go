package domain

import (
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

// Event is a fact recorded by an aggregate. Each concrete type fixes its own
// type name and schema version, so neither can be chosen by the publisher.
type Event interface {
	EventID() uuid.UUID
	EventType() string
	EventVersion() int
	AggregateType() string
	AggregateID() string
	// OrderingKey groups the events that must be delivered in order: all
	// events of one wallet share it.
	OrderingKey() string
	OccurredAt() time.Time
}

// Aggregate types named in event envelopes.
const (
	AggregateWallet           = "Wallet"
	AggregateWagerTransaction = "WagerTransaction"
)

// eventHeader carries the identity and time every event gets at construction.
// It has no exported fields, so it adds nothing to an event's JSON data.
type eventHeader struct {
	id         uuid.UUID
	occurredAt time.Time
}

func newEventHeader(occurredAt time.Time) eventHeader {
	return eventHeader{id: newUUID(), occurredAt: occurredAt}
}

// EventID returns the stable identity of the event, kept across republication.
func (h eventHeader) EventID() uuid.UUID { return h.id }

// OccurredAt returns when the recorded fact happened.
func (h eventHeader) OccurredAt() time.Time { return h.occurredAt }

// providerFields identifies the provider operation behind an event; every
// field is omitted for the internal OPENING transaction.
type providerFields struct {
	ProviderID            ProviderID            `json:"providerId,omitzero"`
	ExternalTransactionID ExternalTransactionID `json:"externalTransactionId,omitzero"`
	RoundID               RoundID               `json:"roundId,omitzero"`
	GameID                GameID                `json:"gameId,omitzero"`
}

func providerFieldsOf(t *WagerTransaction) providerFields {
	if t.external == nil {
		return providerFields{}
	}
	return providerFields{
		ProviderID:            t.external.providerID,
		ExternalTransactionID: t.external.externalID,
		RoundID:               t.external.roundID,
		GameID:                t.external.gameID,
	}
}

// WagerTransactionProcessed reports a successfully completed operation,
// including LOSS (which moves no money) and the internal OPENING credit.
type WagerTransactionProcessed struct {
	eventHeader
	TransactionID TransactionID `json:"transactionId"`
	WalletID      WalletID      `json:"walletId"`
	PlayerID      PlayerID      `json:"playerId"`
	Kind          Kind          `json:"kind"`
	Money         money.Money   `json:"money"`
	// Balance is the wallet balance right after the operation.
	Balance                money.Money   `json:"balance"`
	ReferenceTransactionID TransactionID `json:"referenceTransactionId,omitzero"`
	providerFields
}

func newWagerTransactionProcessed(t *WagerTransaction) WagerTransactionProcessed {
	return WagerTransactionProcessed{
		eventHeader:            newEventHeader(t.updatedAt),
		TransactionID:          t.id,
		WalletID:               t.walletID,
		PlayerID:               t.playerID,
		Kind:                   t.kind,
		Money:                  t.money,
		Balance:                t.resultBalance,
		ReferenceTransactionID: t.referenceID,
		providerFields:         providerFieldsOf(t),
	}
}

// EventType names the event.
func (WagerTransactionProcessed) EventType() string { return "WagerTransactionProcessed" }

// EventVersion is the schema version of the event data.
func (WagerTransactionProcessed) EventVersion() int { return 1 }

// AggregateType names the aggregate the event belongs to.
func (WagerTransactionProcessed) AggregateType() string { return AggregateWagerTransaction }

// AggregateID identifies the transaction.
func (e WagerTransactionProcessed) AggregateID() string { return e.TransactionID.String() }

// OrderingKey is the wallet of the transaction.
func (e WagerTransactionProcessed) OrderingKey() string { return e.WalletID.String() }

// WagerTransactionRejected reports a definitive refusal by a business rule.
type WagerTransactionRejected struct {
	eventHeader
	TransactionID                  TransactionID         `json:"transactionId"`
	WalletID                       WalletID              `json:"walletId"`
	PlayerID                       PlayerID              `json:"playerId"`
	Kind                           Kind                  `json:"kind"`
	Money                          money.Money           `json:"money"`
	FailureCode                    FailureCode           `json:"failureCode"`
	FailureClass                   FailureClass          `json:"failureClass"`
	Balance                        money.Money           `json:"balance"`
	ReferenceExternalTransactionID ExternalTransactionID `json:"referenceExternalTransactionId,omitzero"`
	providerFields
}

func newWagerTransactionRejected(t *WagerTransaction) WagerTransactionRejected {
	return WagerTransactionRejected{
		eventHeader:                    newEventHeader(t.updatedAt),
		TransactionID:                  t.id,
		WalletID:                       t.walletID,
		PlayerID:                       t.playerID,
		Kind:                           t.kind,
		Money:                          t.money,
		FailureCode:                    t.failureCode,
		FailureClass:                   t.failureCode.Class(),
		Balance:                        t.resultBalance,
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(),
		providerFields:                 providerFieldsOf(t),
	}
}

// EventType names the event.
func (WagerTransactionRejected) EventType() string { return "WagerTransactionRejected" }

// EventVersion is the schema version of the event data.
func (WagerTransactionRejected) EventVersion() int { return 1 }

// AggregateType names the aggregate the event belongs to.
func (WagerTransactionRejected) AggregateType() string { return AggregateWagerTransaction }

// AggregateID identifies the transaction.
func (e WagerTransactionRejected) AggregateID() string { return e.TransactionID.String() }

// OrderingKey is the wallet of the transaction.
func (e WagerTransactionRejected) OrderingKey() string { return e.WalletID.String() }

// WagerTransactionPendingReference reports that an operation waits for the
// transaction it references.
type WagerTransactionPendingReference struct {
	eventHeader
	TransactionID                  TransactionID         `json:"transactionId"`
	WalletID                       WalletID              `json:"walletId"`
	PlayerID                       PlayerID              `json:"playerId"`
	Kind                           Kind                  `json:"kind"`
	Money                          money.Money           `json:"money"`
	ReferenceExternalTransactionID ExternalTransactionID `json:"referenceExternalTransactionId"`
	NextAttemptAt                  time.Time             `json:"nextAttemptAt"`
	providerFields
}

func newWagerTransactionPendingReference(t *WagerTransaction) WagerTransactionPendingReference {
	return WagerTransactionPendingReference{
		eventHeader:                    newEventHeader(t.updatedAt),
		TransactionID:                  t.id,
		WalletID:                       t.walletID,
		PlayerID:                       t.playerID,
		Kind:                           t.kind,
		Money:                          t.money,
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(),
		NextAttemptAt:                  t.nextAttemptAt.UTC(),
		providerFields:                 providerFieldsOf(t),
	}
}

// EventType names the event.
func (WagerTransactionPendingReference) EventType() string {
	return "WagerTransactionPendingReference"
}

// EventVersion is the schema version of the event data.
func (WagerTransactionPendingReference) EventVersion() int { return 1 }

// AggregateType names the aggregate the event belongs to.
func (WagerTransactionPendingReference) AggregateType() string { return AggregateWagerTransaction }

// AggregateID identifies the transaction.
func (e WagerTransactionPendingReference) AggregateID() string { return e.TransactionID.String() }

// OrderingKey is the wallet of the transaction.
func (e WagerTransactionPendingReference) OrderingKey() string { return e.WalletID.String() }

// WalletBalanceChanged reports an effective change of a wallet balance.
type WalletBalanceChanged struct {
	eventHeader
	WalletID      WalletID      `json:"walletId"`
	TransactionID TransactionID `json:"transactionId"`
	Direction     Direction     `json:"direction"`
	Money         money.Money   `json:"money"`
	BalanceBefore money.Money   `json:"balanceBefore"`
	BalanceAfter  money.Money   `json:"balanceAfter"`
	WalletVersion int64         `json:"walletVersion"`
}

func newWalletBalanceChanged(entry LedgerEntry) WalletBalanceChanged {
	return WalletBalanceChanged{
		eventHeader:   newEventHeader(entry.createdAt),
		WalletID:      entry.walletID,
		TransactionID: entry.transactionID,
		Direction:     entry.direction,
		Money:         entry.amount,
		BalanceBefore: entry.balanceBefore,
		BalanceAfter:  entry.balanceAfter,
		WalletVersion: entry.walletVersion,
	}
}

// EventType names the event.
func (WalletBalanceChanged) EventType() string { return "WalletBalanceChanged" }

// EventVersion is the schema version of the event data.
func (WalletBalanceChanged) EventVersion() int { return 1 }

// AggregateType names the aggregate the event belongs to.
func (WalletBalanceChanged) AggregateType() string { return AggregateWallet }

// AggregateID identifies the wallet.
func (e WalletBalanceChanged) AggregateID() string { return e.WalletID.String() }

// OrderingKey is the wallet itself.
func (e WalletBalanceChanged) OrderingKey() string { return e.WalletID.String() }

// Envelope is the published, immutable form of an event.
type Envelope struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateType string    `json:"aggregateType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          Event     `json:"data"`
}

// NewEnvelope wraps e with its tracing metadata. Type and version always come
// from the event itself.
func NewEnvelope(e Event, correlationID, causationID string) Envelope {
	return Envelope{
		EventID:       e.EventID(),
		EventType:     e.EventType(),
		AggregateType: e.AggregateType(),
		AggregateID:   e.AggregateID(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    e.OccurredAt().UTC(),
		Version:       e.EventVersion(),
		Data:          e,
	}
}
