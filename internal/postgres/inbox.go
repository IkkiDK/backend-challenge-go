package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

type inbox struct {
	q querier
}

func (r inbox) Find(ctx context.Context, consumer, messageID string) (*app.InboxRecord, error) {
	var (
		record                  = app.InboxRecord{Consumer: consumer, MessageID: messageID}
		transactionID           string
		receivedAt, completedAt time.Time
	)
	err := r.q.QueryRow(ctx, `
		SELECT payload_hash, transaction_id, received_at, completed_at
		FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&record.Hash, &transactionID, &receivedAt, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	if record.TransactionID, err = domain.ParseTransactionID(transactionID); err != nil {
		return nil, fmt.Errorf("stored inbox transaction id: %w", err)
	}
	record.ReceivedAt, record.CompletedAt = receivedAt.UTC(), completedAt.UTC()
	return &record, nil
}

func (r inbox) Insert(ctx context.Context, record app.InboxRecord) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, transaction_id, received_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		record.Consumer, record.MessageID, record.Hash, record.TransactionID.String(), record.ReceivedAt, record.CompletedAt)
	return dbError(err)
}
