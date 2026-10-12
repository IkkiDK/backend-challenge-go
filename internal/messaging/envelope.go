package messaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
	"github.com/IkkiDK/backend-challenge-go/internal/observability"
)

const (
	// requestedEventType is the only message type of wager-transactions.fifo.
	requestedEventType = "WagerTransactionRequested"
	// correlationAttribute optionally carries the producer's correlation id.
	correlationAttribute = "correlationId"
)

// errInvalidMessage reports a message that can never be processed.
var errInvalidMessage = errors.New("invalid message")

// inboundEnvelope is the message contract. Unknown envelope fields are
// tolerated (producers may add transport metadata); data is decoded strictly.
type inboundEnvelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

type inboundData struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	Money                          struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
}

// inboundMessage is a decoded, validated wager-transactions message.
type inboundMessage struct {
	messageID     string
	correlationID string
	request       domain.TransactionRequest
	// hash fingerprints the message content for redelivery checks: the
	// request fingerprint (business fields, as for HTTP) plus the
	// idempotency key; transport metadata such as occurredAt is excluded.
	hash string
}

func decodeMessage(body string, attributes map[string]types.MessageAttributeValue) (inboundMessage, error) {
	var envelope inboundEnvelope
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		return inboundMessage{}, fmt.Errorf("%w: envelope: %w", errInvalidMessage, err)
	}
	switch {
	case !observability.IsTraceableID(envelope.MessageID):
		return inboundMessage{}, fmt.Errorf("%w: messageId must have 1 to 128 visible ASCII characters", errInvalidMessage)
	case envelope.Type != requestedEventType:
		return inboundMessage{}, fmt.Errorf("%w: unsupported type %q", errInvalidMessage, envelope.Type)
	}
	if _, err := time.Parse(time.RFC3339, envelope.OccurredAt); err != nil {
		return inboundMessage{}, fmt.Errorf("%w: occurredAt must be an RFC 3339 timestamp", errInvalidMessage)
	}

	var data inboundData
	decoder := json.NewDecoder(bytes.NewReader(envelope.Data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&data); err != nil {
		return inboundMessage{}, fmt.Errorf("%w: data: %w", errInvalidMessage, err)
	}
	request, err := domain.NewTransactionRequest(domain.TransactionRequestInput{
		ProviderID:                     data.ProviderID,
		ExternalTransactionID:          data.ExternalTransactionID,
		IdempotencyKey:                 data.IdempotencyKey,
		PlayerID:                       data.PlayerID,
		WalletID:                       data.WalletID,
		RoundID:                        data.RoundID,
		GameID:                         data.GameID,
		Kind:                           data.Kind,
		Amount:                         data.Money.Amount,
		Currency:                       data.Money.Currency,
		ReferenceExternalTransactionID: data.ReferenceExternalTransactionID,
	})
	if err != nil {
		return inboundMessage{}, fmt.Errorf("%w: %w", errInvalidMessage, err)
	}

	correlationID := envelope.MessageID
	if attribute, ok := attributes[correlationAttribute]; ok && attribute.StringValue != nil && observability.IsTraceableID(*attribute.StringValue) {
		correlationID = *attribute.StringValue
	}
	hash := sha256.Sum256([]byte(envelope.Type + "\n" + request.IdempotencyKey().String() + "\n" + request.Fingerprint()))
	return inboundMessage{
		messageID:     envelope.MessageID,
		correlationID: correlationID,
		request:       request,
		hash:          hex.EncodeToString(hash[:]),
	}, nil
}
