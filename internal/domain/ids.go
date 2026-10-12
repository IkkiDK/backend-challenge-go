package domain

import (
	"fmt"

	"github.com/google/uuid"
)

const (
	// canonicalUUIDLength is the length of the 8-4-4-4-12 text form of a UUID.
	canonicalUUIDLength    = 36
	maxProviderIDLength    = 64
	maxExternalIDLength    = 128
	maxIdempotencyKeyBytes = 255
)

// newUUID returns a time-ordered UUIDv7. It cannot fail in practice: since Go
// 1.24 crypto/rand never returns an error.
func newUUID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// parseUUID accepts only the canonical 36-character form (any case) of a
// non-nil UUID, so String always returns the lower-case form of the input.
func parseUUID(field, value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.Nil, problem(field, ProblemRequired, "is required")
	}
	id, err := uuid.Parse(value)
	if len(value) != canonicalUUIDLength || err != nil || id == uuid.Nil {
		return uuid.Nil, problem(field, ProblemInvalidFormat, "must be a non-nil UUID in 8-4-4-4-12 form")
	}
	return id, nil
}

// parseToken accepts 1..maxLength visible ASCII characters (0x21-0x7E).
func parseToken(field, value string, maxLength int) (string, error) {
	if value == "" {
		return "", problem(field, ProblemRequired, "is required")
	}
	if len(value) > maxLength {
		return "", problem(field, ProblemTooLong, "must have at most %d characters", maxLength)
	}
	for i := range len(value) {
		if value[i] < '!' || value[i] > '~' {
			return "", problem(field, ProblemInvalidFormat, "must contain only visible ASCII characters")
		}
	}
	return value, nil
}

// WalletID identifies a wallet. The zero value is invalid.
type WalletID struct{ value uuid.UUID }

// NewWalletID returns a new time-ordered wallet identifier.
func NewWalletID() WalletID { return WalletID{value: newUUID()} }

// ParseWalletID parses the canonical text form of a wallet identifier.
func ParseWalletID(s string) (WalletID, error) {
	id, err := parseUUID("walletId", s)
	return WalletID{value: id}, err
}

func (id WalletID) String() string { return id.value.String() }

// IsZero reports whether id is the invalid zero value.
func (id WalletID) IsZero() bool { return id.value == uuid.Nil }

// MarshalText encodes the canonical text form.
func (id WalletID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// TransactionID identifies a wagering transaction. The zero value is invalid.
type TransactionID struct{ value uuid.UUID }

// NewTransactionID returns a new time-ordered transaction identifier.
func NewTransactionID() TransactionID { return TransactionID{value: newUUID()} }

// ParseTransactionID parses the canonical text form of a transaction identifier.
func ParseTransactionID(s string) (TransactionID, error) {
	id, err := parseUUID("transactionId", s)
	return TransactionID{value: id}, err
}

func (id TransactionID) String() string { return id.value.String() }

// IsZero reports whether id is the invalid zero value.
func (id TransactionID) IsZero() bool { return id.value == uuid.Nil }

// MarshalText encodes the canonical text form.
func (id TransactionID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// PlayerID identifies a player. The zero value is invalid.
type PlayerID struct{ value uuid.UUID }

// ParsePlayerID parses the canonical text form of a player identifier.
func ParsePlayerID(s string) (PlayerID, error) {
	id, err := parseUUID("playerId", s)
	return PlayerID{value: id}, err
}

func (id PlayerID) String() string { return id.value.String() }

// IsZero reports whether id is the invalid zero value.
func (id PlayerID) IsZero() bool { return id.value == uuid.Nil }

// MarshalText encodes the canonical text form.
func (id PlayerID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// ProviderID identifies a game provider, e.g. "provider-a". The zero value is invalid.
type ProviderID struct{ value string }

// ParseProviderID accepts 1..64 characters of [A-Za-z0-9._-] starting with a letter or digit.
func ParseProviderID(s string) (ProviderID, error) {
	const field = "providerId"
	if s == "" {
		return ProviderID{}, problem(field, ProblemRequired, "is required")
	}
	if len(s) > maxProviderIDLength {
		return ProviderID{}, problem(field, ProblemTooLong, "must have at most %d characters", maxProviderIDLength)
	}
	for i := range len(s) {
		c := s[i]
		alphanumeric := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alphanumeric && (i == 0 || c != '.' && c != '_' && c != '-') {
			return ProviderID{}, problem(field, ProblemInvalidFormat, "must match [A-Za-z0-9][A-Za-z0-9._-]*")
		}
	}
	return ProviderID{value: s}, nil
}

func (id ProviderID) String() string { return id.value }

// IsZero reports whether id is the invalid zero value.
func (id ProviderID) IsZero() bool { return id.value == "" }

// MarshalText encodes the identifier.
func (id ProviderID) MarshalText() ([]byte, error) { return []byte(id.value), nil }

// ExternalTransactionID is a provider's own identifier of an operation. The zero value is invalid.
type ExternalTransactionID struct{ value string }

// ParseExternalTransactionID accepts 1..128 visible ASCII characters.
func ParseExternalTransactionID(s string) (ExternalTransactionID, error) {
	return parseExternalTransactionID("externalTransactionId", s)
}

func parseExternalTransactionID(field, s string) (ExternalTransactionID, error) {
	value, err := parseToken(field, s, maxExternalIDLength)
	return ExternalTransactionID{value: value}, err
}

func (id ExternalTransactionID) String() string { return id.value }

// IsZero reports whether id is the invalid zero value.
func (id ExternalTransactionID) IsZero() bool { return id.value == "" }

// MarshalText encodes the identifier.
func (id ExternalTransactionID) MarshalText() ([]byte, error) { return []byte(id.value), nil }

// RoundID identifies a game round within a provider. The zero value is invalid.
type RoundID struct{ value string }

// ParseRoundID accepts 1..128 visible ASCII characters.
func ParseRoundID(s string) (RoundID, error) {
	value, err := parseToken("roundId", s, maxExternalIDLength)
	return RoundID{value: value}, err
}

func (id RoundID) String() string { return id.value }

// IsZero reports whether id is the invalid zero value.
func (id RoundID) IsZero() bool { return id.value == "" }

// MarshalText encodes the identifier.
func (id RoundID) MarshalText() ([]byte, error) { return []byte(id.value), nil }

// GameID identifies a game. The zero value is invalid.
type GameID struct{ value string }

// ParseGameID accepts 1..128 visible ASCII characters.
func ParseGameID(s string) (GameID, error) {
	value, err := parseToken("gameId", s, maxExternalIDLength)
	return GameID{value: value}, err
}

func (id GameID) String() string { return id.value }

// IsZero reports whether id is the invalid zero value.
func (id GameID) IsZero() bool { return id.value == "" }

// MarshalText encodes the identifier.
func (id GameID) MarshalText() ([]byte, error) { return []byte(id.value), nil }

// IdempotencyKey is the client-chosen key that makes a request retry-safe. Keys
// are scoped to their provider. The zero value is invalid.
type IdempotencyKey struct{ value string }

// ParseIdempotencyKey accepts 1..255 visible ASCII characters, used verbatim:
// the server never substitutes a key of its own.
func ParseIdempotencyKey(s string) (IdempotencyKey, error) {
	value, err := parseToken("idempotencyKey", s, maxIdempotencyKeyBytes)
	return IdempotencyKey{value: value}, err
}

func (k IdempotencyKey) String() string { return k.value }

// IsZero reports whether k is the invalid zero value.
func (k IdempotencyKey) IsZero() bool { return k.value == "" }

// requiredValue pairs a value that has an invalid zero form with its name.
type requiredValue struct {
	name  string
	value interface{ IsZero() bool }
}

// requireSet returns ErrInvalidValue naming the first zero value.
func requireSet(values ...requiredValue) error {
	for _, v := range values {
		if v.value.IsZero() {
			return fmt.Errorf("%w: %s is required", ErrInvalidValue, v.name)
		}
	}
	return nil
}
