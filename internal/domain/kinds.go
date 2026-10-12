package domain

import "fmt"

// Kind is the type of a wagering transaction.
type Kind string

// Transaction kinds. OPENING is internal; the other five are external operations.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// allowedReferenceKinds maps each kind that may reference another transaction
// to the kinds it may reference: a WIN may name the BET it settles, a REFUND
// returns a BET, and a ROLLBACK undoes a BET, WIN or REFUND.
var allowedReferenceKinds = map[Kind]map[Kind]bool{
	KindWin:      {KindBet: true},
	KindRefund:   {KindBet: true},
	KindRollback: {KindBet: true, KindWin: true, KindRefund: true},
}

// ParseKind returns the kind named s, including the internal OPENING kind.
func ParseKind(s string) (Kind, error) {
	switch kind := Kind(s); kind {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return kind, nil
	default:
		return "", fmt.Errorf("%w: unknown transaction kind %q", ErrInvalidValue, s)
	}
}

// parseExternalKind accepts only the kinds providers may submit.
func parseExternalKind(s string) (Kind, error) {
	const field = "kind"
	switch kind := Kind(s); kind {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return kind, nil
	case KindOpening:
		return "", problem(field, ProblemUnsupportedKind, "OPENING is reserved for internal wallet opening")
	case "":
		return "", problem(field, ProblemRequired, "is required")
	default:
		return "", problem(field, ProblemUnsupportedKind, "must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
	}
}

// IsReversal reports whether the kind undoes another transaction.
func (k Kind) IsReversal() bool {
	return k == KindRefund || k == KindRollback
}

func (k Kind) requiresReference() bool {
	return k.IsReversal()
}

func (k Kind) acceptsReference() bool {
	_, ok := allowedReferenceKinds[k]
	return ok
}

// Status is the processing state of a wagering transaction.
type Status string

// Transaction states. PENDING exists only in memory: a transaction is always
// persisted in one of the other states (see ARCHITECTURE.md).
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus returns the status named s.
func ParseStatus(s string) (Status, error) {
	switch status := Status(s); status {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return status, nil
	default:
		return "", fmt.Errorf("%w: unknown transaction status %q", ErrInvalidValue, s)
	}
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Direction is the side of a ledger entry.
type Direction string

// Ledger entry directions.
const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// ParseDirection returns the direction named s.
func ParseDirection(s string) (Direction, error) {
	switch direction := Direction(s); direction {
	case DirectionDebit, DirectionCredit:
		return direction, nil
	default:
		return "", fmt.Errorf("%w: unknown ledger direction %q", ErrInvalidValue, s)
	}
}
