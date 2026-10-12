package app

import "errors"

var (
	// ErrWalletNotFound reports an unknown wallet.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrWalletAlreadyExists reports a second wallet for the same player and currency.
	ErrWalletAlreadyExists = errors.New("the player already has a wallet in this currency")
	// ErrTransactionNotFound reports an unknown transaction, or one the caller may not read.
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrMessageConflict reports a message id already consumed with different content.
	ErrMessageConflict = errors.New("message id already consumed with different content")

	// ErrDuplicate reports a unique key committed concurrently by another
	// unit of work; the loser re-reads and replays the winner's outcome.
	ErrDuplicate = errors.New("record committed concurrently")
	// ErrStaleVersion reports a wallet updated by another writer since it was
	// read. The row lock makes it impossible in practice; the store retries
	// the unit of work if it ever happens.
	ErrStaleVersion = errors.New("wallet version changed concurrently")
	// ErrUnavailable reports a transient dependency failure (connection,
	// timeout, lock timeout, overload): the same request can be retried.
	ErrUnavailable = errors.New("dependency temporarily unavailable")
	// ErrIntegrity reports data that violates a database invariant: retrying
	// cannot succeed, so the operation is recorded as FAILED for audit.
	ErrIntegrity = errors.New("data integrity violation")
)
