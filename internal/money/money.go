// Package money implements Money, an immutable value object pairing an exact
// amount with its ISO 4217 currency.
//
// Amounts are int64 counts of minor units at the fixed Scale of two decimal
// places, so the representable range is -92233720368547758.08 to
// 92233720368547758.07. Nothing goes through floating point: parsing works on
// the decimal digits, arithmetic is integer arithmetic that reports overflow
// instead of wrapping, and serialization formats the digits back.
package money

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale is the number of decimal places of every amount.
const Scale = 2

// minorUnitsPerMajor is 10^Scale.
const minorUnitsPerMajor = 100

var (
	// ErrInvalidAmount reports text that is not a canonical decimal amount.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrOverflow reports a result outside the int64 range of minor units.
	ErrOverflow = errors.New("money: amount out of range")
	// ErrCurrencyMismatch reports arithmetic or comparison across currencies.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrUninitialized reports use of the zero Money value.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// Money is an exact amount of a currency. The zero value is invalid: every
// operation on it fails with ErrUninitialized.
type Money struct {
	minorUnits int64
	currency   Currency
}

// New returns minorUnits hundredths of currency.
func New(minorUnits int64, currency Currency) (Money, error) {
	if currency.IsZero() {
		return Money{}, ErrUninitialized
	}
	return Money{minorUnits: minorUnits, currency: currency}, nil
}

// Zero returns the zero amount of currency.
func Zero(currency Currency) (Money, error) {
	return New(0, currency)
}

// Parse parses amount, which must be in the canonical form
// -?(0|[1-9][0-9]*)\.[0-9]{2}, for the ISO 4217 currencyCode.
//
// Every accepted amount has exactly one spelling: empty strings, NaN,
// Infinity, exponents, '+' signs, whitespace, separators other than '.',
// missing or extra fraction digits, leading zeros and negative zero are all
// rejected instead of being normalized or rounded.
func Parse(amount, currencyCode string) (Money, error) {
	currency, err := ParseCurrency(currencyCode)
	if err != nil {
		return Money{}, err
	}
	minorUnits, err := parseMinorUnits(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minorUnits: minorUnits, currency: currency}, nil
}

func parseMinorUnits(amount string) (int64, error) {
	unsigned, negative := strings.CutPrefix(amount, "-")
	whole, fraction, hasPoint := strings.Cut(unsigned, ".")
	switch {
	case !hasPoint || len(fraction) != Scale || !isDigits(whole) || !isDigits(fraction):
		return 0, fmt.Errorf("%w: %q is not a decimal with exactly %d fraction digits", ErrInvalidAmount, amount, Scale)
	case len(whole) > 1 && whole[0] == '0':
		return 0, fmt.Errorf("%w: %q has a leading zero", ErrInvalidAmount, amount)
	}

	digits := whole + fraction
	if negative {
		digits = "-" + digits
	}
	minorUnits, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		// The digits were validated above, so range is the only possible failure.
		return 0, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	if negative && minorUnits == 0 {
		return 0, fmt.Errorf("%w: negative zero %q", ErrInvalidAmount, amount)
	}
	return minorUnits, nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// IsValid reports whether m was built by a constructor, i.e. is not the zero value.
func (m Money) IsValid() bool {
	return !m.currency.IsZero()
}

// Currency returns the currency of m.
func (m Money) Currency() Currency {
	return m.currency
}

// MinorUnits returns the amount in hundredths of the currency.
func (m Money) MinorUnits() int64 {
	return m.minorUnits
}

// IsZero reports whether the amount is zero.
func (m Money) IsZero() bool {
	return m.minorUnits == 0
}

// IsPositive reports whether the amount is greater than zero.
func (m Money) IsPositive() bool {
	return m.minorUnits > 0
}

// IsNegative reports whether the amount is less than zero.
func (m Money) IsNegative() bool {
	return m.minorUnits < 0
}

// Add returns m + other.
func (m Money) Add(other Money) (Money, error) {
	if err := m.checkCompatible(other); err != nil {
		return Money{}, err
	}
	if (other.minorUnits > 0 && m.minorUnits > math.MaxInt64-other.minorUnits) ||
		(other.minorUnits < 0 && m.minorUnits < math.MinInt64-other.minorUnits) {
		return Money{}, fmt.Errorf("%w: %s + %s", ErrOverflow, m.Amount(), other.Amount())
	}
	return Money{minorUnits: m.minorUnits + other.minorUnits, currency: m.currency}, nil
}

// Sub returns m - other.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.checkCompatible(other); err != nil {
		return Money{}, err
	}
	if (other.minorUnits < 0 && m.minorUnits > math.MaxInt64+other.minorUnits) ||
		(other.minorUnits > 0 && m.minorUnits < math.MinInt64+other.minorUnits) {
		return Money{}, fmt.Errorf("%w: %s - %s", ErrOverflow, m.Amount(), other.Amount())
	}
	return Money{minorUnits: m.minorUnits - other.minorUnits, currency: m.currency}, nil
}

// Negate returns -m.
func (m Money) Negate() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	if m.minorUnits == math.MinInt64 {
		return Money{}, fmt.Errorf("%w: -(%s)", ErrOverflow, m.Amount())
	}
	return Money{minorUnits: -m.minorUnits, currency: m.currency}, nil
}

// Compare returns -1, 0 or +1 as m is less than, equal to or greater than other.
func (m Money) Compare(other Money) (int, error) {
	if err := m.checkCompatible(other); err != nil {
		return 0, err
	}
	return cmp.Compare(m.minorUnits, other.minorUnits), nil
}

// Equal reports whether m and other have the same currency and amount.
func (m Money) Equal(other Money) bool {
	return m.currency == other.currency && m.minorUnits == other.minorUnits
}

func (m Money) checkCompatible(other Money) error {
	if !m.IsValid() || !other.IsValid() {
		return ErrUninitialized
	}
	if m.currency != other.currency {
		return fmt.Errorf("%w: %s and %s", ErrCurrencyMismatch, m.currency, other.currency)
	}
	return nil
}

// Amount returns the canonical decimal text of the amount, e.g. "25.00" or "-0.50".
func (m Money) Amount() string {
	// Negating in uint64 yields the magnitude even for math.MinInt64.
	magnitude, sign := uint64(m.minorUnits), ""
	if m.minorUnits < 0 {
		magnitude, sign = -magnitude, "-"
	}
	return fmt.Sprintf("%s%d.%02d", sign, magnitude/minorUnitsPerMajor, magnitude%minorUnitsPerMajor)
}

// String formats m as "25.00 BRL".
func (m Money) String() string {
	if !m.IsValid() {
		return "<invalid money>"
	}
	return m.Amount() + " " + m.currency.code
}

// MarshalJSON encodes m as {"amount":"25.00","currency":"BRL"}. The amount is a
// JSON string so consumers never have to parse it as a binary float.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: m.Amount(), Currency: m.currency.code})
}
