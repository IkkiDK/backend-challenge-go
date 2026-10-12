package money

import (
	"errors"
	"fmt"
)

// ErrUnsupportedCurrency reports a code that is not one of the supported ISO 4217
// currencies.
var ErrUnsupportedCurrency = errors.New("money: unsupported currency")

// supportedCurrencies lists the ISO 4217 codes whose minor unit is two decimal
// places: amounts use a fixed scale, so currencies with zero or three decimals
// (JPY, KWD, ...) cannot be represented and are rejected.
var supportedCurrencies = map[string]bool{
	"BRL": true,
	"EUR": true,
	"GBP": true,
	"USD": true,
}

// Currency is a supported ISO 4217 currency. The zero value is invalid.
type Currency struct {
	code string
}

// ParseCurrency returns the currency of an upper-case ISO 4217 code. Lower-case
// or padded codes are rejected rather than normalized.
func ParseCurrency(code string) (Currency, error) {
	if !supportedCurrencies[code] {
		return Currency{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, code)
	}
	return Currency{code: code}, nil
}

// Code returns the ISO 4217 code, or "" for the zero value.
func (c Currency) Code() string {
	return c.code
}

// IsZero reports whether c is the uninitialized zero value.
func (c Currency) IsZero() bool {
	return c.code == ""
}

func (c Currency) String() string {
	return c.code
}
