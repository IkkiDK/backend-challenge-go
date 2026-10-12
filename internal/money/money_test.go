package money

import (
	"errors"
	"math"
	"testing"
)

func mustParse(t *testing.T, amount, currency string) Money {
	t.Helper()
	m, err := Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", amount, currency, err)
	}
	return m
}

func TestParseAcceptsCanonicalAmounts(t *testing.T) {
	tests := []struct {
		amount     string
		minorUnits int64
	}{
		{"0.00", 0},
		{"0.01", 1},
		{"0.10", 10},
		{"25.00", 2500},
		{"1000.00", 100000},
		{"-0.50", -50},
		{"-25.00", -2500},
		{"92233720368547758.07", math.MaxInt64},
		{"-92233720368547758.08", math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.amount, func(t *testing.T) {
			m := mustParse(t, tt.amount, "BRL")
			if m.MinorUnits() != tt.minorUnits {
				t.Fatalf("MinorUnits() = %d, want %d", m.MinorUnits(), tt.minorUnits)
			}
			if m.Amount() != tt.amount {
				t.Fatalf("Amount() = %q, want the input %q back", m.Amount(), tt.amount)
			}
			if m.Currency().Code() != "BRL" {
				t.Fatalf("Currency() = %q, want BRL", m.Currency())
			}
		})
	}
}

func TestParseRejectsNonCanonicalAmounts(t *testing.T) {
	invalid := []string{
		"",
		"25",
		"25.0",
		"25.000",
		"25.5000",
		".50",
		"25.",
		"025.00",
		"00.00",
		"-0.00",
		"+1.00",
		" 1.00",
		"1.00 ",
		"1,00",
		"1.000,00",
		"1_000.00",
		"1e2",
		"1.0e2",
		"1E+02",
		"NaN",
		"nan",
		"Infinity",
		"-Infinity",
		"Inf",
		"0x10.00",
		"１.00", // full-width digit one
		"1.00.00",
		"--1.00",
		"-",
		"-.50",
		"1.-5",
	}
	for _, amount := range invalid {
		t.Run(amount, func(t *testing.T) {
			_, err := Parse(amount, "BRL")
			if !errors.Is(err, ErrInvalidAmount) {
				t.Fatalf("Parse(%q) error = %v, want ErrInvalidAmount", amount, err)
			}
		})
	}
}

func TestParseReportsOverflow(t *testing.T) {
	for _, amount := range []string{
		"92233720368547758.08",
		"-92233720368547758.09",
		"99999999999999999999.99",
	} {
		t.Run(amount, func(t *testing.T) {
			_, err := Parse(amount, "BRL")
			if !errors.Is(err, ErrOverflow) {
				t.Fatalf("Parse(%q) error = %v, want ErrOverflow", amount, err)
			}
		})
	}
}

func TestParseRejectsUnsupportedCurrencies(t *testing.T) {
	for _, code := range []string{"", "brl", "Brl", " BRL", "BRLX", "BR", "JPY", "KWD", "XXX"} {
		t.Run(code, func(t *testing.T) {
			_, err := Parse("1.00", code)
			if !errors.Is(err, ErrUnsupportedCurrency) {
				t.Fatalf("Parse with currency %q error = %v, want ErrUnsupportedCurrency", code, err)
			}
		})
	}
}

func TestZeroValueIsRejected(t *testing.T) {
	var uninitialized Money
	brl := mustParse(t, "1.00", "BRL")

	if uninitialized.IsValid() {
		t.Fatal("zero Money must not be valid")
	}
	if _, err := New(100, Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("New with zero currency error = %v, want ErrUninitialized", err)
	}
	if _, err := uninitialized.Add(brl); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Add on zero Money error = %v, want ErrUninitialized", err)
	}
	if _, err := brl.Sub(uninitialized); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Sub of zero Money error = %v, want ErrUninitialized", err)
	}
	if _, err := uninitialized.Negate(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Negate on zero Money error = %v, want ErrUninitialized", err)
	}
	if _, err := uninitialized.Compare(brl); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Compare on zero Money error = %v, want ErrUninitialized", err)
	}
	if _, err := uninitialized.MarshalJSON(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("MarshalJSON on zero Money error = %v, want ErrUninitialized", err)
	}
}

func TestZeroPerCurrency(t *testing.T) {
	usd, err := ParseCurrency("USD")
	if err != nil {
		t.Fatal(err)
	}
	zero, err := Zero(usd)
	if err != nil {
		t.Fatal(err)
	}
	if !zero.IsZero() || zero.IsPositive() || zero.IsNegative() || zero.Amount() != "0.00" || zero.Currency() != usd {
		t.Fatalf("Zero(USD) = %s, want 0.00 USD", zero)
	}
}

func TestArithmetic(t *testing.T) {
	var (
		a = mustParse(t, "25.50", "BRL")
		b = mustParse(t, "100.00", "BRL")
	)
	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "125.50" {
		t.Fatalf("25.50 + 100.00 = %s, %v; want 125.50", sum, err)
	}
	difference, err := a.Sub(b)
	if err != nil || difference.Amount() != "-74.50" || !difference.IsNegative() {
		t.Fatalf("25.50 - 100.00 = %s, %v; want -74.50", difference, err)
	}
	negated, err := difference.Negate()
	if err != nil || negated.Amount() != "74.50" {
		t.Fatalf("-(-74.50) = %s, %v; want 74.50", negated, err)
	}
	if order, err := a.Compare(b); err != nil || order != -1 {
		t.Fatalf("Compare(25.50, 100.00) = %d, %v; want -1", order, err)
	}
	if order, err := b.Compare(a); err != nil || order != 1 {
		t.Fatalf("Compare(100.00, 25.50) = %d, %v; want 1", order, err)
	}
	if order, err := a.Compare(mustParse(t, "25.50", "BRL")); err != nil || order != 0 {
		t.Fatalf("Compare(25.50, 25.50) = %d, %v; want 0", order, err)
	}
	if a.Amount() != "25.50" || b.Amount() != "100.00" {
		t.Fatal("operations must not mutate their operands")
	}
}

func TestArithmeticOverflowBoundaries(t *testing.T) {
	brl, err := ParseCurrency("BRL")
	if err != nil {
		t.Fatal(err)
	}
	money := func(minorUnits int64) Money {
		m, err := New(minorUnits, brl)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	var (
		maxMoney = money(math.MaxInt64)
		minMoney = money(math.MinInt64)
		one      = money(1)
		minusOne = money(-1)
	)

	overflowing := map[string]func() (Money, error){
		"max + 1":  func() (Money, error) { return maxMoney.Add(one) },
		"min + -1": func() (Money, error) { return minMoney.Add(minusOne) },
		"min - 1":  func() (Money, error) { return minMoney.Sub(one) },
		"max - -1": func() (Money, error) { return maxMoney.Sub(minusOne) },
		"-min":     func() (Money, error) { return minMoney.Negate() },
		"0 - min":  func() (Money, error) { return money(0).Sub(minMoney) },
	}
	for name, op := range overflowing {
		t.Run(name, func(t *testing.T) {
			if _, err := op(); !errors.Is(err, ErrOverflow) {
				t.Fatalf("error = %v, want ErrOverflow", err)
			}
		})
	}

	fitting := map[string]struct {
		op   func() (Money, error)
		want int64
	}{
		"max + 0":  {func() (Money, error) { return maxMoney.Add(money(0)) }, math.MaxInt64},
		"max + -1": {func() (Money, error) { return maxMoney.Add(minusOne) }, math.MaxInt64 - 1},
		"min + 1":  {func() (Money, error) { return minMoney.Add(one) }, math.MinInt64 + 1},
		"min - -1": {func() (Money, error) { return minMoney.Sub(minusOne) }, math.MinInt64 + 1},
		"max - 1":  {func() (Money, error) { return maxMoney.Sub(one) }, math.MaxInt64 - 1},
		"-max":     {func() (Money, error) { return maxMoney.Negate() }, -math.MaxInt64},
		"min - 0":  {func() (Money, error) { return minMoney.Sub(money(0)) }, math.MinInt64},
	}
	for name, tt := range fitting {
		t.Run(name, func(t *testing.T) {
			got, err := tt.op()
			if err != nil || got.MinorUnits() != tt.want {
				t.Fatalf("= %d, %v; want %d", got.MinorUnits(), err, tt.want)
			}
		})
	}
}

func TestCurrencyMismatch(t *testing.T) {
	var (
		brl = mustParse(t, "10.00", "BRL")
		usd = mustParse(t, "10.00", "USD")
	)
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Add BRL+USD error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Sub BRL-USD error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Compare BRL/USD error = %v, want ErrCurrencyMismatch", err)
	}
	if brl.Equal(usd) {
		t.Fatal("10.00 BRL must not equal 10.00 USD")
	}
}

func TestAmountFormatsTheFullRange(t *testing.T) {
	brl, err := ParseCurrency("BRL")
	if err != nil {
		t.Fatal(err)
	}
	tests := map[int64]string{
		0:             "0.00",
		5:             "0.05",
		-5:            "-0.05",
		100:           "1.00",
		-123456:       "-1234.56",
		math.MaxInt64: "92233720368547758.07",
		math.MinInt64: "-92233720368547758.08",
	}
	for minorUnits, want := range tests {
		m, err := New(minorUnits, brl)
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Amount(); got != want {
			t.Errorf("Amount(%d) = %q, want %q", minorUnits, got, want)
		}
	}
}

func TestMarshalJSON(t *testing.T) {
	m := mustParse(t, "25.00", "BRL")
	got, err := m.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"amount":"25.00","currency":"BRL"}`; string(got) != want {
		t.Fatalf("MarshalJSON = %s, want %s", got, want)
	}
}
