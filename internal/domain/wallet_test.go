package domain

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/IkkiDK/backend-challenge-go/internal/money"
)

func openTestWallet(t *testing.T, initial money.Money) (WalletOpening, error) {
	t.Helper()
	walletID, err := ParseWalletID(testWalletID)
	if err != nil {
		t.Fatal(err)
	}
	playerID, err := ParsePlayerID(testPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	return OpenWallet(OpenWalletParams{
		ID:                   walletID,
		PlayerID:             playerID,
		InitialBalance:       initial,
		OpeningTransactionID: NewTransactionID(),
		CorrelationID:        "corr-open",
		Now:                  testNow,
	})
}

func TestOpenWalletWithPositiveBalanceCreditsAnOpeningTransaction(t *testing.T) {
	opening, err := openTestWallet(t, mustMoney(t, "1000.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	w, tx, entry := opening.Wallet, opening.Opening, opening.Entry

	if w.Version() != InitialWalletVersion || w.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet = version %d balance %s, want version 1 balance 1000.00", w.Version(), w.Balance())
	}
	if tx == nil || tx.Kind() != KindOpening || tx.Status() != StatusProcessed || tx.ResultBalance().Amount() != "1000.00" {
		t.Fatalf("opening transaction = %+v, want a PROCESSED OPENING with balance 1000.00", tx)
	}
	snapshot := tx.Snapshot()
	if !snapshot.ProviderID.IsZero() || !snapshot.ExternalTransactionID.IsZero() || !snapshot.IdempotencyKey.IsZero() ||
		snapshot.PayloadHash != "" || !snapshot.RoundID.IsZero() || !snapshot.GameID.IsZero() {
		t.Fatalf("OPENING must carry no provider metadata: %+v", snapshot)
	}
	if entry == nil || entry.Direction() != DirectionCredit || entry.WalletVersion() != InitialWalletVersion ||
		!entry.BalanceBefore().IsZero() || entry.BalanceAfter().Amount() != "1000.00" || entry.TransactionID() != tx.ID() {
		t.Fatalf("opening entry = %+v, want a 0.00 → 1000.00 credit at version 1", entry)
	}

	walletEvents, txEvents := w.PullEvents(), tx.PullEvents()
	if got := eventTypes(walletEvents); !slices.Equal(got, []string{"WalletBalanceChanged"}) {
		t.Fatalf("wallet events = %v", got)
	}
	if got := eventTypes(txEvents); !slices.Equal(got, []string{"WagerTransactionProcessed"}) {
		t.Fatalf("transaction events = %v", got)
	}
	changed := walletEvents[0].(WalletBalanceChanged)
	if changed.WalletVersion != InitialWalletVersion || changed.BalanceAfter.Amount() != "1000.00" {
		t.Fatalf("WalletBalanceChanged = %+v", changed)
	}
	data, err := json.Marshal(NewEnvelope(txEvents[0], "corr-open", ""))
	if err != nil {
		t.Fatal(err)
	}
	for _, inapplicable := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceTransactionId", "causationId"} {
		if strings.Contains(string(data), inapplicable) {
			t.Errorf("internal OPENING event must omit %s: %s", inapplicable, data)
		}
	}
}

func TestOpenWalletWithZeroBalanceCreatesNoFinancialRecords(t *testing.T) {
	opening, err := openTestWallet(t, mustMoney(t, "0.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if opening.Opening != nil || opening.Entry != nil {
		t.Fatal("a zero opening must not create an OPENING transaction or ledger entry")
	}
	if events := opening.Wallet.PullEvents(); len(events) != 0 {
		t.Fatalf("a zero opening emitted %v", eventTypes(events))
	}
	if opening.Wallet.Version() != InitialWalletVersion || !opening.Wallet.Balance().IsZero() {
		t.Fatal("a zero opening must start at version 1 with balance 0.00")
	}
}

func TestOpenWalletRejectsInvalidInput(t *testing.T) {
	if _, err := openTestWallet(t, mustMoney(t, "-1.00", "BRL")); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("negative initial balance error = %v, want ErrInvalidValue", err)
	}
	if _, err := openTestWallet(t, money.Money{}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("uninitialized initial balance error = %v, want ErrInvalidValue", err)
	}
	if _, err := OpenWallet(OpenWalletParams{InitialBalance: mustMoney(t, "1.00", "BRL"), Now: testNow}); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("missing ids error = %v, want ErrInvalidValue", err)
	}
}

func TestDebitAndCreditKeepTheLedgerInvariant(t *testing.T) {
	w := mustWallet(t, "100.00")
	txID := NewTransactionID()

	debit, err := w.Debit(txID, mustMoney(t, "30.00", "BRL"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 {
		t.Fatalf("after debit: balance %s version %d, want 70.00 version 2", w.Balance(), w.Version())
	}
	if debit.Direction() != DirectionDebit || debit.BalanceBefore().Amount() != "100.00" ||
		debit.BalanceAfter().Amount() != "70.00" || debit.WalletVersion() != 2 {
		t.Fatalf("debit entry = %+v", debit)
	}

	credit, err := w.Credit(NewTransactionID(), mustMoney(t, "5.50", "BRL"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "75.50" || w.Version() != 3 || credit.WalletVersion() != 3 {
		t.Fatalf("after credit: balance %s version %d", w.Balance(), w.Version())
	}
	if got := eventTypes(w.PullEvents()); !slices.Equal(got, []string{"WalletBalanceChanged", "WalletBalanceChanged"}) {
		t.Fatalf("events = %v", got)
	}

	if _, err := w.Debit(NewTransactionID(), mustMoney(t, "75.50", "BRL"), testNow); err != nil || !w.Balance().IsZero() {
		t.Fatalf("debiting the exact balance: %v, balance %s", err, w.Balance())
	}
}

func TestDebitNeverMakesTheBalanceNegative(t *testing.T) {
	w := mustWallet(t, "100.00")
	_, err := w.Debit(NewTransactionID(), mustMoney(t, "100.01", "BRL"), testNow)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("error = %v, want ErrInsufficientFunds", err)
	}
	if w.Balance().Amount() != "100.00" || w.Version() != InitialWalletVersion || len(w.PullEvents()) != 0 {
		t.Fatal("a refused debit must leave balance, version and events untouched")
	}
}

func TestMovementsRejectInvalidAmountsAndCurrencies(t *testing.T) {
	w := mustWallet(t, "100.00")
	tests := map[string]struct {
		amount money.Money
		want   error
	}{
		"zero":           {mustMoney(t, "0.00", "BRL"), ErrInvalidValue},
		"negative":       {mustMoney(t, "-1.00", "BRL"), ErrInvalidValue},
		"uninitialized":  {money.Money{}, ErrInvalidValue},
		"other currency": {mustMoney(t, "1.00", "USD"), ErrCurrencyMismatch},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := w.Credit(NewTransactionID(), tt.amount, testNow); !errors.Is(err, tt.want) {
				t.Fatalf("Credit error = %v, want %v", err, tt.want)
			}
			if _, err := w.Debit(NewTransactionID(), tt.amount, testNow); !errors.Is(err, tt.want) {
				t.Fatalf("Debit error = %v, want %v", err, tt.want)
			}
		})
	}
	if w.Balance().Amount() != "100.00" || w.Version() != InitialWalletVersion {
		t.Fatal("refused movements must not change the wallet")
	}
}

func TestCreditReportsBalanceOverflow(t *testing.T) {
	brl, err := money.ParseCurrency("BRL")
	if err != nil {
		t.Fatal(err)
	}
	huge, err := money.New(math.MaxInt64, brl)
	if err != nil {
		t.Fatal(err)
	}
	source := mustWallet(t, "0.00")
	w, err := RehydrateWallet(WalletSnapshot{
		ID: source.ID(), PlayerID: source.PlayerID(), Balance: huge, Version: 7, CreatedAt: testNow, UpdatedAt: testNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Credit(NewTransactionID(), mustMoney(t, "0.01", "BRL"), testNow); !errors.Is(err, ErrBalanceLimitExceeded) {
		t.Fatalf("error = %v, want ErrBalanceLimitExceeded", err)
	}
}

func TestRehydrateWalletValidatesAndEmitsNothing(t *testing.T) {
	source := mustWallet(t, "10.00")
	valid := WalletSnapshot{
		ID: source.ID(), PlayerID: source.PlayerID(), Balance: mustMoney(t, "10.00", "BRL"),
		Version: 5, CreatedAt: testNow, UpdatedAt: testNow,
	}
	w, err := RehydrateWallet(valid)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 5 || len(w.PullEvents()) != 0 {
		t.Fatal("rehydration must restore the version and emit no events")
	}

	invalid := map[string]func(*WalletSnapshot){
		"negative balance":        func(s *WalletSnapshot) { s.Balance = mustMoney(t, "-0.01", "BRL") },
		"uninitialized money":     func(s *WalletSnapshot) { s.Balance = money.Money{} },
		"version zero":            func(s *WalletSnapshot) { s.Version = 0 },
		"missing id":              func(s *WalletSnapshot) { s.ID = WalletID{} },
		"updated before creation": func(s *WalletSnapshot) { s.UpdatedAt = testNow.Add(-1) },
	}
	for name, edit := range invalid {
		t.Run(name, func(t *testing.T) {
			s := valid
			edit(&s)
			if _, err := RehydrateWallet(s); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestMovementsNeverDateTheWalletBack(t *testing.T) {
	w := mustWallet(t, "100.00")
	behind, ahead := testNow.Add(-5*time.Millisecond), testNow.Add(time.Second)

	if _, err := w.Debit(NewTransactionID(), mustMoney(t, "1.00", "BRL"), behind); err != nil {
		t.Fatal(err)
	}
	if !w.UpdatedAt().Equal(testNow) {
		t.Fatalf("an instance whose clock is behind dated the wallet %v, before its creation at %v", w.UpdatedAt(), testNow)
	}
	if _, err := w.Credit(NewTransactionID(), mustMoney(t, "1.00", "BRL"), ahead); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Debit(NewTransactionID(), mustMoney(t, "1.00", "BRL"), testNow); err != nil {
		t.Fatal(err)
	}
	if !w.UpdatedAt().Equal(ahead) {
		t.Fatalf("updatedAt moved back to %v after a movement at %v", w.UpdatedAt(), ahead)
	}
}
