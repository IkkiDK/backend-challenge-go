//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/IkkiDK/backend-challenge-go/internal/app"
	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

// Every instance stamps rows with its own clock, so an instance running behind
// the one that opened a wallet, or a wall clock stepping back during handling,
// must not turn valid operations into failures.
func TestInstancesWhoseClocksDisagreeStillSettle(t *testing.T) {
	const skew = 50 * time.Millisecond
	ahead := newInProcessWithClock(t, testPolicy, func() time.Time { return app.SystemClock().Add(skew) })
	behind := newInProcess(t, testPolicy)
	w := openWalletInProcess(t, ahead, "100.00")

	bet := newOperation(w, "BET", "skew-http-"+w.ID, "25.00")
	result, err := behind.service.Submit(t.Context(), mustRequest(t, bet), httpOrigin())
	if err != nil || result.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("bet handled by an instance %v behind the opener: %v, %+v", skew, err, result.Transaction)
	}

	queued := newOperation(w, "BET", "skew-sqs-"+w.ID, "10.00")
	origin := app.Origin{Channel: app.ChannelSQS, CorrelationID: "test-" + uuid.NewString(), Message: &app.InboundMessage{
		Consumer:   "clock-test",
		MessageID:  uuid.NewString(),
		Hash:       strings.Repeat("0", 64),
		ReceivedAt: app.SystemClock().Add(skew),
	}}
	result, err = behind.service.Submit(t.Context(), mustRequest(t, queued), origin)
	if err != nil || result.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("message whose receipt is dated after the clock reading: %v, %+v", err, result.Transaction)
	}
	requireLedgerMatchesBalance(t, w.ID, 6500)
}
