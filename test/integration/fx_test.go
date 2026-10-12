//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx/fxtest"
	"go.uber.org/goleak"

	"github.com/IkkiDK/backend-challenge-go/internal/bootstrap"
	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

func TestFxApplicationStartsServesAndStopsWithoutLeakingGoroutines(t *testing.T) {
	// Idle keep-alive connections of HTTP clients may outlive the test; every
	// goroutine the service itself started must be gone after Stop.
	defer goleak.VerifyNone(t,
		goleak.IgnoreCurrent(),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)

	port := freePort(t)
	variables := serviceEnvironment("fx-lifecycle", port)
	variables["LOG_LEVEL"] = "error"
	// The AWS SDK reads credentials from the process environment.
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE"} {
		t.Setenv(name, variables[name])
	}
	cfg, err := config.Load(func(name string) string { return variables[name] })
	if err != nil {
		t.Fatal(err)
	}

	app := fxtest.New(t, bootstrap.Options(cfg))
	app.RequireStart()
	baseURL := "http://" + cfg.HTTP.Addr
	if s := call(t, http.MethodGet, baseURL+"/health/ready", "", nil, nil).status; s != http.StatusOK {
		t.Fatalf("readiness after start: %d", s)
	}
	w := openWallet(t, baseURL, "100.00")
	if status, _ := submit(t, baseURL, accessToken(t, "provider-a"), newOperation(w, "BET", "fx-bet-"+w.ID, "10.00")); status != http.StatusCreated {
		t.Fatalf("bet through the composed application: %d", status)
	}
	eventually(t, 10*time.Second, "the relay to publish the bet's events", func() bool {
		return countRows(t, `SELECT count(*) FROM outbox_events WHERE ordering_key = $1 AND published_at IS NULL`, w.ID) == 0
	})

	app.RequireStop()
	if _, err := http.Get(baseURL + "/health/live"); err == nil {
		t.Fatal("the HTTP server must not accept requests after Stop")
	}
	http.DefaultClient.CloseIdleConnections()
}
