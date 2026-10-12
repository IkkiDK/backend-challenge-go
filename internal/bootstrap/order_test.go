package bootstrap

import (
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

// moduleRecorder records the module of every constructor and invocation Fx
// runs, in order. Components register their lifecycle hooks while those run,
// and Fx starts the hooks in the same order.
type moduleRecorder struct{ modules []string }

func (r *moduleRecorder) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.Run:
		r.modules = append(r.modules, e.ModuleName)
	case *fxevent.Invoked:
		r.modules = append(r.modules, e.ModuleName)
	}
}

func TestDependencyChecksStartBeforeTheWorkers(t *testing.T) {
	env := map[string]string{
		"INSTANCE_ID":   "order",
		"DATABASE_URL":  "postgres://wallet_app:secret@localhost:5432/wallet",
		"OIDC_ISSUER":   "http://localhost:8180/realms/wallet",
		"OIDC_JWKS_URL": "http://localhost:8180/realms/wallet/protocol/openid-connect/certs",
	}
	cfg, err := config.Load(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	recorder := &moduleRecorder{}
	var pool *pgxpool.Pool
	app := fx.New(Options(cfg), fx.WithLogger(func() fxevent.Logger { return recorder }), fx.Populate(&pool))
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	firstWorker := slices.Index(recorder.modules, "workers")
	if firstWorker < 0 {
		t.Fatalf("no worker registered: %v", recorder.modules)
	}
	for _, dependency := range []string{"database", "auth", "messaging"} {
		if at := slices.Index(recorder.modules, dependency); at < 0 || at > firstWorker {
			t.Errorf("%s registers its startup check at %d, after the first worker at %d: %v",
				dependency, at, firstWorker, recorder.modules)
		}
	}
}
