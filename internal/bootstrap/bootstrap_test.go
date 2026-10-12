package bootstrap

import (
	"testing"

	"go.uber.org/fx"

	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

func TestDependencyGraphIsComplete(t *testing.T) {
	for name, disabled := range map[string]map[string]string{
		"every component":   {},
		"api only":          {"CONSUMER_ENABLED": "false", "OUTBOX_ENABLED": "false", "REFERENCE_WORKER_ENABLED": "false"},
		"without consumer":  {"CONSUMER_ENABLED": "false"},
		"without the relay": {"OUTBOX_ENABLED": "false"},
	} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{
				"INSTANCE_ID":   "validate",
				"DATABASE_URL":  "postgres://wallet_app:secret@localhost:5432/wallet",
				"OIDC_ISSUER":   "http://localhost:8180/realms/wallet",
				"OIDC_JWKS_URL": "http://localhost:8180/realms/wallet/protocol/openid-connect/certs",
			}
			for key, value := range disabled {
				env[key] = value
			}
			cfg, err := config.Load(func(key string) string { return env[key] })
			if err != nil {
				t.Fatal(err)
			}
			if err := fx.ValidateApp(Options(cfg)); err != nil {
				t.Fatalf("invalid dependency graph: %v", err)
			}
		})
	}
}
