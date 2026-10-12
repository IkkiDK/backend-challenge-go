package bootstrap

import (
	"strconv"
	"strings"
	"testing"

	"go.uber.org/fx"

	"github.com/IkkiDK/backend-challenge-go/internal/config"
)

func TestDependencyGraphIsComplete(t *testing.T) {
	components := []struct {
		variable string
		enabled  func(config.Config) bool
	}{
		{"CONSUMER_ENABLED", func(c config.Config) bool { return c.Consumer.Enabled }},
		{"OUTBOX_ENABLED", func(c config.Config) bool { return c.Outbox.Enabled }},
		{"REFERENCE_WORKER_ENABLED", func(c config.Config) bool { return c.References.Enabled }},
	}
	// Bit i of combination switches component i on: every combination once.
	for combination := range 1 << len(components) {
		env := map[string]string{
			"INSTANCE_ID":   "validate",
			"DATABASE_URL":  "postgres://wallet_app:secret@localhost:5432/wallet",
			"OIDC_ISSUER":   "http://localhost:8180/realms/wallet",
			"OIDC_JWKS_URL": "http://localhost:8180/realms/wallet/protocol/openid-connect/certs",
		}
		enabled := make([]bool, len(components))
		settings := make([]string, len(components))
		for i, c := range components {
			enabled[i] = combination&(1<<i) != 0
			env[c.variable] = strconv.FormatBool(enabled[i])
			settings[i] = c.variable + "=" + env[c.variable]
		}
		t.Run(strings.Join(settings, ","), func(t *testing.T) {
			cfg, err := config.Load(func(key string) string { return env[key] })
			if err != nil {
				t.Fatal(err)
			}
			for i, c := range components {
				if c.enabled(cfg) != enabled[i] {
					t.Fatalf("%s=%t was not applied", c.variable, enabled[i])
				}
			}
			if err := fx.ValidateApp(Options(cfg)); err != nil {
				t.Fatalf("invalid dependency graph: %v", err)
			}
		})
	}
}
