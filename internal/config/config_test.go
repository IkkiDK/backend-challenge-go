package config

import (
	"strings"
	"testing"
	"time"
)

func env(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

var requiredEnv = map[string]string{
	"DATABASE_URL":  "postgres://wallet_app:secret@localhost:5432/wallet",
	"OIDC_ISSUER":   "http://localhost:8180/realms/wallet",
	"OIDC_JWKS_URL": "http://localhost:8180/realms/wallet/protocol/openid-connect/certs",
	"INSTANCE_ID":   "wallet-1",
}

func TestLoadAppliesDefaults(t *testing.T) {
	cfg, err := Load(env(requiredEnv))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":8080" || cfg.Auth.Audience != "wallet-api" || cfg.SQS.InboundQueue != "wager-transactions.fifo" ||
		!cfg.Consumer.Enabled || !cfg.Outbox.Enabled || !cfg.References.Enabled || cfg.Outbox.Relay.Owner != "wallet-1" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.References.Policy.MaxAttempts != 12 || cfg.References.Policy.BaseDelay != time.Second {
		t.Fatalf("unexpected reference policy: %+v", cfg.References.Policy)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		"INSTANCE_ID":                "wallet-1",
		"CONSUMER_CONCURRENCY":       "0",
		"OUTBOX_LEASE":               "1s",
		"OUTBOX_PUBLISH_TIMEOUT":     "5s",
		"REFERENCE_MAX_ATTEMPTS":     "zero",
		"HTTP_REQUEST_TIMEOUT":       "soon",
		"CONSUMER_RETRY_BASE_DELAY":  "10s",
		"CONSUMER_RETRY_MAX_DELAY":   "1s",
		"LOG_LEVEL":                  "loud",
		"REFERENCE_WORKER_ENABLED":   "maybe",
		"REFERENCE_RETRY_BASE_DELAY": "-1s",
	}))
	if err == nil {
		t.Fatal("invalid configuration must fail")
	}
	for _, want := range []string{
		"DATABASE_URL is required", "OIDC_ISSUER is required", "OIDC_JWKS_URL is required",
		"CONSUMER_CONCURRENCY", "outbox lease", "REFERENCE_MAX_ATTEMPTS", "HTTP_REQUEST_TIMEOUT",
		"CONSUMER_RETRY_BASE_DELAY", "LOG_LEVEL", "REFERENCE_WORKER_ENABLED", "reference retry policy",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n%v", want, err)
		}
	}
}
