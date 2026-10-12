// Package auth authenticates callers with OAuth 2.0 access tokens issued by
// an external OpenID Connect provider (Keycloak) and models what they may do.
//
// Tokens are JWTs validated locally against the provider's published signing
// keys (JWKS): signature, issuer, audience and expiry. Keys are cached and
// refetched when an unknown key id appears, so key rotation needs no restart.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/IkkiDK/backend-challenge-go/internal/domain"
)

// Scopes granted to clients through their access tokens.
const (
	// ScopeWageringWrite submits operations for the token's provider.
	ScopeWageringWrite = "wagering:write"
	// ScopeWageringRead reads the transactions of the token's provider.
	ScopeWageringRead = "wagering:read"
	// ScopeWageringReadAll reads the transactions of every provider.
	ScopeWageringReadAll = "wagering:read-all"
	// ScopeWalletsWrite opens wallets.
	ScopeWalletsWrite = "wallets:write"
	// ScopeWalletsRead reads wallets and ledgers.
	ScopeWalletsRead = "wallets:read"
	// ScopeWalletsReconcile reconciles wallet balances.
	ScopeWalletsReconcile = "wallets:reconcile"
)

// fetchTimeout bounds every request to the identity provider.
const fetchTimeout = 5 * time.Second

// ErrInvalidToken reports a missing, malformed, expired or untrusted token.
var ErrInvalidToken = errors.New("invalid access token")

// Config locates the identity provider.
type Config struct {
	// Issuer must equal the iss claim of every accepted token.
	Issuer string
	// JWKSURL serves the provider's signing keys. It may use an internal
	// address that differs from the public issuer URL.
	JWKSURL string
	// Audience must appear in the aud claim, so tokens minted for other
	// services are refused.
	Audience string
}

// Principal is an authenticated caller.
type Principal struct {
	Subject  string
	ClientID string
	// ProviderID is the game provider the client acts for; zero for
	// internal services. It comes from the token, never from the request.
	ProviderID domain.ProviderID
	scopes     map[string]bool
}

// HasScope reports whether the token granted scope.
func (p Principal) HasScope(scope string) bool {
	return p.scopes[scope]
}

// Verifier validates access tokens.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
	jwksURL  string
	client   *http.Client
}

// NewVerifier returns a verifier that fetches signing keys lazily.
func NewVerifier(cfg Config) *Verifier {
	client := &http.Client{Timeout: fetchTimeout}
	// The key set keeps this context for every future key fetch, so it must
	// outlive any request.
	keys := oidc.NewRemoteKeySet(oidc.ClientContext(context.Background(), client), cfg.JWKSURL)
	return &Verifier{
		verifier: oidc.NewVerifier(cfg.Issuer, keys, &oidc.Config{
			ClientID:             cfg.Audience,
			SupportedSigningAlgs: []string{oidc.RS256},
		}),
		jwksURL: cfg.JWKSURL,
		client:  client,
	}
}

// CheckKeys fetches the signing keys once, to verify at startup that the
// identity provider is reachable.
func (v *Verifier) CheckKeys(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch signing keys: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch signing keys: %s", resp.Status)
	}
	return nil
}

// Verify authenticates a raw bearer token.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	token, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	var claims struct {
		Scope           string `json:"scope"`
		ProviderID      string `json:"provider_id"`
		AuthorizedParty string `json:"azp"`
	}
	if err := token.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: claims: %w", ErrInvalidToken, err)
	}
	principal := Principal{
		Subject:  token.Subject,
		ClientID: claims.AuthorizedParty,
		scopes:   make(map[string]bool),
	}
	for _, scope := range strings.Fields(claims.Scope) {
		principal.scopes[scope] = true
	}
	if claims.ProviderID != "" {
		if principal.ProviderID, err = domain.ParseProviderID(claims.ProviderID); err != nil {
			return Principal{}, fmt.Errorf("%w: provider_id claim: %w", ErrInvalidToken, err)
		}
	}
	return principal, nil
}
