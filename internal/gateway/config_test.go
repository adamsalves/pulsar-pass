package gateway_test

import (
	"reflect"
	"testing"

	"github.com/adamsalves/pulsar-pass/internal/gateway"
)

// TestParseAuthTokens pins the AUTH_TOKENS grammar: whitespace
// tolerance, loud failures for malformed pairs, duplicates and the
// empty spec — a typo must not silently shrink the identity table.
func TestParseAuthTokens(t *testing.T) {
	t.Run("valid pairs with surrounding whitespace", func(t *testing.T) {
		tokens, err := gateway.ParseAuthTokens(" tok-a = user-a , tok-b=user-b ")
		if err != nil {
			t.Fatalf("ParseAuthTokens() error = %v", err)
		}
		want := map[string]string{"tok-a": "user-a", "tok-b": "user-b"}
		if !reflect.DeepEqual(tokens, want) {
			t.Errorf("tokens = %v, want %v", tokens, want)
		}
	})

	t.Run("missing user_id fails", func(t *testing.T) {
		if _, err := gateway.ParseAuthTokens("tok-a=user-a,broken"); err == nil {
			t.Error("ParseAuthTokens() error = nil, want failure for pair without '='")
		}
	})

	t.Run("empty side of a pair fails", func(t *testing.T) {
		for _, spec := range []string{"tok-a=", "=user-a", " =user-a", "tok-a= "} {
			if _, err := gateway.ParseAuthTokens(spec); err == nil {
				t.Errorf("ParseAuthTokens(%q) error = nil, want failure", spec)
			}
		}
	})

	t.Run("duplicate token fails", func(t *testing.T) {
		if _, err := gateway.ParseAuthTokens("tok-a=user-a,tok-a=user-b"); err == nil {
			t.Error("ParseAuthTokens() error = nil, want failure for duplicate token")
		}
	})

	t.Run("empty spec fails instead of locking out silently", func(t *testing.T) {
		if _, err := gateway.ParseAuthTokens(""); err == nil {
			t.Error("ParseAuthTokens() error = nil, want failure for empty spec")
		}
	})
}

// TestLoadConfigDefaultsToDevTokens: AUTH_TOKENS unset or empty falls
// back to the documented dev mapping the quickstart and the smoke run
// use. The empty value is pinned so the test stays deterministic on
// hosts where the variable is exported (e.g. after the load recipe).
func TestLoadConfigDefaultsToDevTokens(t *testing.T) {
	t.Setenv("AUTH_TOKENS", "")
	cfg, err := gateway.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.AuthTokens["pp-token-user-1"] != "user-1" {
		t.Errorf("dev default missing pp-token-user-1: %v", cfg.AuthTokens)
	}
	if cfg.AuthTokens["pp-token-user-2"] != "user-2" {
		t.Errorf("dev default missing pp-token-user-2: %v", cfg.AuthTokens)
	}
}

// TestLoadConfigRejectsMalformedAuthTokens: a present-but-broken
// AUTH_TOKENS fails the boot instead of shrinking the table.
func TestLoadConfigRejectsMalformedAuthTokens(t *testing.T) {
	t.Setenv("AUTH_TOKENS", "tok-a=user-a,broken")
	if _, err := gateway.LoadConfig(); err == nil {
		t.Error("LoadConfig() error = nil, want failure for malformed AUTH_TOKENS")
	}
}
