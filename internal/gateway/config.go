// Package gateway implements the HTTP ingress of PulsarPass. It
// resolves the caller identity from bearer tokens, validates requests,
// enforces idempotency headers and publishes commands to the broker.
package gateway

import (
	"fmt"
	"strings"

	"github.com/adamsalves/pulsar-pass/pkg/config"
)

// AuthTokensEnv carries the gateway bearer-token table as CSV pairs of
// token=user_id.
const AuthTokensEnv = "AUTH_TOKENS"

// devAuthTokens is the documented lab mapping, active when AUTH_TOKENS
// is unset — the quickstart, the smoke script and the load test run
// against it. Real deployments set their own table (kind: Secret).
const devAuthTokens = "pp-token-user-1=user-1,pp-token-user-2=user-2"

// Config carries the pulsar-gateway runtime settings.
type Config struct {
	Env         string
	HTTPAddr    string
	HealthAddr  string
	BusMode     string
	NATSURL     string
	RedisAddr   string
	MaxQuantity int
	// AuthTokens resolves bearer tokens to user ids (identity is
	// established by the gateway, never client-supplied).
	AuthTokens map[string]string
}

// ParseAuthTokens parses the AUTH_TOKENS CSV ("token=user_id,...").
// Malformed pairs and duplicate tokens fail loudly — a typo must not
// silently drop an identity from the table — and an empty spec is an
// error for direct callers (a silent total lockout is not an option);
// through LoadConfig an empty env resolves to the dev defaults before
// this parser runs.
func ParseAuthTokens(spec string) (map[string]string, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, fmt.Errorf("invalid %s: at least one token=user_id pair is required", AuthTokensEnv)
	}
	tokens := make(map[string]string)
	for _, pair := range strings.Split(spec, ",") {
		token, user, found := strings.Cut(strings.TrimSpace(pair), "=")
		token, user = strings.TrimSpace(token), strings.TrimSpace(user)
		if !found || token == "" || user == "" {
			return nil, fmt.Errorf("invalid %s pair %q: expected token=user_id", AuthTokensEnv, pair)
		}
		if _, dup := tokens[token]; dup {
			return nil, fmt.Errorf("invalid %s: duplicate token %q", AuthTokensEnv, token)
		}
		tokens[token] = user
	}
	return tokens, nil
}

// LoadConfig reads pulsar-gateway settings from the environment.
// AUTH_TOKENS unset or empty falls back to the dev mapping (the
// codebase-wide config.String convention); a present-but-malformed
// value fails the boot instead of shrinking the table.
func LoadConfig() (Config, error) {
	authTokens, err := ParseAuthTokens(config.String(AuthTokensEnv, devAuthTokens))
	if err != nil {
		return Config{}, err
	}
	return Config{
		Env:         config.String("APP_ENV", "development"),
		HTTPAddr:    config.String("HTTP_ADDR", ":8080"),
		HealthAddr:  config.String("HEALTH_ADDR", ":9091"),
		BusMode:     config.String("BUS_MODE", "nats"),
		NATSURL:     config.String("NATS_URL", "nats://localhost:4222"),
		RedisAddr:   config.String("REDIS_ADDR", "localhost:6379"),
		MaxQuantity: config.Int("MAX_RESERVATION_QTY", 8),
		AuthTokens:  authTokens,
	}, nil
}
