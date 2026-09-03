package gateway

import (
	"context"
	"net/http"
	"strings"
)

const userIDKey contextKey = "user_id"

// Auth resolves the caller identity from the Authorization header and
// puts the user_id on the request context: Bearer <token> is looked up
// in the configured token table (AUTH_TOKENS; kind ships it as a
// Secret). The identity is established by the gateway or not at all —
// a request without a valid token gets 401 before reaching any
// handler. X-User-Id no longer exists: the ownership check downstream
// (payment) can trust the user_id inside the command because it was
// resolved here, not claimed by the caller.
//
// The lookup is a plain map get: token strings are low-entropy dev
// secrets, and a constant-time table scan is out of scope — real
// credential hardening (sessions, hashed keys, an identity provider)
// joins when the project outgrows the lab.
func Auth(tokens map[string]string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, ok := resolveIdentity(r.Header.Get("Authorization"), tokens)
			if !ok {
				w.Header().Set("WWW-Authenticate", `Bearer realm="pulsar-gateway"`)
				writeError(w, http.StatusUnauthorized, "a valid bearer token is required")
				return
			}
			ctx := context.WithValue(r.Context(), userIDKey, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// resolveIdentity extracts the bearer token and looks it up. The
// scheme is case-insensitive (RFC 7235); the token is matched exactly.
func resolveIdentity(header string, tokens map[string]string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	userID, ok := tokens[strings.TrimSpace(token)]
	return userID, ok && userID != ""
}

// UserIDFrom extracts the identity resolved by the Auth middleware.
func UserIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}
