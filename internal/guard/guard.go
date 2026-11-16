// Package guard implements the mandatory browser-request defense of
// docs/design.md §15.2: a closed Host allowlist (DNS rebinding), Origin and
// Sec-Fetch-Site checks, a CSRF proof on every state change, exact-origin CORS,
// and an optional bearer deployment token for non-browser clients. Binding to
// loopback alone is not a caller boundary; this is.
package guard

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// CSRFHeader must be present (value "1") on every POST/PUT/PATCH/DELETE. A
// cross-site page cannot set a custom header without a CORS preflight, and the
// preflight is refused for any origin outside the allowlist.
const CSRFHeader = "X-Lakehouse-CSRF"

// Config configures the guard. Hosts are exact `host:port` values compared
// case-insensitively; Origins are exact scheme://host[:port] values.
type Config struct {
	AllowedHosts   []string
	AllowedOrigins []string
	// Token, when non-empty, must be presented as `Authorization: Bearer`. It
	// is for CLI/scripts only; the dashboard never holds it.
	Token string
	// TokenExempt lists exact paths that skip the token (liveness only). They
	// are still subject to the Host check.
	TokenExempt []string
}

// DefaultHosts returns the loopback host:port values for the given listen
// port plus the dashboard and in-network names.
func DefaultHosts(apiPort string) []string {
	hosts := []string{"localhost:5173", "127.0.0.1:5173", "api:8080"}
	for _, h := range []string{"localhost", "127.0.0.1"} {
		hosts = append(hosts, h+":"+apiPort)
	}
	return hosts
}

// DefaultOrigins returns the dashboard and API origins for loopback.
func DefaultOrigins(apiPort string) []string {
	origins := []string{"http://localhost:5173", "http://127.0.0.1:5173"}
	for _, h := range []string{"localhost", "127.0.0.1"} {
		origins = append(origins, "http://"+h+":"+apiPort)
	}
	return origins
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func reject(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="lakehouse"`)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(b)
}

func set(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, i := range items {
		if i = strings.TrimSpace(i); i != "" {
			m[strings.ToLower(i)] = true
		}
	}
	return m
}

func isMutation(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// Middleware wraps next with the guard. A rejected request is never passed on
// and no handler runs for it.
func Middleware(cfg Config, next http.Handler) http.Handler {
	hosts := set(cfg.AllowedHosts)
	origins := set(cfg.AllowedOrigins)
	exempt := set(cfg.TokenExempt)
	token := []byte(cfg.Token)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Host: a rebinding attacker's domain is not on the list.
		if !hosts[strings.ToLower(r.Host)] {
			reject(w, http.StatusForbidden, "forbidden", "request host is not allowed")
			return
		}

		// 2. Origin, when supplied. Absence is not an error.
		origin := r.Header.Get("Origin")
		if origin != "" && !origins[strings.ToLower(origin)] {
			reject(w, http.StatusForbidden, "forbidden", "cross-origin request refused")
			return
		}

		// 3. Sec-Fetch-Site, when supplied.
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			reject(w, http.StatusForbidden, "forbidden", "cross-site request refused")
			return
		}

		// CORS: only the exact dashboard origin, never a wildcard.
		if origin != "" {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			// Preflight from an allowlisted origin (a foreign one was refused above).
			if origin == "" {
				reject(w, http.StatusForbidden, "forbidden", "preflight without an origin refused")
				return
			}
			h := w.Header()
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type, "+CSRFHeader+", Authorization")
			h.Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// 4. Optional deployment token.
		if len(token) > 0 && !exempt[strings.ToLower(r.URL.Path)] {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") ||
				subtle.ConstantTimeCompare([]byte(got), token) != 1 {
				reject(w, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
				return
			}
		}

		// 5. CSRF proof on every state change, regardless of Origin.
		if isMutation(r.Method) && r.Header.Get(CSRFHeader) != "1" {
			reject(w, http.StatusForbidden, "forbidden", "missing "+CSRFHeader+" header")
			return
		}

		next.ServeHTTP(w, r)
	})
}
