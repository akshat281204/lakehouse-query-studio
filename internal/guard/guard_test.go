package guard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newHandler(token string) (http.Handler, *int) {
	hits := 0
	h := Middleware(Config{
		AllowedHosts:   DefaultHosts("8080"),
		AllowedOrigins: DefaultOrigins("8080"),
		Token:          token,
		TokenExempt:    []string{"/healthz"},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.WriteHeader(204) }))
	return h, &hits
}

func do(h http.Handler, method, path, host string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(""))
	req.Host = host
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHostAllowlistStopsDNSRebinding(t *testing.T) {
	h, hits := newHandler("")
	for _, host := range []string{"evil.example.com", "evil.example.com:8080", "127.0.0.1", "127.0.0.1:9999", "localhost.evil.com:8080", ""} {
		if rec := do(h, "GET", "/api/v1/datasets", host, nil); rec.Code != 403 {
			t.Errorf("host %q: %d", host, rec.Code)
		}
	}
	if *hits != 0 {
		t.Fatalf("handler ran %d times for rejected requests", *hits)
	}
	for _, host := range []string{"127.0.0.1:8080", "localhost:8080", "LOCALHOST:8080", "localhost:5173", "api:8080"} {
		if rec := do(h, "GET", "/api/v1/datasets", host, nil); rec.Code != 204 {
			t.Errorf("host %q: %d", host, rec.Code)
		}
	}
}

func TestHostIsCheckedEvenForHealthAndReadiness(t *testing.T) {
	h, _ := newHandler("")
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		if rec := do(h, "GET", p, "evil.example.com", nil); rec.Code != 403 {
			t.Errorf("%s: %d", p, rec.Code)
		}
	}
}

func TestOriginAndFetchSite(t *testing.T) {
	h, _ := newHandler("")
	const host = "localhost:8080"
	if rec := do(h, "GET", "/x", host, map[string]string{"Origin": "https://evil.example.com"}); rec.Code != 403 {
		t.Errorf("foreign origin: %d", rec.Code)
	}
	if rec := do(h, "GET", "/x", host, map[string]string{"Origin": "http://localhost:5173"}); rec.Code != 204 {
		t.Errorf("dashboard origin: %d", rec.Code)
	}
	// Same-origin GET without Origin (what a browser sends for polling/paging).
	if rec := do(h, "GET", "/x", host, map[string]string{"Sec-Fetch-Site": "same-origin"}); rec.Code != 204 {
		t.Errorf("same-origin without Origin: %d", rec.Code)
	}
	for _, v := range []string{"same-site", "none"} {
		if rec := do(h, "GET", "/x", host, map[string]string{"Sec-Fetch-Site": v}); rec.Code != 204 {
			t.Errorf("fetch-site %s: %d", v, rec.Code)
		}
	}
	if rec := do(h, "GET", "/x", host, map[string]string{"Sec-Fetch-Site": "cross-site"}); rec.Code != 403 {
		t.Errorf("cross-site: %d", rec.Code)
	}
	if rec := do(h, "GET", "/x", host, nil); rec.Code != 204 {
		t.Errorf("no headers (CLI): %d", rec.Code)
	}
}

func TestCSRFProofRequiredOnEveryMutation(t *testing.T) {
	h, hits := newHandler("")
	const host = "localhost:8080"
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		// Even a same-origin-looking request is refused without the proof,
		// and so is one with no Origin at all (a cross-site form post).
		for _, hdr := range []map[string]string{nil, {"Origin": "http://localhost:5173"}, {"Sec-Fetch-Site": "same-origin"}} {
			if rec := do(h, m, "/api/v1/queries", host, hdr); rec.Code != 403 {
				t.Errorf("%s %v: %d", m, hdr, rec.Code)
			}
		}
		if rec := do(h, m, "/api/v1/queries", host, map[string]string{CSRFHeader: "0"}); rec.Code != 403 {
			t.Errorf("%s wrong value: %d", m, rec.Code)
		}
	}
	if *hits != 0 {
		t.Fatalf("handler ran for refused mutations: %d", *hits)
	}
	if rec := do(h, "POST", "/api/v1/queries", host, map[string]string{CSRFHeader: "1"}); rec.Code != 204 {
		t.Fatalf("with proof: %d", rec.Code)
	}
	// Safe methods need no proof.
	if rec := do(h, "GET", "/api/v1/queries", host, nil); rec.Code != 204 {
		t.Fatalf("GET: %d", rec.Code)
	}
}

func TestCORSIsExactOriginNeverWildcard(t *testing.T) {
	h, hits := newHandler("")
	const host = "localhost:8080"
	rec := do(h, "GET", "/x", host, map[string]string{"Origin": "http://localhost:5173"})
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("ACAO = %q", got)
	}
	rec = do(h, "GET", "/x", host, nil)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("ACAO set without an Origin")
	}
	// Foreign preflight is refused, so the CSRF header can never be sent.
	before := *hits
	rec = do(h, "OPTIONS", "/api/v1/queries", host, map[string]string{
		"Origin": "https://evil.example.com", "Access-Control-Request-Method": "POST",
		"Access-Control-Request-Headers": CSRFHeader})
	if rec.Code != 403 || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("foreign preflight: %d %v", rec.Code, rec.Header())
	}
	rec = do(h, "OPTIONS", "/api/v1/queries", host, map[string]string{
		"Origin": "http://localhost:5173", "Access-Control-Request-Method": "POST"})
	if rec.Code != 204 || !strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), CSRFHeader) ||
		rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Fatalf("dashboard preflight: %d %v", rec.Code, rec.Header())
	}
	if *hits != before {
		t.Fatal("preflights must not reach the handler")
	}
}

func TestBearerToken(t *testing.T) {
	h, _ := newHandler("s3cret-token-value")
	const host = "localhost:8080"
	if rec := do(h, "GET", "/api/v1/datasets", host, nil); rec.Code != 401 || rec.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("missing token: %d", rec.Code)
	}
	for _, auth := range []string{"Bearer wrong", "Bearer ", "s3cret-token-value", "Basic czNjcmV0", "bearer s3cret-token-value"} {
		if rec := do(h, "GET", "/api/v1/datasets", host, map[string]string{"Authorization": auth}); rec.Code != 401 {
			t.Errorf("auth %q: %d", auth, rec.Code)
		}
	}
	if rec := do(h, "GET", "/api/v1/datasets", host, map[string]string{"Authorization": "Bearer s3cret-token-value"}); rec.Code != 204 {
		t.Errorf("valid token: %d", rec.Code)
	}
	// Liveness is exempt from the token so container healthchecks work, but
	// not from the Host check.
	if rec := do(h, "GET", "/healthz", host, nil); rec.Code != 204 {
		t.Errorf("healthz: %d", rec.Code)
	}
	if rec := do(h, "GET", "/healthz", "evil.example.com", nil); rec.Code != 403 {
		t.Errorf("healthz bad host: %d", rec.Code)
	}
	if rec := do(h, "GET", "/readyz", host, nil); rec.Code != 401 {
		t.Errorf("readyz needs the token: %d", rec.Code)
	}
}

func TestRejectionsUseTheErrorEnvelopeAndNeverEchoInput(t *testing.T) {
	h, _ := newHandler("")
	rec := do(h, "GET", "/x", "evil-host-sentinel.example", map[string]string{"Origin": "https://origin-sentinel.example"})
	body := rec.Body.String()
	if rec.Code != 403 || !strings.Contains(body, `"code":"forbidden"`) {
		t.Fatalf("%d %s", rec.Code, body)
	}
	if strings.Contains(body, "sentinel") {
		t.Fatalf("rejection echoes caller input: %s", body)
	}
}
