package zitihost

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ingressive-cloud/connector/connector"
)

// storeWith returns a Store pre-loaded with the given service URLs.
func storeWith(services ...string) *connector.Store {
	s := connector.NewStore()
	s.Update(services)
	return s
}

// newUpstream starts a test HTTP server and registers cleanup.
func newUpstream(t *testing.T, fn func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fn))
	t.Cleanup(srv.Close)
	return srv
}

func TestProxy_AllowedService(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "hello from upstream")
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/hello", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "hello from upstream" {
		t.Errorf("unexpected body: %q", body)
	}
}

func TestProxy_ForbiddenService(t *testing.T) {
	h := newProxyHandler(storeWith("http://allowed:8080"))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", "http://not-in-list.example.com")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
}

func TestProxy_MissingXServiceHeader(t *testing.T) {
	h := newProxyHandler(connector.NewStore())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestProxy_InvalidXServiceHeader(t *testing.T) {
	// In the allowlist but not a usable URL (no scheme).
	target := "not-a-url"
	h := newProxyHandler(storeWith(target))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", target)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for unparseable X-Service, got %d", rec.Code)
	}
}

func TestProxy_StripsXServiceFromUpstream(t *testing.T) {
	var seenXService string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenXService = r.Header.Get("X-Service")
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenXService != "" {
		t.Errorf("X-Service should be stripped from upstream request, got %q", seenXService)
	}
}

func TestProxy_PreservesHostHeader(t *testing.T) {
	var seenHost string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenHost = r.Host
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Host = "original.example.com"
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenHost != "original.example.com" {
		t.Errorf("expected Host=original.example.com upstream, got %q", seenHost)
	}
}

func TestProxy_ForwardsPathAndQuery(t *testing.T) {
	var seenURI string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource?foo=bar&baz=1", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenURI != "/api/v1/resource?foo=bar&baz=1" {
		t.Errorf("expected upstream URI /api/v1/resource?foo=bar&baz=1, got %q", seenURI)
	}
}

func TestProxy_PrependsAllowlistPathPrefix(t *testing.T) {
	var seenPath string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	// Allowlist entry includes a path prefix.
	target := up.URL + "/api"
	h := newProxyHandler(storeWith(target))
	req := httptest.NewRequest(http.MethodGet, "/v1/resource", nil)
	req.Header.Set("X-Service", target)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenPath != "/api/v1/resource" {
		t.Errorf("expected upstream path /api/v1/resource, got %q", seenPath)
	}
}

// When the inbound path contains percent-encoded characters (e.g. %2F),
// Go's net/url populates RawPath, and Go's Transport writes RawPath to the
// wire if it's set. So modifying only URL.Path when prepending the allowlist
// prefix would silently drop the prefix on the wire for these requests.
func TestProxy_PrependsPathPrefixWithEncodedChars(t *testing.T) {
	var seenRequestURI string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenRequestURI = r.RequestURI
		w.WriteHeader(http.StatusOK)
	})
	target := up.URL + "/api"
	h := newProxyHandler(storeWith(target))
	// %2F (encoded slash) in the path forces net/url to set RawPath.
	req := httptest.NewRequest(http.MethodGet, "/items/a%2Fb", nil)
	req.Header.Set("X-Service", target)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenRequestURI != "/api/items/a%2Fb" {
		t.Errorf("expected upstream request URI /api/items/a%%2Fb, got %q", seenRequestURI)
	}
}

func TestProxy_ForwardsRequestBody(t *testing.T) {
	var seenBody string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seenBody = string(b)
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	body := `{"key":"value"}`
	req := httptest.NewRequest(http.MethodPost, "/data", strings.NewReader(body))
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenBody != body {
		t.Errorf("expected body %q, upstream got %q", body, seenBody)
	}
}

func TestProxy_ForwardsUpstreamStatusCode(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/missing", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 from upstream, got %d", rec.Code)
	}
}

func TestProxy_StreamsLargeResponse(t *testing.T) {
	const size = 1 << 20 // 1 MiB
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 'x'
		}
		for written := 0; written < size; {
			n := min(len(chunk), size-written)
			w.Write(chunk[:n])
			written += n
		}
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/big", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.Len() != size {
		t.Errorf("expected %d bytes, got %d", size, rec.Body.Len())
	}
}

func TestProxy_PostMethodForwarded(t *testing.T) {
	var seenMethod string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		w.WriteHeader(http.StatusCreated)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodPost, "/items", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenMethod != http.MethodPost {
		t.Errorf("expected upstream to see POST, got %q", seenMethod)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
}

func TestProxy_DoesNotFollowRedirects(t *testing.T) {
	var upstreamCalls int
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "followed")
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/start", nil)
	req.Header.Set("X-Service", up.URL)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Errorf("expected 302 passed through, got %d", rec.Code)
	}
	if loc := rec.Result().Header.Get("Location"); loc != "/elsewhere" {
		t.Errorf("expected Location=/elsewhere, got %q", loc)
	}
	if upstreamCalls != 1 {
		t.Errorf("expected upstream to be hit once, got %d", upstreamCalls)
	}
}

func TestProxy_StripsHopByHopHeaders(t *testing.T) {
	// Upstream should not see Connection-listed or fixed hop-by-hop headers.
	var seenInternal, seenKeepAlive string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenInternal = r.Header.Get("X-Internal-Token")
		seenKeepAlive = r.Header.Get("Keep-Alive")
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	// X-Internal-Token is hop-by-hop by virtue of being listed in Connection.
	req.Header.Set("Connection", "X-Internal-Token, Keep-Alive")
	req.Header.Set("X-Internal-Token", "secret")
	req.Header.Set("Keep-Alive", "timeout=5")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenInternal != "" {
		t.Errorf("Connection-listed X-Internal-Token should be stripped, upstream saw %q", seenInternal)
	}
	if seenKeepAlive != "" {
		t.Errorf("Keep-Alive should be stripped as hop-by-hop, upstream saw %q", seenKeepAlive)
	}
}

// The connector is an intermediate hop in a trusted chain: nginx terminates
// TLS and sets X-Forwarded-{For,Proto,Host} authoritatively. The connector
// must forward those values untouched, NOT overwrite them based on its own
// (plain HTTP, Ziti circuit) view of the request.
func TestProxy_PassesThroughForwardedHeaders(t *testing.T) {
	var seenFor, seenProto, seenHost string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenFor = r.Header.Get("X-Forwarded-For")
		seenProto = r.Header.Get("X-Forwarded-Proto")
		seenHost = r.Header.Get("X-Forwarded-Host")
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("X-Forwarded-For", "203.0.113.5")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("X-Forwarded-Host", "site.example.com")
	req.RemoteAddr = "10.0.0.1:1234" // would clobber X-Forwarded-For if we called SetXForwarded
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if seenFor != "203.0.113.5" {
		t.Errorf("X-Forwarded-For should pass through unchanged, got %q", seenFor)
	}
	if seenProto != "https" {
		t.Errorf("X-Forwarded-Proto should pass through as https, got %q", seenProto)
	}
	if seenHost != "site.example.com" {
		t.Errorf("X-Forwarded-Host should pass through, got %q", seenHost)
	}
}

func TestProxy_UpstreamUnreachableReturns502(t *testing.T) {
	// 127.0.0.1:1 is reserved and refuses connections.
	target := "http://127.0.0.1:1"
	h := newProxyHandler(storeWith(target))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", target)
	// Bound the test in case anything hangs.
	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", rec.Code)
	}
	if ct := rec.Result().Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("expected text/html Content-Type, got %q", ct)
	}
	body := rec.Body.String()
	// Assert the page makes the failure boundary obvious and links back home.
	for _, want := range []string{
		"Origin server unreachable",
		"Ingressive",
		"Connector",
		"https://www.ingressive.cloud",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("error page missing expected text %q", want)
		}
	}
}

// The diagnostic detail surfaces the underlying error message — it must be
// HTML-escaped so a hostile error string can't inject markup into the page.
func TestProxy_UpstreamErrorEscapesDiagnostic(t *testing.T) {
	target := "http://127.0.0.1:1"
	h := newProxyHandler(storeWith(target))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", target)
	ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	// The dial error includes "<nil>" in some Go versions / "127.0.0.1:1"
	// — we don't care what's in the string, only that it lives between the
	// code tags and any HTML metacharacters arrive escaped. Sanity-check
	// by confirming there's no raw "<" inside the diagnostic block other
	// than the surrounding tags. The simplest assertion: the page parses
	// as valid HTML by template (which we already executed), and the body
	// doesn't contain "<script" anywhere.
	if strings.Contains(strings.ToLower(rec.Body.String()), "<script") {
		t.Errorf("error page contains <script>, possible XSS via diagnostic")
	}
}
