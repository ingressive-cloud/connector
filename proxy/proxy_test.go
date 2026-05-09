package proxy_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ingressive-cloud/connector/connector"
	"github.com/ingressive-cloud/connector/proxy"
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
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/hello", nil)
	req.Header.Set("X-Service", up.URL)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello from upstream" {
		t.Errorf("unexpected body: %q", body)
	}
}

func TestProxy_ForbiddenService(t *testing.T) {
	app := proxy.New(storeWith("http://allowed:8080"))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", "http://not-in-list.example.com")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %d", resp.StatusCode)
	}
}

func TestProxy_MissingXServiceHeader(t *testing.T) {
	app := proxy.New(connector.NewStore())
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestProxy_StripsXServiceFromUpstream(t *testing.T) {
	var seenXService string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenXService = r.Header.Get("X-Service")
		w.WriteHeader(http.StatusOK)
	})
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)

	resp, _ := app.Test(req)
	resp.Body.Close()

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
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Host = "original.example.com"

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

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
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/resource?foo=bar&baz=1", nil)
	req.Header.Set("X-Service", up.URL)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if seenURI != "/api/v1/resource?foo=bar&baz=1" {
		t.Errorf("expected upstream URI /api/v1/resource?foo=bar&baz=1, got %q", seenURI)
	}
}

func TestProxy_ForwardsRequestBody(t *testing.T) {
	var seenBody string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seenBody = string(b)
		w.WriteHeader(http.StatusOK)
	})
	app := proxy.New(storeWith(up.URL))
	body := `{"key":"value"}`
	req := httptest.NewRequest(http.MethodPost, "/data", strings.NewReader(body))
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if seenBody != body {
		t.Errorf("expected body %q, upstream got %q", body, seenBody)
	}
}

func TestProxy_ForwardsUpstreamStatusCode(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/missing", nil)
	req.Header.Set("X-Service", up.URL)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 from upstream, got %d", resp.StatusCode)
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
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/big", nil)
	req.Header.Set("X-Service", up.URL)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(body) != size {
		t.Errorf("expected %d bytes, got %d", size, len(body))
	}
}

func TestProxy_PostMethodForwarded(t *testing.T) {
	var seenMethod string
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		w.WriteHeader(http.StatusCreated)
	})
	app := proxy.New(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodPost, "/items", nil)
	req.Header.Set("X-Service", up.URL)

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	resp.Body.Close()

	if seenMethod != http.MethodPost {
		t.Errorf("expected upstream to see POST, got %q", seenMethod)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}
}
