package zitihost

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// capHandler records slog records for assertion.
type capHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}
func (h *capHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capHandler) WithGroup(string) slog.Handler      { return h }

func (h *capHandler) only(t *testing.T) (slog.Record, map[string]any) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("expected exactly 1 log record, got %d", len(h.records))
	}
	rec := h.records[0]
	fields := map[string]any{}
	rec.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()
		return true
	})
	return rec, fields
}

// captureLogs installs a capturing handler as the slog default for the test.
func captureLogs(t *testing.T) *capHandler {
	t.Helper()
	cap := &capHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(cap))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return cap
}

func TestAccessLog_Success(t *testing.T) {
	cap := captureLogs(t)

	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "zstd")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "hello")
	})
	h := accessLog(final, "conn-7c9f")

	req := httptest.NewRequest(http.MethodGet, "http://app.internal/api/orders?secret=shh", nil)
	req.Header.Set("X-Service", "http://orders.svc:8080")
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	req.Header.Set("X-Request-Id", "01JZP8QH3")
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec, f := cap.only(t)
	if rec.Level != slog.LevelInfo {
		t.Errorf("expected INFO, got %v", rec.Level)
	}
	if rec.Message != "request" {
		t.Errorf("expected msg=request, got %q", rec.Message)
	}
	if f["status"] != int64(200) {
		t.Errorf("status = %v, want 200", f["status"])
	}
	if f["path"] != "/api/orders" {
		t.Errorf("path = %v, want /api/orders (query must be stripped)", f["path"])
	}
	if f["client_ip"] != "203.0.113.7" {
		t.Errorf("client_ip = %v, want leftmost XFF 203.0.113.7", f["client_ip"])
	}
	if f["request_id"] != "01JZP8QH3" {
		t.Errorf("request_id = %v", f["request_id"])
	}
	if f["replica"] != "conn-7c9f" {
		t.Errorf("replica = %v", f["replica"])
	}
	if f["encoding"] != "zstd" {
		t.Errorf("encoding = %v, want zstd", f["encoding"])
	}
	if f["upstream"] != "http://orders.svc:8080" {
		t.Errorf("upstream = %v", f["upstream"])
	}
	if _, ok := f["origin_ms"]; !ok {
		t.Error("origin_ms should be present on success")
	}
	if tm, ok := f["total_ms"].(float64); !ok || tm < 0 {
		t.Errorf("total_ms should be a non-negative float, got %v (%T)", f["total_ms"], f["total_ms"])
	}
	// Sensitive data must never appear.
	if _, ok := f["query"]; ok {
		t.Error("query must not be logged")
	}
}

func TestAccessLog_UpstreamError(t *testing.T) {
	cap := captureLogs(t)

	// Real proxy handler pointed at an unreachable origin exercises the
	// ErrorHandler -> requestInfo -> access-log plumbing end to end.
	target := "http://127.0.0.1:1"
	h := accessLog(newProxyHandler(storeWith(target)), "conn-1")

	req := httptest.NewRequest(http.MethodGet, "http://app.internal/checkout", nil)
	req.Header.Set("X-Service", target)
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec, f := cap.only(t)
	if rec.Level != slog.LevelWarn {
		t.Errorf("expected WARN for upstream error, got %v", rec.Level)
	}
	if rec.Message != "upstream error" {
		t.Errorf("expected msg=\"upstream error\", got %q", rec.Message)
	}
	if f["status"] != int64(http.StatusBadGateway) {
		t.Errorf("status = %v, want 502", f["status"])
	}
	if _, ok := f["error"]; !ok {
		t.Error("error field should be present on failure")
	}
	if _, ok := f["origin_ms"]; ok {
		t.Error("origin_ms must be omitted when the origin never responded")
	}
}

func TestAccessLog_PreservesFlusher(t *testing.T) {
	// Streaming/SSE depends on the wrapped writer still being an http.Flusher.
	var sawFlusher bool
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawFlusher = w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(accessLog(final, ""))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()

	if !sawFlusher {
		t.Error("wrapped ResponseWriter must still implement http.Flusher")
	}
}

func TestClientIP(t *testing.T) {
	tests := []struct {
		xff  string
		want string
	}{
		{"", ""},
		{"203.0.113.7", "203.0.113.7"},
		{"203.0.113.7, 10.0.0.1", "203.0.113.7"},
		{" 203.0.113.7 , 10.0.0.1", "203.0.113.7"},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.xff != "" {
			req.Header.Set("X-Forwarded-For", tc.xff)
		}
		if got := clientIP(req); got != tc.want {
			t.Errorf("clientIP(%q) = %q, want %q", tc.xff, got, tc.want)
		}
	}
}
