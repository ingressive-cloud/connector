package zitihost

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// accessKey carries per-request state from the proxy's ErrorHandler back to the
// access-log middleware, which are separated by httputil.ReverseProxy.
type accessKey struct{}

type requestInfo struct {
	err error
}

// accessLog wraps next, emitting one JSON log line per completed request from
// the connector's origin-facing vantage. replica labels which connector
// instance served the request. The query string and sensitive headers
// (Authorization, Cookie, …) are deliberately never logged.
//
// msg is constant ("request") on the happy path — the fields are the message —
// and descriptive only on failure ("upstream error", "client disconnected"),
// where a human stopping to read the line benefits from a sentence.
func accessLog(next http.Handler, replica string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := &requestInfo{}
		rec := &responseRecorder{ResponseWriter: w, start: time.Now()}

		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), accessKey{}, info)))

		dur := time.Since(rec.start)
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}

		level := slog.LevelInfo
		msg := "request"
		switch {
		case info.err != nil && errors.Is(info.err, context.Canceled):
			msg = "client disconnected"
		case info.err != nil:
			level, msg = slog.LevelWarn, "upstream error"
		case status >= 500:
			level = slog.LevelWarn
		}

		// Ordered most-important-first so the line is scannable raw.
		attrs := []slog.Attr{
			slog.Int("status", status),
			slog.String("method", r.Method),
			slog.String("host", r.Host),
			slog.String("path", r.URL.Path),
			slog.Float64("total_ms", ms(dur)),
		}
		// origin_ms only when the origin actually produced a response — on a
		// transport failure the "time to first byte" is a failed dial, not
		// origin latency, so omitting it is more honest than logging it. It's a
		// portion of total_ms (the rest is overlay + transfer + our overhead).
		if info.err == nil && rec.wroteHeader {
			attrs = append(attrs, slog.Float64("origin_ms", ms(rec.firstByteAt.Sub(rec.start))))
		}
		attrs = append(attrs, slog.Int64("bytes", rec.bytes))
		if enc := rec.Header().Get("Content-Encoding"); enc != "" {
			attrs = append(attrs, slog.String("encoding", enc))
		}
		if up := r.Header.Get("X-Service"); up != "" {
			attrs = append(attrs, slog.String("upstream", up))
		}
		if ip := clientIP(r); ip != "" {
			attrs = append(attrs, slog.String("client_ip", ip))
		}
		if id := r.Header.Get("X-Request-Id"); id != "" {
			attrs = append(attrs, slog.String("request_id", id))
		}
		if replica != "" {
			attrs = append(attrs, slog.String("replica", replica))
		}
		if info.err != nil {
			attrs = append(attrs, slog.String("error", info.err.Error()))
		}

		slog.LogAttrs(context.Background(), level, msg, attrs...)
	})
}

// ms renders a duration as fractional milliseconds at microsecond resolution,
// so sub-millisecond requests (common when adjacent to the origin) don't round
// to zero, while staying a queryable JSON number.
func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// clientIP returns the leftmost X-Forwarded-For entry — the original client as
// seen by the trusted edge. Empty if the header is absent.
func clientIP(r *http.Request) string {
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return ""
	}
	if i := strings.IndexByte(xff, ','); i >= 0 {
		xff = xff[:i]
	}
	return strings.TrimSpace(xff)
}

// responseRecorder captures status and byte count while preserving the
// Flusher/Hijacker behaviour ReverseProxy relies on for streaming responses and
// protocol upgrades (WebSocket). Without forwarding these, SSE would stall and
// upgrades would fail.
type responseRecorder struct {
	http.ResponseWriter
	start       time.Time
	firstByteAt time.Time
	status      int
	bytes       int64
	wroteHeader bool
}

var (
	_ http.Flusher  = (*responseRecorder)(nil)
	_ http.Hijacker = (*responseRecorder)(nil)
)

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.firstByteAt = time.Now()
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
	}
	r.status = http.StatusSwitchingProtocols
	r.wroteHeader = true
	return h.Hijack()
}

// Unwrap lets http.ResponseController reach the underlying writer for any
// capability responseRecorder doesn't override.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
