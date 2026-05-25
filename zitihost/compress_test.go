package zitihost

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestPickEncoding(t *testing.T) {
	tests := []struct {
		header string
		want   string
	}{
		{"", ""},
		{"gzip", "gzip"},
		{"zstd", "zstd"},
		{"gzip, zstd", "zstd"},
		{"gzip, deflate, br, zstd", "zstd"},
		{"br, deflate", ""},
		{"identity", ""},
		{"GZIP", "gzip"},                            // case-insensitive
		{"gzip;q=0", ""},                            // explicit refusal
		{"gzip;q=0.5, zstd;q=0.8", "zstd"},          // q-value preference
		{"gzip;q=0.8, zstd;q=0.5", "gzip"},          // gzip wins on q
		{"gzip;q=0.5, zstd;q=0.5", "zstd"},          // zstd wins ties
		{"gzip ; q=1.0 , zstd ; q=1.0", "zstd"},     // whitespace tolerant
		{"zstd;q=0, gzip", "gzip"},                  // zstd refused, gzip allowed
		{"gzip;q=0, zstd;q=0", ""},                  // all refused
		{"gzip;q=0.5;foo=bar, zstd;baz=qux", "zstd"}, // ignore unknown params
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			got := pickEncoding(tc.header)
			if got != tc.want {
				t.Errorf("pickEncoding(%q) = %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}

func TestIsCompressibleType(t *testing.T) {
	tests := []struct {
		ct   string
		want bool
	}{
		{"", false},
		{"text/html", true},
		{"text/html; charset=utf-8", true},
		{"TEXT/HTML", true},
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"image/svg+xml", true},
		{"image/jpeg", false},
		{"image/png", false},
		{"video/mp4", false},
		{"font/woff2", false},
		{"application/octet-stream", false},
	}
	for _, tc := range tests {
		t.Run(tc.ct, func(t *testing.T) {
			got := isCompressibleType(tc.ct)
			if got != tc.want {
				t.Errorf("isCompressibleType(%q) = %v, want %v", tc.ct, got, tc.want)
			}
		})
	}
}

// largeText returns n bytes of compressible content (repeating dictionary words
// rather than a single byte — single-byte repetition compresses too unrealistically
// well and hides real-world ratio behaviour).
func largeText(n int) []byte {
	const phrase = "the quick brown fox jumps over the lazy dog. "
	var buf bytes.Buffer
	for buf.Len() < n {
		buf.WriteString(phrase)
	}
	return buf.Bytes()[:n]
}

func TestProxy_CompressesLargeTextWithZstd(t *testing.T) {
	body := largeText(8192)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "gzip, zstd")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Result().Header.Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("expected Content-Encoding=zstd, got %q", got)
	}
	if rec.Result().Header.Get("Content-Length") != "" {
		t.Errorf("Content-Length should be dropped when streaming compressed body")
	}
	if vary := rec.Result().Header.Get("Vary"); !strings.Contains(strings.ToLower(vary), "accept-encoding") {
		t.Errorf("expected Vary to include Accept-Encoding, got %q", vary)
	}
	if rec.Body.Len() >= len(body) {
		t.Errorf("compressed body (%d) should be smaller than original (%d)", rec.Body.Len(), len(body))
	}

	dec, err := zstd.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}
	if !bytes.Equal(out, body) {
		t.Errorf("decoded body mismatch (len got=%d want=%d)", len(out), len(body))
	}
}

func TestProxy_CompressesLargeTextWithGzipFallback(t *testing.T) {
	body := largeText(4096)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if got := rec.Result().Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("expected Content-Encoding=gzip, got %q", got)
	}
	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer gz.Close()
	out, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("gzip decode: %v", err)
	}
	if !bytes.Equal(out, body) {
		t.Errorf("decoded body mismatch")
	}
}

func TestProxy_SkipsCompressionBelowMinLength(t *testing.T) {
	body := largeText(500) // < 1000 threshold
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("expected no Content-Encoding for small body, got %q", ce)
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Errorf("body should pass through unchanged")
	}
}

func TestProxy_SkipsCompressionForIncompressibleType(t *testing.T) {
	body := largeText(8192)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/img", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("expected no Content-Encoding for image/jpeg, got %q", ce)
	}
	if rec.Body.Len() != len(body) {
		t.Errorf("body length should be unchanged; got %d want %d", rec.Body.Len(), len(body))
	}
}

func TestProxy_SkipsCompressionWhenContentLengthUnknown(t *testing.T) {
	// No Content-Length → chunked. Per "trust Content-Length" rule, skip.
	body := largeText(8192)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		// Deliberately don't set Content-Length; httptest will chunk.
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("expected no Content-Encoding for chunked response, got %q", ce)
	}
}

func TestProxy_PreservesUpstreamContentEncoding(t *testing.T) {
	// Upstream already gzipped — pass through, do not re-encode.
	plain := largeText(4096)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write(plain)
	gw.Close()
	gzipped := buf.Bytes()

	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(len(gzipped)))
		w.WriteHeader(http.StatusOK)
		w.Write(gzipped)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "gzip" {
		t.Errorf("expected upstream gzip pass-through, got Content-Encoding=%q", ce)
	}
	if !bytes.Equal(rec.Body.Bytes(), gzipped) {
		t.Errorf("body should be unchanged gzipped bytes")
	}
}

func TestProxy_SkipsCompressionWhenClientDoesNotAccept(t *testing.T) {
	body := largeText(8192)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	// No Accept-Encoding header at all.
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("expected no Content-Encoding when client doesn't accept, got %q", ce)
	}
	if rec.Body.Len() != len(body) {
		t.Errorf("body should be unchanged")
	}
}

func TestProxy_SkipsCompressionForHEADRequest(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		// HEAD responses carry the headers a GET would return, including
		// Content-Length for the body that won't be sent.
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", "8192")
		w.WriteHeader(http.StatusOK)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodHead, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("HEAD responses must not gain Content-Encoding, got %q", ce)
	}
	if cl := rec.Result().Header.Get("Content-Length"); cl != "8192" {
		t.Errorf("HEAD response must preserve Content-Length, got %q", cl)
	}
}

// Pool reuse: hit the proxy concurrently with mixed encodings so the same
// encoder objects get Reset across goroutines. Any state bleed between
// streams would corrupt decode.
func TestProxy_ConcurrentCompressionViaPool(t *testing.T) {
	body := largeText(4096)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("X-Service", up.URL)
			if i%2 == 0 {
				req.Header.Set("Accept-Encoding", "zstd, gzip")
			} else {
				req.Header.Set("Accept-Encoding", "gzip")
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			var dec io.ReadCloser
			switch rec.Result().Header.Get("Content-Encoding") {
			case "zstd":
				zr, err := zstd.NewReader(rec.Body)
				if err != nil {
					errs <- err
					return
				}
				dec = zr.IOReadCloser()
			case "gzip":
				gz, err := gzip.NewReader(rec.Body)
				if err != nil {
					errs <- err
					return
				}
				dec = gz
			default:
				errs <- io.ErrUnexpectedEOF
				return
			}
			defer dec.Close()
			out, err := io.ReadAll(dec)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(out, body) {
				errs <- io.ErrShortBuffer
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent compression: %v", err)
	}
}

func TestProxy_SkipsCompressionFor206Partial(t *testing.T) {
	// Compressing a 206 would invalidate the byte-range mapping.
	body := largeText(8192)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Range", "bytes 0-8191/16384")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(body)
	})
	h := newProxyHandler(storeWith(up.URL))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Service", up.URL)
	req.Header.Set("Range", "bytes=0-8191")
	req.Header.Set("Accept-Encoding", "zstd, gzip")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Errorf("expected 206 pass-through, got %d", rec.Code)
	}
	if ce := rec.Result().Header.Get("Content-Encoding"); ce != "" {
		t.Errorf("206 must not be compressed, got Content-Encoding=%q", ce)
	}
}
