package zitihost

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Both encoders carry per-instance buffers that aren't cheap to allocate on
// every request. They support Reset(io.Writer) for reuse, so we pool them.
// Encoders are pulled from the pool, Reset to the response pipe, used to encode
// one stream, Closed (which flushes the final block), and returned.

type resettableWriteCloser interface {
	io.WriteCloser
	Reset(io.Writer)
}

var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			// NewWriter only fails on invalid options — programmer error.
			panic(fmt.Sprintf("zstd encoder construction: %v", err))
		}
		return enc
	},
}

var gzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

func getEncoder(encoding string) resettableWriteCloser {
	switch encoding {
	case "zstd":
		return zstdEncoderPool.Get().(*zstd.Encoder)
	case "gzip":
		return gzipWriterPool.Get().(*gzip.Writer)
	}
	return nil
}

func putEncoder(encoding string, w resettableWriteCloser) {
	switch encoding {
	case "zstd":
		zstdEncoderPool.Put(w.(*zstd.Encoder))
	case "gzip":
		gzipWriterPool.Put(w.(*gzip.Writer))
	}
}

// compressMinBytes is the smallest Content-Length we'll bother compressing.
// Below this, the frame/header overhead and CPU aren't worth the bytes saved
// and can actually grow the response. Mirrors nginx gzip_min_length defaults.
const compressMinBytes = 1000

// compressibleTypes is the set of MIME types we compress. Anything not on this
// list passes through untouched — image/video/audio/font formats are already
// compressed, and re-compressing them wastes CPU for no gain.
var compressibleTypes = map[string]bool{
	"text/html":                 true,
	"text/plain":                true,
	"text/css":                  true,
	"text/xml":                  true,
	"text/javascript":           true,
	"text/csv":                  true,
	"text/markdown":             true,
	"application/json":          true,
	"application/javascript":    true,
	"application/xml":           true,
	"application/xml+rss":       true,
	"application/xhtml+xml":     true,
	"application/wasm":          true,
	"application/manifest+json": true,
	"image/svg+xml":             true,
	"image/svg":                 true, // Just in case some servers omit the +xml suffix
}

// modifyResponseForCompression wraps an upstream response with a streaming
// compressor when it is worth doing. The decision is intentionally header-only
// (no body buffering): we trust the upstream's Content-Length and Content-Type.
// If Content-Length is unknown (chunked) we pass through — buffering to decide
// would add latency-to-first-byte for streamed responses.
//
// Negotiation prefers zstd over gzip when the client offers both. The edge
// Nginx normalises Accept-Encoding upstream to one canonical value, so in
// production this picker sees one of "zstd" / "gzip" / "". The full parser is
// kept so the connector still behaves correctly if reached without that map
// (direct tests, future deployments).
func modifyResponseForCompression(resp *http.Response) error {
	if resp.Header.Get("Content-Encoding") != "" {
		return nil
	}
	if resp.Request != nil && resp.Request.Method == http.MethodHead {
		return nil
	}
	if resp.StatusCode == http.StatusPartialContent {
		return nil
	}
	if resp.ContentLength < 0 || resp.ContentLength <= compressMinBytes {
		return nil
	}
	if !isCompressibleType(resp.Header.Get("Content-Type")) {
		return nil
	}
	var acceptEncoding string
	if resp.Request != nil {
		acceptEncoding = resp.Request.Header.Get("Accept-Encoding")
	}
	encoding := pickEncoding(acceptEncoding)
	if encoding == "" {
		return nil
	}

	pr, pw := io.Pipe()
	src := resp.Body
	go func() {
		defer src.Close()
		enc := getEncoder(encoding)
		defer putEncoder(encoding, enc)
		enc.Reset(pw)

		_, copyErr := io.Copy(enc, src)
		closeErr := enc.Close()
		if copyErr != nil {
			pw.CloseWithError(copyErr)
			return
		}
		pw.CloseWithError(closeErr)
	}()

	resp.Body = pr
	resp.Header.Set("Content-Encoding", encoding)
	addVary(resp.Header, "Accept-Encoding")
	resp.Header.Del("Content-Length")
	resp.ContentLength = -1
	return nil
}

func isCompressibleType(ct string) bool {
	if ct == "" {
		return false
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return compressibleTypes[strings.ToLower(strings.TrimSpace(ct))]
}

// pickEncoding returns "zstd", "gzip", or "" — the best supported encoding the
// client accepts. Respects q-values; zstd wins ties.
func pickEncoding(acceptEncoding string) string {
	if acceptEncoding == "" {
		return ""
	}
	zstdQ, gzipQ := -1.0, -1.0
	for _, part := range strings.Split(acceptEncoding, ",") {
		token, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		token = strings.ToLower(strings.TrimSpace(token))
		if token == "" {
			continue
		}
		q := 1.0
		for params != "" {
			var p string
			p, params, _ = strings.Cut(params, ";")
			k, v, ok := strings.Cut(strings.TrimSpace(p), "=")
			if !ok || strings.ToLower(strings.TrimSpace(k)) != "q" {
				continue
			}
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				q = parsed
			}
		}
		if q <= 0 {
			continue
		}
		switch token {
		case "zstd":
			zstdQ = q
		case "gzip":
			gzipQ = q
		}
	}
	if zstdQ >= 0 && (gzipQ < 0 || zstdQ >= gzipQ) {
		return "zstd"
	}
	if gzipQ >= 0 {
		return "gzip"
	}
	return ""
}

// addVary appends a token to the Vary header if it isn't already present.
func addVary(h http.Header, token string) {
	for _, existing := range h.Values("Vary") {
		for _, t := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return
			}
		}
	}
	h.Add("Vary", token)
}
