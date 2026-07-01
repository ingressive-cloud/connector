package zitihost

import (
	"bytes"
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

// zstdLevel is SpeedBetterCompression (~zstd level 7–9). On web text this lands
// around gzip-6 for CPU cost while compressing noticeably smaller. klauspost's
// zstd only exposes four coarse buckets, so a numeric "9" maps here anyway.
const zstdLevel = zstd.SpeedBetterCompression

var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(io.Discard, zstd.WithEncoderLevel(zstdLevel))
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

// compressibleTypes is the exact-match fallback in isCompressibleType for types
// not already caught by its text/* and +json/+xml rules (e.g. application/json,
// application/javascript, application/wasm). Image/video/audio/font formats are
// deliberately absent — already compressed, so re-compressing wastes CPU.
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
// compressor when it is worth doing. For responses with a known Content-Length
// the decision is header-only. For chunked responses (unknown length) we peek
// up to compressMinBytes to decide — this covers the large dynamic-framework
// class (Node/Rails/Django stream their output, so it arrives chunked) that a
// Content-Length-only rule would skip. text/event-stream is never touched
// (isCompressibleType rejects it), so live SSE streams are never buffered.
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

	// Known length: compress iff above the floor.
	if resp.ContentLength >= 0 {
		if resp.ContentLength <= compressMinBytes {
			return nil
		}
		startStreamingCompressor(resp, encoding, resp.Body, resp.Body)
		return nil
	}

	// Unknown length (chunked): read up to the floor to decide. io.ReadFull
	// returns nil only if it filled the buffer — i.e. the body exceeds
	// compressMinBytes and is worth compressing. EOF/ErrUnexpectedEOF mean the
	// whole body is <= the floor (matching the known-length skip rule) or the
	// read failed; either way replay what we consumed and pass through.
	orig := resp.Body
	prefix := make([]byte, compressMinBytes+1)
	n, err := io.ReadFull(orig, prefix)
	prefix = prefix[:n]
	if err != nil {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(prefix), orig), orig}
		return nil
	}
	startStreamingCompressor(resp, encoding, io.MultiReader(bytes.NewReader(prefix), orig), orig)
	return nil
}

// startStreamingCompressor swaps resp.Body for a pipe that compresses src with
// the given encoding, closing body once the copy completes. src may wrap body
// (e.g. a MultiReader replaying peeked bytes), so the underlying body is closed
// explicitly rather than via src.
func startStreamingCompressor(resp *http.Response, encoding string, src io.Reader, body io.Closer) {
	pr, pw := io.Pipe()
	go func() {
		defer body.Close()
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
}

func isCompressibleType(ct string) bool {
	if ct == "" {
		return false
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.ToLower(strings.TrimSpace(ct))
	// Never compress Server-Sent Events: an open-ended stream that must flush
	// per event — buffering or encoding it stalls delivery.
	if ct == "text/event-stream" {
		return false
	}
	// All text/* is compressible.
	if strings.HasPrefix(ct, "text/") {
		return true
	}
	// Structured-syntax suffixes (RFC 6839) — e.g. application/vnd.api+json,
	// application/ld+json, application/problem+json, application/atom+xml.
	if strings.HasSuffix(ct, "+json") || strings.HasSuffix(ct, "+xml") {
		return true
	}
	return compressibleTypes[ct]
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
