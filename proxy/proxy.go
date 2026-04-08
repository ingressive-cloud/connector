package proxy

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/ingressive/connector/connector"
)

// hopByHopHeaders are per-connection headers that must not be forwarded.
var hopByHopHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailers":            true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

var upstreamClient = &http.Client{}

// New creates a Fiber app that reverse-proxies requests to allowed services.
//
// Every request must include an X-Service header containing the full target
// URL (e.g. http://localhost:8080). The header is stripped before forwarding.
// Requests whose X-Service value is not in the current allowed set are
// rejected with 403 Forbidden. The Host header from the incoming request is
// preserved to the upstream.
func New(store *connector.Store) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		StreamRequestBody:     true,
	})

	app.All("/*", func(c *fiber.Ctx) error {
		target := c.Get("X-Service")
		if target == "" {
			return c.Status(fiber.StatusBadRequest).SendString("missing X-Service header")
		}
		if !store.IsAllowed(target) {
			return c.Status(fiber.StatusForbidden).SendString("service not in allowed list")
		}

		// Build upstream URL: strip trailing slash from target, append full request URI.
		upstreamURL := strings.TrimRight(target, "/") + c.OriginalURL()

		req, err := http.NewRequest(string(c.Method()), upstreamURL, bytes.NewReader(c.Body()))
		if err != nil {
			return c.Status(fiber.StatusBadGateway).SendString(fmt.Sprintf("build upstream request: %v", err))
		}

		// Copy incoming headers, excluding X-Service and hop-by-hop headers.
		c.Request().Header.VisitAll(func(key, val []byte) {
			k := strings.ToLower(string(key))
			if k == "x-service" || hopByHopHeaders[k] || k == "host" {
				return
			}
			req.Header.Set(string(key), string(val))
		})

		// Preserve the Host header from the incoming request, not the upstream host.
		req.Host = c.Hostname()

		resp, err := upstreamClient.Do(req)
		if err != nil {
			return c.Status(fiber.StatusBadGateway).SendString(fmt.Sprintf("upstream error: %v", err))
		}
		// Do not defer resp.Body.Close() here — fasthttp closes the stream itself
		// after writing via SetBodyStream, since resp.Body implements io.ReadCloser.

		c.Status(resp.StatusCode)

		// Forward response headers, excluding hop-by-hop headers.
		for k, vs := range resp.Header {
			if hopByHopHeaders[strings.ToLower(k)] {
				continue
			}
			for _, v := range vs {
				c.Response().Header.Add(k, v)
			}
		}

		return c.SendStream(resp.Body)
	})

	return app
}
