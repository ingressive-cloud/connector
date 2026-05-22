// Package zitihost handles Ziti identity lifecycle (enroll/load) and hosting
// the connector's HTTP service on the Ziti overlay.
package zitihost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ingressive-cloud/connector/connector"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/enroll"
)

const identityFile = "identity.json"

// Enroll exchanges a one-time Ziti enrollment JWT for a permanent identity
// config (key + cert + controller URL), serialized as identity.json bytes.
// Pure — no disk I/O, no globals. Callers that want to cache the result
// somewhere durable (a K8s Secret, a file on disk, anywhere) take the bytes
// and persist them themselves.
//
// This is the function the Ingressive controller calls during initial
// bootstrap so the enrollment happens exactly once per identity; the
// connector binary then mounts the resulting identity.json from a K8s Secret
// and never enrolls.
func Enroll(jwt string) ([]byte, error) {
	if jwt == "" {
		return nil, fmt.Errorf("enroll: jwt is empty")
	}
	claims, jwtToken, err := enroll.ParseToken(jwt)
	if err != nil {
		return nil, fmt.Errorf("parse enrollment JWT: %w", err)
	}
	flags := enroll.EnrollmentFlags{
		JwtString: jwt,
		Token:     claims,
		JwtToken:  jwtToken,
		KeyAlg:    ziti.KeyAlgVar("EC"),
	}
	cfg, err := enroll.Enroll(flags)
	if err != nil {
		return nil, fmt.Errorf("enroll: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal identity config: %w", err)
	}
	return data, nil
}

// EnsureIdentity checks for an existing identity.json in dir. If missing and
// jwt is non-empty, it enrolls against the Ziti controller via Enroll and
// writes the resulting identity to dir. Returns an error if neither is
// available.
func EnsureIdentity(dir, jwt string) error {
	identityPath := filepath.Join(dir, identityFile)
	if _, err := os.Stat(identityPath); err == nil {
		slog.Info("Identity loaded", "path", identityPath)
		return nil
	}
	if jwt == "" {
		return fmt.Errorf("identity not found at %s and ENROLLMENT_JWT is not set", identityPath)
	}

	slog.Info("Enrolling new identity")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create identity dir: %w", err)
	}

	data, err := Enroll(jwt)
	if err != nil {
		return err
	}
	if err := os.WriteFile(identityPath, data, 0600); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}
	slog.Info("Identity enrolled — persist this directory on a volume to avoid re-enrollment on restart", "path", identityPath)
	return nil
}

// LoadContext loads a Ziti context from the identity.json in dir.
func LoadContext(dir string) (ziti.Context, error) {
	return ziti.NewContextFromFile(filepath.Join(dir, identityFile))
}

// EnsureWorkingContext loads the identity in dir and verifies it can talk to
// the Ziti controller. If verification fails and jwt is non-empty, the cached
// identity is deleted and re-enrolled once. Returns the verified context or
// the underlying error. Used on startup so a revoked identity self-heals.
func EnsureWorkingContext(dir, jwt string) (ziti.Context, error) {
	attempt := func() (ziti.Context, error) {
		if err := EnsureIdentity(dir, jwt); err != nil {
			return nil, err
		}
		c, err := LoadContext(dir)
		if err != nil {
			return nil, err
		}
		if _, err := c.GetCurrentIdentity(); err != nil {
			return nil, err
		}
		return c, nil
	}

	c, err := attempt()
	if err == nil {
		return c, nil
	}
	if jwt == "" {
		return nil, fmt.Errorf("identity unusable and no ENROLLMENT_JWT to re-enroll: %w", err)
	}

	slog.Warn("identity verification failed — re-enrolling", "err", err)
	if rmErr := os.Remove(filepath.Join(dir, identityFile)); rmErr != nil && !os.IsNotExist(rmErr) {
		return nil, fmt.Errorf("remove stale identity: %w", rmErr)
	}
	c, err = attempt()
	if err != nil {
		return nil, fmt.Errorf("after re-enrollment: %w", err)
	}
	return c, nil
}

// WatchHealth polls the Ziti controller to detect the connector being kicked
// from the network. Returns nil on context cancel, or a non-nil error after
// the controller has been unreachable for `threshold` consecutive polls.
// The caller should propagate the error up so the process exits — letting the
// process restart via the supervisor is cleaner than letting the SDK loop
// indefinitely logging the same failure.
func WatchHealth(ctx context.Context, zitiCtx ziti.Context) error {
	const (
		interval  = 30 * time.Second
		threshold = 3
	)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := zitiCtx.GetCurrentIdentity(); err != nil {
				failures++
				slog.Warn("ziti health check failed", "err", err, "consecutive_failures", failures)
				if failures >= threshold {
					return fmt.Errorf("ziti unreachable after %d consecutive failures: %w", failures, err)
				}
			} else if failures > 0 {
				slog.Info("ziti recovered")
				failures = 0
			}
		}
	}
}

// ServiceName returns the Ziti service name this connector should host.
// Identity name and service name are identical: "connector-<uuid>", set by
// management when the connector is provisioned.
func ServiceName(zitiCtx ziti.Context) (string, error) {
	id, err := zitiCtx.GetCurrentIdentity()
	if err != nil {
		return "", fmt.Errorf("get current identity: %w", err)
	}
	if id.Name == nil || *id.Name == "" {
		return "", fmt.Errorf("identity has no name")
	}
	slog.Debug("service name resolved", "service", *id.Name)
	return *id.Name, nil
}

// Host starts serving HTTP on the named Ziti service until ctx is cancelled.
// Incoming HTTP requests are forwarded to the upstream URL in the X-Service
// header, which must be present in store's allowed list.
func Host(ctx context.Context, zitiCtx ziti.Context, serviceName string, store *connector.Store) error {
	listener, err := zitiCtx.Listen(serviceName)
	if err != nil {
		return fmt.Errorf("listen %q: %w", serviceName, err)
	}
	slog.Info("✓ Ready to route traffic with Ingressive")
	slog.Debug("mesh listener bound", "service", serviceName)

	server := &http.Server{
		Handler:           newProxyHandler(store),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// newProxyHandler returns the HTTP handler that validates the X-Service header
// and forwards the request to that upstream via httputil.ReverseProxy.
//
// ReverseProxy does the things a hand-rolled proxy almost always forgets:
// it streams bodies, strips Connection-listed hop-by-hop headers (not just a
// fixed set), preserves trailers, forwards Upgrade/WebSocket via hijacking,
// flushes streaming responses, and propagates client cancellation. Our only
// custom logic is choosing the upstream URL from the X-Service header.
func newProxyHandler(store *connector.Store) http.Handler {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns: 100,
		// Per-host default is 2, which causes connection churn under any
		// concurrent load against a single customer upstream. Match the
		// global cap so the pool can actually be reused.
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		// Matches Nginx's proxy_read_timeout default. A slow-starting
		// upstream (cold serverless, JIT warm-up, expensive query) should
		// not fail here when it wouldn't fail at the edge.
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	rp := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			// X-Service has already been validated and parsed by the outer
			// handler — it stashed the parsed *url.URL on the request context.
			target := pr.In.Context().Value(targetURLKey{}).(*url.URL)

			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			// If the allowlist entry carries a path prefix (e.g.
			// http://internal/api), prepend it to the request path. Update
			// RawPath too when present — Go's Transport writes RawPath to the
			// wire if non-empty, so modifying only Path would silently drop
			// the prefix for any request with percent-encoded path chars.
			if target.Path != "" && target.Path != "/" {
				prefix := strings.TrimRight(target.Path, "/")
				pr.Out.URL.Path = prefix + pr.Out.URL.Path
				if pr.Out.URL.RawPath != "" {
					pr.Out.URL.RawPath = prefix + pr.Out.URL.RawPath
				}
			}
			// Preserve the client's Host header to the upstream — many origins
			// vhost on it.
			pr.Out.Host = pr.In.Host
			pr.Out.Header.Del("X-Service")

			// ReverseProxy in Rewrite mode strips inbound Forwarded /
			// X-Forwarded-* headers before calling us, because in the general
			// case those values could be client-supplied and spoofable. In our
			// topology the only thing that talks to the connector is the
			// edge's Nginx over the Ziti overlay — a trusted hop that
			// terminates TLS and authoritatively sets these. So copy them
			// back through. We deliberately do NOT call pr.SetXForwarded():
			// it would overwrite Proto with "http" (our Ziti listener is
			// plain HTTP) and append the meaningless Ziti circuit endpoint
			// to For.
			for _, h := range [...]string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v := pr.In.Header.Values(h); len(v) > 0 {
					pr.Out.Header[h] = v
				}
			}
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// Don't log client disconnects as errors.
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("upstream error", "err", err, "target", r.Header.Get("X-Service"))
			writeUpstreamErrorPage(w, err)
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Service")
		if target == "" {
			http.Error(w, "missing X-Service header", http.StatusBadRequest)
			return
		}
		if !store.IsAllowed(target) {
			http.Error(w, "service not in allowed list", http.StatusForbidden)
			return
		}
		u, err := url.Parse(target)
		if err != nil || u.Scheme == "" || u.Host == "" {
			http.Error(w, "invalid X-Service header", http.StatusBadRequest)
			return
		}
		ctx := context.WithValue(r.Context(), targetURLKey{}, u)
		rp.ServeHTTP(w, r.WithContext(ctx))
	})
}

// targetURLKey is a private context key for passing the parsed X-Service URL
// from the outer handler into ReverseProxy.Rewrite without re-parsing.
type targetURLKey struct{}

// writeUpstreamErrorPage renders a friendly 502 HTML page when the connector
// fails to reach the customer's origin. The intent is to make the failure
// boundary obvious: the Ingressive edge and the connector are clearly fine
// (the user is reading this page, after all) — the issue is between the
// connector and the application behind it.
func writeUpstreamErrorPage(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusBadGateway)
	_ = upstreamErrorTmpl.Execute(w, struct{ Err string }{Err: err.Error()})
}

var upstreamErrorTmpl = template.Must(template.New("upstream_error").Parse(upstreamErrorHTML))

const upstreamErrorHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>502 — Origin server unreachable</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; background: #fafafa; color: #1a1a1a; display: flex; align-items: center; justify-content: center; min-height: 100vh; }
  main { max-width: 560px; padding: 2.5rem 2rem; background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; margin: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
  h1 { font-size: 1.5rem; margin: 0 0 1rem; }
  p { line-height: 1.55; margin: 0 0 1rem; color: #374151; }
  .chain { display: flex; gap: 0.5rem; align-items: center; margin: 1.75rem 0; font-size: 0.85rem; flex-wrap: wrap; }
  .chain .node { padding: 0.3rem 0.6rem; border-radius: 4px; }
  .chain .ok { background: #ecfdf5; color: #047857; }
  .chain .fail { background: #fef2f2; color: #b91c1c; font-weight: 600; }
  .chain .arrow { color: #9ca3af; }
  details { margin-top: 1.5rem; background: #f3f4f6; border-radius: 6px; padding: 0.75rem 1rem; font-size: 0.85rem; }
  details summary { cursor: pointer; color: #6b7280; }
  details code { display: block; margin-top: 0.5rem; word-break: break-all; color: #374151; font-size: 0.8rem; }
  footer { margin-top: 1.75rem; font-size: 0.8rem; color: #9ca3af; }
  footer a { color: #2563eb; text-decoration: none; }
  footer a:hover { text-decoration: underline; }
</style>
</head>
<body>
<main>
  <h1>Origin server unreachable</h1>
  <p>Ingressive successfully delivered your request to the connector running in this site's network, but the connector could not reach the application behind it.</p>
  <p>This is usually transient — the application may be restarting, crashed, or briefly unreachable on its network. Try again in a moment.</p>
  <div class="chain">
    <span class="node ok">&#10003; Ingressive edge</span>
    <span class="arrow">&rarr;</span>
    <span class="node ok">&#10003; Connector</span>
    <span class="arrow">&rarr;</span>
    <span class="node fail">&#10007; Origin</span>
  </div>
  <details>
    <summary>Diagnostic detail</summary>
    <code>{{.Err}}</code>
  </details>
  <footer>Routed by <a href="https://www.ingressive.cloud">Ingressive</a></footer>
</main>
</body>
</html>
`
