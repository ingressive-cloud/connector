// Package zitihost handles Ziti identity lifecycle (enroll/load) and hosting
// the connector's HTTP service on the Ziti overlay.
package zitihost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
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
			// http://internal/api), prepend it to the request path.
			if target.Path != "" && target.Path != "/" {
				pr.Out.URL.Path = strings.TrimRight(target.Path, "/") + pr.Out.URL.Path
			}
			// Preserve the client's Host header to the upstream — many origins
			// vhost on it.
			pr.Out.Host = pr.In.Host
			pr.Out.Header.Del("X-Service")
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// Don't log client disconnects as errors.
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Warn("upstream error", "err", err, "target", r.Header.Get("X-Service"))
			http.Error(w, "upstream error", http.StatusBadGateway)
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
