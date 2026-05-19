// Package zitihost handles Ziti identity lifecycle (enroll/load) and hosting
// the connector's HTTP service on the Ziti overlay.
package zitihost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ingressive-cloud/connector/connector"
	"github.com/openziti/sdk-golang/ziti"
	"github.com/openziti/sdk-golang/ziti/enroll"
)

const identityFile = "identity.json"

// hopByHopHeaders are stripped before forwarding to the upstream.
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
		// Hit the controller to verify the identity is still valid.
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

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	return http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Service")
		if target == "" {
			http.Error(w, "missing X-Service header", http.StatusBadRequest)
			return
		}
		if !store.IsAllowed(target) {
			http.Error(w, "service not in allowed list", http.StatusForbidden)
			return
		}

		upstreamURL := strings.TrimRight(target, "/") + r.RequestURI

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadGateway)
			return
		}

		req, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL, bytes.NewReader(body))
		if err != nil {
			http.Error(w, "build upstream request: "+err.Error(), http.StatusBadGateway)
			return
		}
		req.Host = r.Host

		for k, vs := range r.Header {
			lower := strings.ToLower(k)
			if lower == "x-service" || hopByHopHeaders[lower] {
				continue
			}
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for k, vs := range resp.Header {
			if hopByHopHeaders[strings.ToLower(k)] {
				continue
			}
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
}
