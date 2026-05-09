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

// EnsureIdentity checks for an existing identity.json in dir. If missing and
// jwt is non-empty, it enrolls against the Ziti controller and writes the
// resulting identity to dir. Returns an error if neither is available.
func EnsureIdentity(dir, jwt string) error {
	identityPath := filepath.Join(dir, identityFile)
	if _, err := os.Stat(identityPath); err == nil {
		slog.Info("ziti identity found", "path", identityPath)
		return nil
	}
	if jwt == "" {
		return fmt.Errorf("identity not found at %s and ENROLLMENT_JWT is not set", identityPath)
	}

	slog.Info("enrolling ziti identity", "dir", dir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create identity dir: %w", err)
	}

	claims, jwtToken, err := enroll.ParseToken(jwt)
	if err != nil {
		return fmt.Errorf("parse enrollment JWT: %w", err)
	}

	flags := enroll.EnrollmentFlags{
		JwtString: jwt,
		Token:     claims,
		JwtToken:  jwtToken,
		KeyAlg:    ziti.KeyAlgVar("EC"),
	}
	cfg, err := enroll.Enroll(flags)
	if err != nil {
		return fmt.Errorf("enroll: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal identity config: %w", err)
	}
	if err := os.WriteFile(identityPath, data, 0600); err != nil {
		return fmt.Errorf("write identity: %w", err)
	}
	slog.Info("ziti enrollment complete", "path", identityPath)
	return nil
}

// LoadContext loads a Ziti context from the identity.json in dir.
func LoadContext(dir string) (ziti.Context, error) {
	return ziti.NewContextFromFile(filepath.Join(dir, identityFile))
}

// ServiceName returns the Ziti service name this connector should host.
// It authenticates the context, reads the identity name, and returns
// "connector-<identityName>". The identity name equals the connector UUID
// set by the management reconciler at provisioning time.
func ServiceName(zitiCtx ziti.Context) (string, error) {
	id, err := zitiCtx.GetCurrentIdentity()
	if err != nil {
		return "", fmt.Errorf("get current identity: %w", err)
	}
	if id.Name == nil || *id.Name == "" {
		return "", fmt.Errorf("ziti identity has no name")
	}
	svc := "connector-" + *id.Name
	slog.Info("ziti service name resolved", "service", svc)
	return svc, nil
}

// Host starts serving HTTP on the named Ziti service until ctx is cancelled.
// Incoming HTTP requests are forwarded to the upstream URL in the X-Service
// header, which must be present in store's allowed list.
func Host(ctx context.Context, zitiCtx ziti.Context, serviceName string, store *connector.Store) error {
	listener, err := zitiCtx.Listen(serviceName)
	if err != nil {
		return fmt.Errorf("ziti listen %q: %w", serviceName, err)
	}
	slog.Info("ziti listener ready", "service", serviceName)

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
