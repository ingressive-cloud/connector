package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ingressive-cloud/connector/connector"
	"github.com/ingressive-cloud/connector/zitihost"
	"golang.org/x/sync/errgroup"
)

const defaultAPIURL = "https://console.ingressive.cloud"
const defaultIdentityDir = "/etc/ingressive"

// Version is the connector binary version. Overridden at build time via
//
//	go build -ldflags "-X main.Version=$VERSION"
//
// It's surfaced to the Ingressive API via the Hello message so the console can
// show which version is running on each replica.
var Version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	apiURL := envOr("INGRESSIVE_API_URL", defaultAPIURL)
	keyID := mustEnv("INGRESSIVE_API_KEY_ID")
	keySecret := mustEnv("INGRESSIVE_API_KEY_SECRET")
	identityDir := envOr("INGRESSIVE_IDENTITY_DIR", defaultIdentityDir)
	enrollmentJWT := os.Getenv("ENROLLMENT_JWT")

	instanceLabel := envOr("INGRESSIVE_INSTANCE_LABEL", "")
	if instanceLabel == "" {
		if h, err := os.Hostname(); err == nil {
			instanceLabel = h
		} else {
			instanceLabel = "connector"
		}
	}

	// The connector's identity is the API key principal — the server
	// derives the connector from the credential, not from a URL slug.
	wsBase := strings.Replace(apiURL, "https://", "wss://", 1)
	wsBase = strings.Replace(wsBase, "http://", "ws://", 1)
	wsURL := strings.TrimRight(wsBase, "/") + "/connectors/ws"

	slog.Info("Connector starting", "instance", instanceLabel)

	store := connector.NewStore()

	wsClient := &connector.Client{
		WSURL:         wsURL,
		KeyID:         keyID,
		KeySecret:     keySecret,
		InstanceLabel: instanceLabel,
		Version:       Version,
		Store:         store,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Mesh data-plane setup is synchronous and must succeed before we start
	// running. A failure here means the connector cannot route customer
	// traffic, so it must exit non-zero rather than degrade to a control-plane
	// shell that misleads the API into thinking the connector is healthy.
	//
	// EnsureWorkingContext also handles the case where the cached identity
	// has been revoked on the controller: it deletes the stale file and
	// re-enrolls via ENROLLMENT_JWT if one is provided.
	zitiCtx, err := zitihost.EnsureWorkingContext(identityDir, enrollmentJWT)
	if err != nil {
		return fmt.Errorf("mesh context: %w", err)
	}
	svcName, err := zitihost.ServiceName(zitiCtx)
	if err != nil {
		return fmt.Errorf("resolve service name: %w", err)
	}

	// Run both subsystems in an errgroup so either one exiting (cleanly or
	// otherwise) cancels the other and brings the process down.
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		wsClient.Run(gctx)
		// Run only returns on context cancel; if it returns for any other
		// reason that's a bug, but treat it as a fatal exit either way.
		return nil
	})

	g.Go(func() error {
		if err := zitihost.Host(gctx, zitiCtx, svcName, store); err != nil {
			if gctx.Err() != nil {
				return nil // shutting down — not a fatal error
			}
			return fmt.Errorf("mesh: %w", err)
		}
		return nil
	})

	// Watchdog: if we get kicked off the Ziti network, exit non-zero rather
	// than let the SDK loop logging the same failure forever. The supervisor
	// will restart us; on restart EnsureWorkingContext re-enrolls if possible.
	g.Go(func() error {
		return zitihost.WatchHealth(gctx, zitiCtx)
	})

	return g.Wait()
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "var", key)
		os.Exit(1)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
