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

const defaultAPIURL = "https://app.ingressive.cloud"
const defaultIdentityDir = "/etc/ingressive"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	apiURL := envOr("INGRESSIVE_API_URL", defaultAPIURL)
	connectorSlug := mustEnv("CONNECTOR_ID")
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

	wsBase := strings.Replace(apiURL, "https://", "wss://", 1)
	wsBase = strings.Replace(wsBase, "http://", "ws://", 1)
	wsURL := strings.TrimRight(wsBase, "/") + "/connectors/" + connectorSlug + "/ws"

	slog.Info("Connector starting", "instance", instanceLabel)

	store := connector.NewStore()

	wsClient := &connector.Client{
		WSURL:         wsURL,
		KeyID:         keyID,
		KeySecret:     keySecret,
		InstanceLabel: instanceLabel,
		Store:         store,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Mesh data-plane setup is synchronous and must succeed before we start
	// running. A failure here means the connector cannot route customer
	// traffic, so it must exit non-zero rather than degrade to a control-plane
	// shell that misleads the API into thinking the connector is healthy.
	if err := zitihost.EnsureIdentity(identityDir, enrollmentJWT); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	zitiCtx, err := zitihost.LoadContext(identityDir)
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
