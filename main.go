package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/ingressive-cloud/connector/connector"
	"github.com/ingressive-cloud/connector/zitihost"
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

	slog.Info("starting", "connector", connectorSlug, "instance", instanceLabel, "ws", wsURL)

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

	var wg sync.WaitGroup

	// WebSocket control-plane loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		wsClient.Run(ctx)
	}()

	// Ziti data-plane: enroll if needed, then host on the overlay.
	if err := zitihost.EnsureIdentity(identityDir, enrollmentJWT); err != nil {
		slog.Warn("ziti identity unavailable — data path disabled", "err", err)
	} else {
		zitiCtx, err := zitihost.LoadContext(identityDir)
		if err != nil {
			slog.Warn("failed to load ziti context — data path disabled", "err", err)
		} else {
			svcName, err := zitihost.ServiceName(zitiCtx)
			if err != nil {
				slog.Warn("failed to resolve ziti service name — data path disabled", "err", err)
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := zitihost.Host(ctx, zitiCtx, svcName, store); err != nil {
						if ctx.Err() == nil {
							slog.Error("ziti host error", "err", err)
						}
					}
				}()
			}
		}
	}

	wg.Wait()
	return nil
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
