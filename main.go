package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/ingressive/connector/connector"
	"github.com/ingressive/connector/netbird"
	"github.com/ingressive/connector/proxy"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

const defaultAPIURL = "https://app.ingressive.cloud"

func run() error {
	apiURL := envOr("INGRESSIVE_API_URL", defaultAPIURL)
	keyID := mustEnv("INGRESSIVE_API_KEY_ID")
	keySecret := mustEnv("INGRESSIVE_API_KEY_SECRET")

	// Build WebSocket URL: the /connector/ws endpoint derives gateway+connector
	// IDs from the access key principal, so no gateway ID env var is needed.
	wsBase := strings.Replace(apiURL, "https://", "wss://", 1)
	wsBase = strings.Replace(wsBase, "http://", "ws://", 1)
	wsURL := strings.TrimRight(wsBase, "/") + "/connector/ws"

	// Discover Netbird interface to bind the proxy on.
	listenIP, err := netbird.FindAddr()
	if err != nil {
		return fmt.Errorf("discover netbird interface: %w", err)
	}

	// INGRESSIVE_ADVERTISE_ADDR overrides the address reported to bifrost in
	// the register message. Used when running in dev
	advertiseIP := envOr("INGRESSIVE_ADVERTISE_ADDR", listenIP)

	bindAddr := listenIP + ":8484"
	slog.Info("starting", "proxy_addr", bindAddr, "advertise_addr", advertiseIP, "gateway_ws", wsURL)

	store := connector.NewStore()

	client := &connector.Client{
		WSURL:     wsURL,
		KeyID:     keyID,
		KeySecret: keySecret,
		Store:     store,
		NetbirdIP: advertiseIP,
	}

	app := proxy.New(store)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	// WebSocket client — keeps store up to date.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.Run(ctx)
	}()

	// Fiber proxy server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			<-ctx.Done()
			_ = app.Shutdown()
		}()
		if err := app.Listen(bindAddr); err != nil {
			slog.Error("proxy server error", "err", err)
		}
	}()

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
