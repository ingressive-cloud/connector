package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ingressive-cloud/connector/awsv4"
)

const (
	heartbeatInterval = 15 * time.Second
	wsReadDeadline    = 45 * time.Second

	defaultInitBackoff = time.Second
	maxBackoff         = 60 * time.Second
	reconnectDelay     = 4 * time.Second
)

// Store is a thread-safe in-memory set of allowed service URLs.
type Store struct {
	mu      sync.RWMutex
	allowed map[string]struct{}
}

func NewStore() *Store {
	return &Store{allowed: make(map[string]struct{})}
}

func (s *Store) Update(services []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]struct{}, len(services))
	for _, u := range services {
		next[u] = struct{}{}
	}
	s.allowed = next
}

func (s *Store) IsAllowed(svcURL string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.allowed[svcURL]
	return ok
}

func (s *Store) AllowedServices() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.allowed))
	for k := range s.allowed {
		out = append(out, k)
	}
	return out
}

// Client manages the control-plane WebSocket connection to the Ingressive API.
type Client struct {
	WSURL         string
	KeyID         string
	KeySecret     string
	InstanceLabel string
	// Version is the connector binary version reported to the server in the
	// Hello message. Empty is permitted — the server treats that as "unknown".
	Version string
	Store   *Store

	// Injectable for testing.
	InitialBackoff time.Duration
	NewTicker      func(d time.Duration) (<-chan time.Time, func())
	Dialer         *websocket.Dialer
}

func (c *Client) initialBackoff() time.Duration {
	if c.InitialBackoff > 0 {
		return c.InitialBackoff
	}
	return defaultInitBackoff
}

func (c *Client) Run(ctx context.Context) {
	backoff := c.initialBackoff()
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := c.connect(ctx)
		if err != nil && ctx.Err() == nil {
			if connected {
				slog.Warn("Ingressive API connection dropped", "err", err, "reconnect_in", reconnectDelay)
			} else {
				slog.Error("Cannot reach Ingressive API", "err", err, "reconnect_in", backoff)
			}
		}
		if ctx.Err() != nil {
			return
		}
		var delay time.Duration
		if connected {
			delay = reconnectDelay
			backoff = c.initialBackoff()
		} else {
			delay = backoff
			backoff = min(backoff*2, maxBackoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (c *Client) connect(ctx context.Context) (connected bool, _ error) {
	req, err := c.buildSignedRequest(ctx)
	if err != nil {
		return false, err
	}

	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	conn, _, err := dialer.DialContext(ctx, c.WSURL, req.Header)
	if err != nil {
		return false, fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(wsReadDeadline))

	slog.Info("Connected to the Ingressive API")

	// Send hello so the server registers this replica.
	helloMsg := map[string]string{
		"type":           "hello",
		"instance_label": c.InstanceLabel,
	}
	if c.Version != "" {
		helloMsg["version"] = c.Version
	}
	hello, _ := json.Marshal(helloMsg)
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		return true, fmt.Errorf("write hello: %w", err)
	}

	newTicker := c.NewTicker
	if newTicker == nil {
		newTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
	tickCh, stopTicker := newTicker(heartbeatInterval)
	defer stopTicker()

	type readResult struct {
		data []byte
		err  error
	}
	msgCh := make(chan readResult, 8)
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				msgCh <- readResult{err: err}
				return
			}
			msgCh <- readResult{data: data}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			bye, _ := json.Marshal(map[string]string{"type": "goodbye"})
			_ = conn.WriteMessage(websocket.TextMessage, bye)
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
			return true, nil

		case <-tickCh:
			pong, _ := json.Marshal(map[string]string{"type": "pong"})
			if err := conn.WriteMessage(websocket.TextMessage, pong); err != nil {
				return true, fmt.Errorf("write pong: %w", err)
			}

		case res := <-msgCh:
			if res.err != nil {
				if websocket.IsCloseError(res.err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return true, nil
				}
				return true, fmt.Errorf("read: %w", res.err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(wsReadDeadline))
			c.handleMessage(res.data)
		}
	}
}

func (c *Client) handleMessage(data []byte) {
	var msg struct {
		Type     string `json:"type"`
		Services []struct {
			URL string `json:"url"`
		} `json:"services"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		slog.Warn("ignoring unparseable message", "err", err)
		return
	}

	switch msg.Type {
	case "allowlist_update":
		urls := make([]string, 0, len(msg.Services))
		for _, s := range msg.Services {
			urls = append(urls, s.URL)
		}
		c.Store.Update(urls)
		slog.Info("Routing services", "count", len(urls), "services", urls)
	case "ping":
		// server-initiated ping — nothing to do, pong is sent on the ticker
	default:
		slog.Debug("unknown server message", "type", msg.Type)
	}
}

func (c *Client) buildSignedRequest(ctx context.Context) (*http.Request, error) {
	signingURL := strings.Replace(c.WSURL, "wss://", "https://", 1)
	signingURL = strings.Replace(signingURL, "ws://", "http://", 1)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signingURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if c.KeyID != "" {
		if err := awsv4.SignRequest(req, c.KeyID, c.KeySecret); err != nil {
			return nil, fmt.Errorf("sign request: %w", err)
		}
	}

	return req, nil
}
