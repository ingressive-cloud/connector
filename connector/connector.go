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
	"github.com/ingressive/connector/awsv4"
)

const (
	// DefaultHeartbeatInterval is how often the connector sends a heartbeat to the server.
	DefaultHeartbeatInterval = 30 * time.Second

	defaultInitBackoff = time.Second
	maxBackoff         = 60 * time.Second

	// reconnectDelay is the fixed wait before reconnecting after a connection that
	// was successfully established drops. Short so we recover quickly from transient
	// network blips without hammering the server on a hard failure.
	reconnectDelay = 4 * time.Second
)

// GatewayConfig is the message pushed by the server on connect and on every service change.
type GatewayConfig struct {
	GatewayID string   `json:"gateway_id"`
	UpdateID  string   `json:"update_id"`
	Services  []string `json:"services"`
}

// ConnectorMessage is sent from this connector to the server.
type ConnectorMessage struct {
	Type     string `json:"type"`                // "register", "heartbeat", "ack", or "goodbye"
	IP       string `json:"ip,omitempty"`        // present on "register"
	UpdateID string `json:"update_id,omitempty"` // present on "ack"
	Success  bool   `json:"success,omitempty"`   // present on "ack"
}

// Store is a thread-safe in-memory set of allowed service URLs.
type Store struct {
	mu      sync.RWMutex
	allowed map[string]struct{}
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{allowed: make(map[string]struct{})}
}

// Update atomically replaces the allowed service set with the services from cfg.
func (s *Store) Update(cfg GatewayConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]struct{}, len(cfg.Services))
	for _, svc := range cfg.Services {
		next[svc] = struct{}{}
	}
	s.allowed = next
}

// IsAllowed reports whether svcURL is in the current allowed set.
func (s *Store) IsAllowed(svcURL string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.allowed[svcURL]
	return ok
}

// AllowedServices returns a snapshot of the current allowed service URLs.
func (s *Store) AllowedServices() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.allowed))
	for k := range s.allowed {
		out = append(out, k)
	}
	return out
}

// Client manages a WebSocket connection to the Bifrost gateway API.
// It keeps the Store up-to-date and sends periodic heartbeats.
type Client struct {
	// Required.
	WSURL     string // full WebSocket URL: wss://host/connector/ws
	KeyID     string // INGRESSIVE_API_KEY_ID (empty skips signing, for tests)
	KeySecret string // INGRESSIVE_API_KEY_SECRET
	Store     *Store
	NetbirdIP string // IP address on the Netbird interface — reported on register

	// Injectable for testing.
	HeartbeatInterval time.Duration // defaults to DefaultHeartbeatInterval
	InitialBackoff    time.Duration // defaults to 1s
	// NewTicker replaces time.NewTicker for heartbeats.
	NewTicker func(d time.Duration) (<-chan time.Time, func())
	// Dialer replaces websocket.DefaultDialer.
	Dialer *websocket.Dialer
}

func (c *Client) heartbeatInterval() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return DefaultHeartbeatInterval
}

func (c *Client) initialBackoff() time.Duration {
	if c.InitialBackoff > 0 {
		return c.InitialBackoff
	}
	return defaultInitBackoff
}

// Run starts the connection loop, reconnecting with exponential backoff whenever
// the connection is lost. It blocks until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := c.initialBackoff()
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := c.Connect(ctx)
		if err != nil && ctx.Err() == nil {
			if connected {
				slog.Info("gateway connection dropped", "err", err, "reconnect_in", reconnectDelay)
			} else {
				slog.Error("gateway connection failed", "err", err, "reconnect_in", backoff)
			}
		}
		if ctx.Err() != nil {
			return
		}
		var delay time.Duration
		if connected {
			// Was connected then dropped — retry quickly and reset backoff so that
			// any subsequent dial failures start from the beginning of the ramp.
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

// Connect establishes a single WebSocket session: signs the upgrade, sends
// heartbeats, processes config updates, and returns when the connection closes.
// The first return value reports whether the WebSocket dial succeeded; errors
// after that point (dropped connection, write failures) are returned with
// connected=true so Run can distinguish a transient drop from a dial failure.
func (c *Client) Connect(ctx context.Context) (connected bool, _ error) {
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

	slog.Info("connected to gateway", "url", c.WSURL)

	// Send register message immediately so the server knows our IP.
	regMsg := ConnectorMessage{Type: "register", IP: c.NetbirdIP}
	regData, _ := json.Marshal(regMsg)
	if err := conn.WriteMessage(websocket.TextMessage, regData); err != nil {
		return true, fmt.Errorf("write register: %w", err)
	}

	newTicker := c.NewTicker
	if newTicker == nil {
		newTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
	tickCh, stopTicker := newTicker(c.heartbeatInterval())
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
			// Graceful goodbye so the server can immediately remove us from the upstream pool.
			gb := ConnectorMessage{Type: "goodbye"}
			gbData, _ := json.Marshal(gb)
			_ = conn.WriteMessage(websocket.TextMessage, gbData)
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
			return true, nil

		case <-tickCh:
			hb := ConnectorMessage{Type: "heartbeat"}
			data, _ := json.Marshal(hb)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return true, fmt.Errorf("write heartbeat: %w", err)
			}

		case res := <-msgCh:
			if res.err != nil {
				if websocket.IsCloseError(res.err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return true, nil
				}
				return true, fmt.Errorf("read: %w", res.err)
			}
			if err := c.handleMessage(conn, res.data); err != nil {
				return true, err
			}
		}
	}
}

func (c *Client) handleMessage(conn *websocket.Conn, data []byte) error {
	var cfg GatewayConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		slog.Warn("ignoring unparseable message from server", "err", err)
		return nil
	}
	if cfg.UpdateID == "" {
		return nil
	}

	c.Store.Update(cfg)
	slog.Info("config updated", "update_id", cfg.UpdateID, "services", len(cfg.Services))

	ack := ConnectorMessage{
		Type:     "ack",
		UpdateID: cfg.UpdateID,
		Success:  true,
	}
	ackData, _ := json.Marshal(ack)
	if err := conn.WriteMessage(websocket.TextMessage, ackData); err != nil {
		return fmt.Errorf("write ack: %w", err)
	}
	return nil
}

// buildSignedRequest builds an HTTP GET suitable for the WebSocket upgrade
// handshake, signed with AWSv4. Signing is skipped if KeyID is empty (tests).
func (c *Client) buildSignedRequest(ctx context.Context) (*http.Request, error) {
	// AWSv4 requires an https:// URL; convert wss→https, ws→http.
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
