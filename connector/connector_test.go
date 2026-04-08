package connector_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ingressive/connector/connector"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// newWSServer spins up an httptest.Server that upgrades to WebSocket and calls handler.
func newWSServer(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("ws upgrade: %v", err)
			return
		}
		defer conn.Close()
		handler(conn)
	}))
}

// wsURL converts an httptest server URL (http://...) to a ws:// URL.
func wsURL(srv *httptest.Server) string {
	return "ws" + srv.URL[4:]
}

// makeClient creates a Client with no signing (empty KeyID) and a slow heartbeat.
func makeClient(url string, store *connector.Store) *connector.Client {
	return &connector.Client{
		WSURL:             url,
		Store:             store,
		HeartbeatInterval: 10 * time.Second,
	}
}

// --- Store tests ---

func TestStore_IsAllowed_True(t *testing.T) {
	s := connector.NewStore()
	s.Update(connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://localhost:8080"}})
	if !s.IsAllowed("http://localhost:8080") {
		t.Fatal("expected service to be allowed")
	}
}

func TestStore_IsAllowed_False(t *testing.T) {
	s := connector.NewStore()
	s.Update(connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://localhost:8080"}})
	if s.IsAllowed("http://evil.example.com") {
		t.Fatal("expected unknown service to be denied")
	}
}

func TestStore_EmptyAfterInit(t *testing.T) {
	s := connector.NewStore()
	if s.IsAllowed("http://anything") {
		t.Fatal("fresh store should deny everything")
	}
}

func TestStore_Update_ReplacesAllowed(t *testing.T) {
	s := connector.NewStore()
	s.Update(connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://old:8080"}})
	s.Update(connector.GatewayConfig{UpdateID: "u2", Services: []string{"http://new:9090"}})
	if s.IsAllowed("http://old:8080") {
		t.Error("old service should be removed after update")
	}
	if !s.IsAllowed("http://new:9090") {
		t.Error("new service should be allowed after update")
	}
}

func TestStore_AllowedServices(t *testing.T) {
	s := connector.NewStore()
	s.Update(connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://a", "http://b"}})
	if len(s.AllowedServices()) != 2 {
		t.Errorf("expected 2 services, got %d", len(s.AllowedServices()))
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := connector.NewStore()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ {
			s.Update(connector.GatewayConfig{UpdateID: "u", Services: []string{"http://a"}})
		}
		close(done)
	}()
	for i := 0; i < 500; i++ {
		_ = s.IsAllowed("http://a")
		_ = s.AllowedServices()
	}
	<-done
}

// --- Client / Connect tests ---

func TestClient_ReceivesInitialConfig(t *testing.T) {
	cfg := connector.GatewayConfig{
		GatewayID: "gw1",
		UpdateID:  "u1",
		Services:  []string{"http://localhost:8080", "https://internal:3000"},
	}
	ackCh := make(chan connector.ConnectorMessage, 1)
	srv := newWSServer(t, func(conn *websocket.Conn) {
		data, _ := json.Marshal(cfg)
		conn.WriteMessage(websocket.TextMessage, data)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		// Read messages skipping register; capture the ack.
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var m connector.ConnectorMessage
			if json.Unmarshal(msg, &m) == nil && m.Type == "ack" {
				ackCh <- m
				break
			}
		}
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Connect(ctx)

	if !store.IsAllowed("http://localhost:8080") {
		t.Error("http://localhost:8080 should be allowed after initial config")
	}
	if !store.IsAllowed("https://internal:3000") {
		t.Error("https://internal:3000 should be allowed after initial config")
	}
	select {
	case ack := <-ackCh:
		if ack.Type != "ack" {
			t.Errorf("expected type=ack, got %q", ack.Type)
		}
		if ack.UpdateID != "u1" {
			t.Errorf("expected update_id=u1, got %q", ack.UpdateID)
		}
		if !ack.Success {
			t.Error("expected ack.Success=true")
		}
	case <-time.After(time.Second):
		t.Fatal("no ack received within 1s")
	}
}

func TestClient_UpdatesConfigOnChange(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		send := func(cfg connector.GatewayConfig) {
			data, _ := json.Marshal(cfg)
			conn.WriteMessage(websocket.TextMessage, data)
			conn.SetReadDeadline(time.Now().Add(time.Second))
			conn.ReadMessage() // consume ack
		}
		send(connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://svc-a:8080"}})
		send(connector.GatewayConfig{UpdateID: "u2", Services: []string{"http://svc-b:9090"}})
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Connect(ctx)

	if store.IsAllowed("http://svc-a:8080") {
		t.Error("svc-a should be evicted after second config update")
	}
	if !store.IsAllowed("http://svc-b:9090") {
		t.Error("svc-b should be allowed after second config update")
	}
}

func TestClient_SendsHeartbeat(t *testing.T) {
	heartbeatCh := make(chan struct{}, 1)
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		// Read messages until we see a heartbeat (skip the initial register).
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var cm connector.ConnectorMessage
			if json.Unmarshal(msg, &cm) == nil && cm.Type == "heartbeat" {
				close(heartbeatCh)
				break
			}
		}
	})
	defer srv.Close()

	store := connector.NewStore()
	client := makeClient(wsURL(srv), store)
	client.HeartbeatInterval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.Connect(ctx)

	select {
	case <-heartbeatCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat received within 2s")
	}
	cancel()
}

func TestClient_ReconnectsOnDisconnect(t *testing.T) {
	var connCount atomic.Int32
	configSent := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if connCount.Add(1) == 1 {
			return // close immediately to trigger reconnect
		}
		cfg := connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://reconnected:8080"}}
		data, _ := json.Marshal(cfg)
		conn.WriteMessage(websocket.TextMessage, data)
		// Read until ack to ensure the client has processed the config.
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var m connector.ConnectorMessage
			if json.Unmarshal(msg, &m) == nil && m.Type == "ack" {
				break
			}
		}
		close(configSent)
		time.Sleep(3 * time.Second)
	}))
	defer srv.Close()

	store := connector.NewStore()
	client := &connector.Client{
		WSURL:             wsURL(srv),
		Store:             store,
		HeartbeatInterval: 10 * time.Second,
		InitialBackoff:    50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go client.Run(ctx)

	select {
	case <-configSent:
	case <-ctx.Done():
		t.Fatal("client did not reconnect and receive config within 5s")
	}
	if !store.IsAllowed("http://reconnected:8080") {
		t.Fatal("store not updated after reconnect")
	}
	cancel()
}

func TestClient_IgnoresInvalidJSON(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.WriteMessage(websocket.TextMessage, []byte("{not valid json"))
		cfg := connector.GatewayConfig{UpdateID: "u1", Services: []string{"http://valid:8080"}}
		data, _ := json.Marshal(cfg)
		conn.WriteMessage(websocket.TextMessage, data)
		conn.SetReadDeadline(time.Now().Add(time.Second))
		conn.ReadMessage()
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Connect(ctx)

	if !store.IsAllowed("http://valid:8080") {
		t.Fatal("valid config after invalid JSON was not applied")
	}
}

func TestClient_IgnoresMissingUpdateID(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		cfg := connector.GatewayConfig{GatewayID: "gw1", Services: []string{"http://svc:8080"}}
		data, _ := json.Marshal(cfg)
		conn.WriteMessage(websocket.TextMessage, data)
		time.Sleep(200 * time.Millisecond)
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Connect(ctx)

	if store.IsAllowed("http://svc:8080") {
		t.Fatal("service should not be allowed when update_id is missing")
	}
}
