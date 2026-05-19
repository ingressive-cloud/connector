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
	"github.com/ingressive-cloud/connector/connector"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

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

func wsURL(srv *httptest.Server) string {
	return "ws" + srv.URL[4:]
}

func makeClient(url string, store *connector.Store) *connector.Client {
	return &connector.Client{
		WSURL:          url,
		Store:          store,
		InstanceLabel:  "test",
		InitialBackoff: 50 * time.Millisecond,
	}
}

// allowlistMsg returns a server-side allowlist_update JSON message.
func allowlistMsg(urls ...string) []byte {
	type svc struct {
		URL string `json:"url"`
	}
	type msg struct {
		Type     string `json:"type"`
		Services []svc  `json:"services"`
	}
	svcs := make([]svc, len(urls))
	for i, u := range urls {
		svcs[i] = svc{URL: u}
	}
	b, _ := json.Marshal(msg{Type: "allowlist_update", Services: svcs})
	return b
}

// readMsgType reads one message and returns its "type" field.
func readMsgType(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m.Type
}

// --- Store tests ---

func TestStore_IsAllowed_True(t *testing.T) {
	s := connector.NewStore()
	s.Update([]string{"http://localhost:8080"})
	if !s.IsAllowed("http://localhost:8080") {
		t.Fatal("expected service to be allowed")
	}
}

func TestStore_IsAllowed_False(t *testing.T) {
	s := connector.NewStore()
	s.Update([]string{"http://localhost:8080"})
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
	s.Update([]string{"http://old:8080"})
	s.Update([]string{"http://new:9090"})
	if s.IsAllowed("http://old:8080") {
		t.Error("old service should be removed after update")
	}
	if !s.IsAllowed("http://new:9090") {
		t.Error("new service should be allowed after update")
	}
}

func TestStore_AllowedServices(t *testing.T) {
	s := connector.NewStore()
	s.Update([]string{"http://a", "http://b"})
	if len(s.AllowedServices()) != 2 {
		t.Errorf("expected 2 services, got %d", len(s.AllowedServices()))
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := connector.NewStore()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 500; i++ {
			s.Update([]string{"http://a"})
		}
		close(done)
	}()
	for i := 0; i < 500; i++ {
		_ = s.IsAllowed("http://a")
		_ = s.AllowedServices()
	}
	<-done
}

// --- Client / connect tests ---

func TestClient_SendsHello(t *testing.T) {
	type helloFields struct {
		Label   string
		Version string
	}
	helloCh := make(chan helloFields, 1)
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var m struct {
			Type          string `json:"type"`
			InstanceLabel string `json:"instance_label"`
			Version       string `json:"version"`
		}
		if json.Unmarshal(data, &m) == nil && m.Type == "hello" {
			helloCh <- helloFields{Label: m.InstanceLabel, Version: m.Version}
		}
	})
	defer srv.Close()

	store := connector.NewStore()
	// Pass through a version; the server should see it in the Hello payload.
	client := &connector.Client{WSURL: wsURL(srv), Store: store, InstanceLabel: "unit-test", Version: "0.1.2-test"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go client.Run(ctx)

	select {
	case got := <-helloCh:
		if got.Label != "unit-test" {
			t.Errorf("expected instance_label=unit-test, got %q", got.Label)
		}
		if got.Version != "0.1.2-test" {
			t.Errorf("expected version=0.1.2-test, got %q", got.Version)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no hello received within 2s")
	}
}

// TestClient_SendsHello_NoVersion confirms that when Version is empty the
// "version" field is omitted from the Hello message entirely.
func TestClient_SendsHello_NoVersion(t *testing.T) {
	type helloRaw map[string]any
	helloCh := make(chan helloRaw, 1)
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var m helloRaw
		if json.Unmarshal(data, &m) == nil {
			helloCh <- m
		}
	})
	defer srv.Close()

	store := connector.NewStore()
	client := &connector.Client{WSURL: wsURL(srv), Store: store, InstanceLabel: "no-ver"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go client.Run(ctx)

	select {
	case got := <-helloCh:
		if _, ok := got["version"]; ok {
			t.Errorf("expected no 'version' key in Hello when unset, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no hello received within 2s")
	}
}

func TestClient_ReceivesAllowlistUpdate(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		conn.ReadMessage() // consume hello
		conn.WriteMessage(websocket.TextMessage, allowlistMsg("http://localhost:8080", "https://internal:3000"))
		time.Sleep(200 * time.Millisecond)
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Run(ctx)

	if !store.IsAllowed("http://localhost:8080") {
		t.Error("http://localhost:8080 should be allowed")
	}
	if !store.IsAllowed("https://internal:3000") {
		t.Error("https://internal:3000 should be allowed")
	}
}

func TestClient_UpdateReplacesAllowlist(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		conn.ReadMessage() // hello
		conn.WriteMessage(websocket.TextMessage, allowlistMsg("http://svc-a:8080"))
		conn.WriteMessage(websocket.TextMessage, allowlistMsg("http://svc-b:9090"))
		time.Sleep(200 * time.Millisecond)
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Run(ctx)

	if store.IsAllowed("http://svc-a:8080") {
		t.Error("svc-a should be evicted after second update")
	}
	if !store.IsAllowed("http://svc-b:9090") {
		t.Error("svc-b should be allowed after second update")
	}
}

func TestClient_SendsPong(t *testing.T) {
	pongCh := make(chan struct{}, 1)
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		conn.ReadMessage() // hello
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct{ Type string `json:"type"` }
			if json.Unmarshal(data, &m) == nil && m.Type == "pong" {
				select {
				case pongCh <- struct{}{}:
				default:
				}
				return
			}
		}
	})
	defer srv.Close()

	store := connector.NewStore()
	client := &connector.Client{
		WSURL:          wsURL(srv),
		Store:          store,
		InstanceLabel:  "test",
		InitialBackoff: 50 * time.Millisecond,
		NewTicker: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(50 * time.Millisecond)
			return t.C, t.Stop
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.Run(ctx)

	select {
	case <-pongCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no pong received within 2s")
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
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		conn.ReadMessage() // hello
		conn.WriteMessage(websocket.TextMessage, allowlistMsg("http://reconnected:8080"))
		close(configSent)
		time.Sleep(3 * time.Second)
	}))
	defer srv.Close()

	store := connector.NewStore()
	client := &connector.Client{
		WSURL:          wsURL(srv),
		Store:          store,
		InstanceLabel:  "test",
		InitialBackoff: 50 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go client.Run(ctx)

	select {
	case <-configSent:
	case <-ctx.Done():
		t.Fatal("client did not reconnect and receive config within 5s")
	}
	time.Sleep(100 * time.Millisecond)
	if !store.IsAllowed("http://reconnected:8080") {
		t.Fatal("store not updated after reconnect")
	}
	cancel()
}

func TestClient_IgnoresInvalidJSON(t *testing.T) {
	srv := newWSServer(t, func(conn *websocket.Conn) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		conn.ReadMessage() // hello
		conn.WriteMessage(websocket.TextMessage, []byte("{not valid json"))
		conn.WriteMessage(websocket.TextMessage, allowlistMsg("http://valid:8080"))
		time.Sleep(200 * time.Millisecond)
	})
	defer srv.Close()

	store := connector.NewStore()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	makeClient(wsURL(srv), store).Run(ctx)

	if !store.IsAllowed("http://valid:8080") {
		t.Fatal("valid config after invalid JSON was not applied")
	}
}
