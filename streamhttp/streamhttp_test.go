package streamhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

func TestStreamableHTTPFlow(t *testing.T) {
	server := &Server{
		Backend: func(ctx context.Context) (jsonrpc.MessageConn, error) {
			clientSide, agentSide := jsonrpc.Pipe(32)
			go fakeAgent(t, agentSide)
			return clientSide, nil
		},
	}
	ts := httptest.NewUnstartedServer(server)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	client, err := Dial(ts.URL, WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initReq, _ := jsonrpc.NewRequest(json.RawMessage(`1`), "initialize", map[string]any{"protocolVersion": 1})
	if err := client.Send(ctx, initReq); err != nil {
		t.Fatal(err)
	}
	initResp := receive(t, ctx, client)
	if !initResp.IsResponse() || initResp.ID == nil {
		t.Fatalf("unexpected initialize response: %#v", initResp)
	}

	sessionReq, _ := jsonrpc.NewRequest(json.RawMessage(`2`), "session/new", map[string]any{"cwd": "/tmp", "mcpServers": []any{}})
	if err := client.Send(ctx, sessionReq); err != nil {
		t.Fatal(err)
	}
	sessionResp := receive(t, ctx, client)
	sessionID := jsonrpc.SessionIDFromMessage(sessionResp)
	if sessionID != "sess-1" {
		t.Fatalf("session id = %q", sessionID)
	}

	promptReq, _ := jsonrpc.NewRequest(json.RawMessage(`3`), "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []any{map[string]any{"type": "text", "text": "hi"}},
	})
	if err := client.Send(ctx, promptReq); err != nil {
		t.Fatal(err)
	}
	update := receive(t, ctx, client)
	if update.Method != "session/update" {
		t.Fatalf("update method = %q", update.Method)
	}
	promptResp := receive(t, ctx, client)
	if !promptResp.IsResponse() {
		t.Fatalf("unexpected prompt response: %#v", promptResp)
	}
}

func TestBackendContextOutlivesInitializeRequest(t *testing.T) {
	ctxCh := make(chan context.Context, 1)
	server := &Server{
		Backend: func(ctx context.Context) (jsonrpc.MessageConn, error) {
			ctxCh <- ctx
			clientSide, agentSide := jsonrpc.Pipe(32)
			go fakeAgent(t, agentSide)
			return clientSide, nil
		},
	}
	ts := httptest.NewUnstartedServer(server)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	client, err := Dial(ts.URL, WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	initReq, _ := jsonrpc.NewRequest(json.RawMessage(`1`), "initialize", map[string]any{"protocolVersion": 1})
	if err := client.Send(ctx, initReq); err != nil {
		t.Fatal(err)
	}
	_ = receive(t, ctx, client)

	backendCtx := <-ctxCh
	select {
	case <-backendCtx.Done():
		t.Fatal("backend context was canceled after initialize returned")
	case <-time.After(50 * time.Millisecond):
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backendCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("backend context was not canceled on client close")
	}
}

func TestSessionCloseUnregistersSession(t *testing.T) {
	server := &Server{
		Backend: func(ctx context.Context) (jsonrpc.MessageConn, error) {
			clientSide, agentSide := jsonrpc.Pipe(32)
			go fakeAgent(t, agentSide)
			return clientSide, nil
		},
	}
	ts := httptest.NewUnstartedServer(server)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	client, err := Dial(ts.URL, WithHTTPClient(ts.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initReq, _ := jsonrpc.NewRequest(json.RawMessage(`1`), "initialize", map[string]any{"protocolVersion": 1})
	if err := client.Send(ctx, initReq); err != nil {
		t.Fatal(err)
	}
	_ = receive(t, ctx, client)

	sessionReq, _ := jsonrpc.NewRequest(json.RawMessage(`2`), "session/new", map[string]any{"cwd": "/tmp", "mcpServers": []any{}})
	if err := client.Send(ctx, sessionReq); err != nil {
		t.Fatal(err)
	}
	sessionResp := receive(t, ctx, client)
	sessionID := jsonrpc.SessionIDFromMessage(sessionResp)
	if sessionID == "" {
		t.Fatal("missing session id")
	}

	closeReq, _ := jsonrpc.NewRequest(json.RawMessage(`3`), "session/close", map[string]any{"sessionId": sessionID})
	if err := client.Send(ctx, closeReq); err != nil {
		t.Fatal(err)
	}
	closeResp := receive(t, ctx, client)
	if !closeResp.IsResponse() || closeResp.Error != nil {
		t.Fatalf("unexpected close response: %#v", closeResp)
	}

	promptReq, _ := jsonrpc.NewRequest(json.RawMessage(`4`), "session/prompt", map[string]any{
		"sessionId": sessionID,
		"prompt":    []any{map[string]any{"type": "text", "text": "hi"}},
	})
	err = client.Send(ctx, promptReq)
	if err == nil {
		t.Fatal("expected send to closed session to fail")
	}
	if !errors.Is(err, io.EOF) && !strings.Contains(err.Error(), "404") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadSSELargeEvent(t *testing.T) {
	const size = 11 << 20
	payload := strings.Repeat("x", size)
	notify, err := jsonrpc.NewNotification("session/update", map[string]any{
		"sessionId": "sess-1",
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": payload},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	line, err := notify.MarshalJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	client := &Client{
		recv: make(chan *jsonrpc.Message, 1),
		done: make(chan struct{}),
	}
	err = client.readSSE(context.Background(), "sess-1", strings.NewReader("data: "+strings.TrimSpace(string(line))+"\n\n"))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("readSSE error = %v, want EOF", err)
	}
	got := receive(t, context.Background(), client)
	if got.Method != "session/update" {
		t.Fatalf("method = %q", got.Method)
	}
	if !strings.Contains(string(got.Params), payload[:1024]) {
		t.Fatal("large payload was not preserved")
	}
}

func fakeAgent(t *testing.T, conn jsonrpc.MessageConn) {
	t.Helper()
	for {
		msg, err := conn.Receive(context.Background())
		if err != nil {
			return
		}
		switch msg.Method {
		case "initialize":
			sendResult(t, conn, msg, map[string]any{
				"protocolVersion": 1,
				"agentCapabilities": map[string]any{
					"loadSession": false,
				},
			})
		case "session/new":
			sendResult(t, conn, msg, map[string]any{"sessionId": "sess-1"})
		case "session/prompt":
			notify, err := jsonrpc.NewNotification("session/update", map[string]any{
				"sessionId": "sess-1",
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": "hello"},
				},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.Send(context.Background(), notify); err != nil {
				t.Error(err)
				return
			}
			sendResult(t, conn, msg, map[string]any{"stopReason": "end_turn"})
		case "session/close":
			sendResult(t, conn, msg, map[string]any{})
		default:
			resp, _ := jsonrpc.NewErrorResponse(*msg.ID, jsonrpc.MethodNotFound(msg.Method))
			_ = conn.Send(context.Background(), resp)
		}
	}
}

func sendResult(t *testing.T, conn jsonrpc.MessageConn, req *jsonrpc.Message, result any) {
	t.Helper()
	resp, err := jsonrpc.NewResult(*req.ID, result)
	if err != nil {
		t.Error(err)
		return
	}
	if err := conn.Send(context.Background(), resp); err != nil {
		t.Error(err)
	}
}

func receive(t *testing.T, ctx context.Context, client *Client) *jsonrpc.Message {
	t.Helper()
	msg, err := client.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}
