package stdio

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

func TestConnRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	left := New(a, a)
	right := New(b, b)

	msg, err := jsonrpc.NewRequest(json.RawMessage(`1`), "initialize", map[string]any{"protocolVersion": 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := left.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	got, err := right.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "initialize" {
		t.Fatalf("method = %q", got.Method)
	}
}

func TestConnRoundTripLargeMessage(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	left := New(a, a)
	right := New(b, b)

	const size = 11 << 20
	payload := strings.Repeat("x", size)
	msg, err := jsonrpc.NewNotification("session/update", map[string]any{
		"sessionId": "sess-1",
		"update": map[string]any{
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"type": "text", "text": payload},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := left.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	got, err := right.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "session/update" {
		t.Fatalf("method = %q", got.Method)
	}
	if !strings.Contains(string(got.Params), payload[:1024]) {
		t.Fatal("large payload was not preserved")
	}
}
