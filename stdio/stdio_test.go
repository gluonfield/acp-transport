package stdio

import (
	"context"
	"encoding/json"
	"net"
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
