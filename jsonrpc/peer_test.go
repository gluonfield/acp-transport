package jsonrpc

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPeerHandlesNotificationsInReceiveOrder(t *testing.T) {
	server, client := Pipe(8)
	defer server.Close()
	defer client.Close()

	var mu sync.Mutex
	var got []int
	done := make(chan struct{})

	peer := NewPeer(server, HandlerFunc(func(_ context.Context, req Request) (json.RawMessage, *Error) {
		var params struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			t.Errorf("decode params: %v", err)
			return nil, nil
		}
		if params.N == 1 {
			time.Sleep(50 * time.Millisecond)
		}
		mu.Lock()
		got = append(got, params.N)
		if len(got) == 2 {
			close(done)
		}
		mu.Unlock()
		return nil, nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = peer.Serve(ctx) }()

	first, err := NewNotification("note", map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewNotification("note", map[string]int{"n": 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := client.Send(ctx, second); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notifications")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("notification order = %v, want [1 2]", got)
	}
}
