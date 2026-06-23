package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

type Request struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
}

type Handler interface {
	HandleJSONRPC(context.Context, Request) (json.RawMessage, *Error)
}

type HandlerFunc func(context.Context, Request) (json.RawMessage, *Error)

func (f HandlerFunc) HandleJSONRPC(ctx context.Context, req Request) (json.RawMessage, *Error) {
	return f(ctx, req)
}

type Peer struct {
	conn    MessageConn
	handler Handler
	nextID  atomic.Uint64

	mu      sync.Mutex
	pending map[string]chan *Message
}

func NewPeer(conn MessageConn, handler Handler) *Peer {
	return &Peer{
		conn:    conn,
		handler: handler,
		pending: make(map[string]chan *Message),
	}
}

func (p *Peer) Serve(ctx context.Context) error {
	for {
		msg, err := p.conn.Receive(ctx)
		if err != nil {
			p.failPending()
			return err
		}

		switch {
		case msg.IsResponse():
			p.handleResponse(msg)
		case msg.IsRequest():
			go p.handleRequest(ctx, msg)
		case msg.IsNotification():
			p.handleNotification(ctx, msg)
		default:
			// Validate already rejects this, but keep the loop defensive.
		}
	}
}

func (p *Peer) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	idNum := p.nextID.Add(1)
	id, _ := json.Marshal(idNum)
	msg, err := NewRequest(id, method, params)
	if err != nil {
		return nil, err
	}
	key, err := IDKey(id)
	if err != nil {
		return nil, err
	}

	respCh := make(chan *Message, 1)
	p.mu.Lock()
	p.pending[key] = respCh
	p.mu.Unlock()

	if err := p.conn.Send(ctx, msg); err != nil {
		p.deletePending(key)
		return nil, err
	}

	select {
	case <-ctx.Done():
		p.deletePending(key)
		_ = p.Notify(context.Background(), "$/cancel_request", map[string]json.RawMessage{"requestId": id})
		return nil, ctx.Err()
	case resp, ok := <-respCh:
		if !ok {
			return nil, ErrClosed
		}
		if resp.Error != nil {
			return nil, resp.Error
		}
		return append(json.RawMessage(nil), resp.Result...), nil
	}
}

func (p *Peer) Notify(ctx context.Context, method string, params any) error {
	msg, err := NewNotification(method, params)
	if err != nil {
		return err
	}
	return p.conn.Send(ctx, msg)
}

func (p *Peer) Close() error {
	p.failPending()
	return p.conn.Close()
}

func (p *Peer) handleResponse(msg *Message) {
	key, err := IDKey(*msg.ID)
	if err != nil {
		return
	}
	p.mu.Lock()
	ch := p.pending[key]
	delete(p.pending, key)
	p.mu.Unlock()
	if ch != nil {
		ch <- msg.Clone()
		close(ch)
	}
}

func (p *Peer) handleRequest(ctx context.Context, msg *Message) {
	if p.handler == nil {
		resp, _ := NewErrorResponse(*msg.ID, MethodNotFound(msg.Method))
		_ = p.conn.Send(context.Background(), resp)
		return
	}

	result, rpcErr := p.handler.HandleJSONRPC(ctx, Request{
		ID:     append(json.RawMessage(nil), (*msg.ID)...),
		Method: msg.Method,
		Params: append(json.RawMessage(nil), msg.Params...),
	})
	var resp *Message
	var err error
	if rpcErr != nil {
		resp, err = NewErrorResponse(*msg.ID, rpcErr)
	} else {
		resp, err = NewRawResult(*msg.ID, result)
	}
	if err != nil {
		resp, _ = NewErrorResponse(*msg.ID, InternalError("failed to encode response", map[string]any{"error": err.Error()}))
	}
	_ = p.conn.Send(context.Background(), resp)
}

func (p *Peer) handleNotification(ctx context.Context, msg *Message) {
	if p.handler == nil {
		return
	}
	_, _ = p.handler.HandleJSONRPC(ctx, Request{
		Method: msg.Method,
		Params: append(json.RawMessage(nil), msg.Params...),
	})
}

func (p *Peer) deletePending(key string) {
	p.mu.Lock()
	delete(p.pending, key)
	p.mu.Unlock()
}

func (p *Peer) failPending() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, ch := range p.pending {
		delete(p.pending, key)
		close(ch)
	}
}

func MarshalRaw(v any) (json.RawMessage, *Error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, InternalError("failed to marshal result", map[string]any{"error": err.Error()})
	}
	return b, nil
}

func DecodeParams[T any](params json.RawMessage) (T, *Error) {
	var out T
	if len(params) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(params, &out); err != nil {
		return out, InvalidParams("invalid params", map[string]any{"error": err.Error()})
	}
	return out, nil
}

func EncodeResult(v any) (json.RawMessage, *Error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, InternalError(fmt.Sprintf("failed to encode %T", v), map[string]any{"error": err.Error()})
	}
	return b, nil
}
