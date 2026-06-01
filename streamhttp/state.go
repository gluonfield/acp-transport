package streamhttp

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"sync"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

var ErrEventHistoryGone = jsonrpc.InvalidRequest("event history is no longer available", nil)

type event struct {
	id   uint64
	data []byte
}

type streamScope struct {
	nextID uint64
	ch     chan event
	queue  []event
}

type connState struct {
	id        string
	backend   jsonrpc.MessageConn
	cancel    context.CancelFunc
	maxQueued int

	mu            sync.Mutex
	scopes        map[string]*streamScope
	sessions      map[string]bool
	pendingScope  map[string]string
	pendingMethod map[string]string
	initKey       string
	initCh        chan *jsonrpc.Message
	err           error
	closed        bool
}

func newConnState(id string, backend jsonrpc.MessageConn, cancel context.CancelFunc, maxQueued int) *connState {
	return &connState{
		id:            id,
		backend:       backend,
		cancel:        cancel,
		maxQueued:     maxQueued,
		scopes:        map[string]*streamScope{"": {}},
		sessions:      make(map[string]bool),
		pendingScope:  make(map[string]string),
		pendingMethod: make(map[string]string),
		initCh:        make(chan *jsonrpc.Message, 1),
	}
}

func (c *connState) setInit(msg *jsonrpc.Message) error {
	key, err := jsonrpc.IDKey(*msg.ID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initKey = key
	return nil
}

func (c *connState) waitInit(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-c.initCh:
		if !ok {
			return nil, c.closeErr()
		}
		return msg, nil
	}
}

func (c *connState) run() {
	for {
		msg, err := c.backend.Receive(context.Background())
		if err != nil {
			c.closeWith(err)
			return
		}
		c.routeFromBackend(msg)
	}
}

func (c *connState) sendToBackend(ctx context.Context, msg *jsonrpc.Message, sessionID string) error {
	if msg.IsRequest() {
		key, err := jsonrpc.IDKey(*msg.ID)
		if err != nil {
			return err
		}
		c.mu.Lock()
		if c.closed {
			err := c.closeErrLocked()
			c.mu.Unlock()
			return err
		}
		c.pendingScope[key] = sessionID
		c.pendingMethod[key] = msg.Method
		c.mu.Unlock()
	}
	return c.backend.Send(ctx, msg)
}

func (c *connState) routeFromBackend(msg *jsonrpc.Message) {
	if msg.IsResponse() {
		if c.routeInit(msg) {
			return
		}
	}

	scope, method := c.scopeFor(msg)
	if sessionID := jsonrpc.SessionIDFromMessage(msg); sessionID != "" {
		c.registerSession(sessionID)
	}
	c.emit(scope, msg)
	if method == "session/close" && msg.Error == nil && scope != "" {
		c.unregisterSession(scope)
	}
}

func (c *connState) routeInit(msg *jsonrpc.Message) bool {
	key, err := jsonrpc.IDKey(*msg.ID)
	if err != nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return true
	}
	if key != c.initKey || c.initKey == "" {
		return false
	}
	c.initKey = ""
	select {
	case c.initCh <- msg.Clone():
	default:
	}
	return true
}

func (c *connState) scopeFor(msg *jsonrpc.Message) (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if msg.IsResponse() {
		key, err := jsonrpc.IDKey(*msg.ID)
		if err == nil {
			scope := c.pendingScope[key]
			method := c.pendingMethod[key]
			delete(c.pendingScope, key)
			delete(c.pendingMethod, key)
			return scope, method
		}
	}

	if sessionID := jsonrpc.SessionIDFromMessage(msg); sessionID != "" {
		return sessionID, ""
	}
	return "", ""
}

func (c *connState) registerSession(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessions[sessionID] = true
	if c.scopes[sessionID] == nil {
		c.scopes[sessionID] = &streamScope{}
	}
}

func (c *connState) hasSession(sessionID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[sessionID]
}

func (c *connState) unregisterSession(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessions, sessionID)
}

func (c *connState) attachStream(sessionID string, after uint64) (<-chan event, func(), error) {
	replayCap := defaultSSEBuffer

	c.mu.Lock()
	scope := c.scopeLocked(sessionID)
	replay, err := scope.eventsAfter(after)
	if err != nil {
		c.mu.Unlock()
		return nil, nil, err
	}
	if len(replay) > replayCap {
		replayCap = len(replay)
	}
	ch := make(chan event, replayCap)
	old := scope.ch
	scope.ch = ch
	for _, ev := range replay {
		ch <- ev
	}
	c.mu.Unlock()

	if old != nil {
		close(old)
	}

	return ch, func() {
		c.mu.Lock()
		if scope.ch == ch {
			scope.ch = nil
		}
		c.mu.Unlock()
	}, nil
}

func (s *streamScope) eventsAfter(after uint64) ([]event, error) {
	if after > s.nextID {
		return nil, ErrEventHistoryGone
	}
	if after == 0 {
		return append([]event(nil), s.queue...), nil
	}
	if len(s.queue) == 0 {
		if after < s.nextID {
			return nil, ErrEventHistoryGone
		}
		return nil, nil
	}
	if after < s.queue[0].id-1 {
		return nil, ErrEventHistoryGone
	}
	i := 0
	for i < len(s.queue) && s.queue[i].id <= after {
		i++
	}
	return append([]event(nil), s.queue[i:]...), nil
}

func (c *connState) emit(sessionID string, msg *jsonrpc.Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		c.closeWith(err)
		return
	}

	var old chan event
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	scope := c.scopeLocked(sessionID)
	scope.nextID++
	ev := event{id: scope.nextID, data: data}
	scope.queue = append(scope.queue, ev)
	if len(scope.queue) > c.maxQueued {
		copy(scope.queue, scope.queue[len(scope.queue)-c.maxQueued:])
		scope.queue = scope.queue[:c.maxQueued]
	}
	if scope.ch != nil {
		select {
		case scope.ch <- ev:
		default:
			old = scope.ch
			scope.ch = nil
		}
	}
	c.mu.Unlock()

	if old != nil {
		close(old)
	}
}

func (c *connState) scopeLocked(sessionID string) *streamScope {
	scope := c.scopes[sessionID]
	if scope == nil {
		scope = &streamScope{}
		c.scopes[sessionID] = scope
	}
	return scope
}

func (c *connState) close() {
	c.closeWith(jsonrpc.ErrClosed)
}

func (c *connState) closeWith(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if err == nil {
		err = io.EOF
	}
	c.closed = true
	c.err = err
	streams := make([]chan event, 0, len(c.scopes))
	for _, scope := range c.scopes {
		if scope.ch != nil {
			streams = append(streams, scope.ch)
			scope.ch = nil
		}
	}
	close(c.initCh)
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	_ = c.backend.Close()
	for _, ch := range streams {
		close(ch)
	}
}

func (c *connState) closeErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErrLocked()
}

func (c *connState) closeErrLocked() error {
	if c.err != nil {
		return c.err
	}
	return io.EOF
}

func parseLastEventID(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}
