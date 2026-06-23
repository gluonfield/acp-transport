package streamhttp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"github.com/gluonfield/acp-transport/internal/wire"
	"github.com/gluonfield/acp-transport/jsonrpc"
)

type Client struct {
	endpoint   string
	httpClient *http.Client
	token      string

	mu            sync.Mutex
	connID        string
	recv          chan *jsonrpc.Message
	done          chan struct{}
	closeOnce     sync.Once
	streamCancel  map[string]context.CancelFunc
	responseScope map[string]string
	lastEventID   map[string]uint64
}

type ClientOption func(*clientConfig) error

type clientConfig struct {
	httpClient *http.Client
	token      string
	h2c        bool
}

func Dial(endpoint string, opts ...ClientOption) (*Client, error) {
	cfg := clientConfig{}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, err
	}
	if cfg.httpClient == nil {
		cfg.httpClient = newHTTPClient(cfg.h2c)
	}
	return &Client{
		endpoint:      endpoint,
		httpClient:    cfg.httpClient,
		token:         cfg.token,
		recv:          make(chan *jsonrpc.Message, 256),
		done:          make(chan struct{}),
		streamCancel:  make(map[string]context.CancelFunc),
		responseScope: make(map[string]string),
		lastEventID:   make(map[string]uint64),
	}, nil
}

func WithHTTPClient(client *http.Client) ClientOption {
	return func(cfg *clientConfig) error {
		if client == nil {
			return errors.New("nil http client")
		}
		cfg.httpClient = client
		return nil
	}
}

func WithBearerToken(token string) ClientOption {
	return func(cfg *clientConfig) error {
		cfg.token = token
		return nil
	}
}

func WithH2C() ClientOption {
	return func(cfg *clientConfig) error {
		cfg.h2c = true
		return nil
	}
}

func (c *Client) Send(ctx context.Context, msg *jsonrpc.Message) error {
	if msg == nil {
		return jsonrpc.ErrInvalidMessage
	}
	if msg.IsRequest() && msg.Method == "initialize" && c.connectionID() == "" {
		return c.initialize(ctx, msg)
	}
	if c.connectionID() == "" {
		return errors.New("ACP connection is not initialized")
	}

	sessionID := jsonrpc.SessionIDFromMessage(msg)
	if msg.IsResponse() && msg.ID != nil {
		if scope := c.takeResponseScope(*msg.ID); scope != "" {
			sessionID = scope
		}
	}
	req, err := c.newPost(ctx, msg, sessionID)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return responseError(resp)
	}
	return nil
}

func (c *Client) Receive(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, io.EOF
	case msg, ok := <-c.recv:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		cancels := make([]context.CancelFunc, 0, len(c.streamCancel))
		for _, cancel := range c.streamCancel {
			cancels = append(cancels, cancel)
		}
		connID := c.connID
		c.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		if connID != "" {
			req, err := http.NewRequest(http.MethodDelete, c.endpoint, nil)
			if err == nil {
				req.Header.Set(HeaderConnectionID, connID)
				c.addAuth(req)
				resp, err := c.httpClient.Do(req)
				if err == nil {
					_ = resp.Body.Close()
				}
			}
		}
		close(c.done)
	})
	return nil
}

func (c *Client) initialize(ctx context.Context, msg *jsonrpc.Message) error {
	req, err := c.newPost(ctx, msg, "")
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	connID := resp.Header.Get(HeaderConnectionID)
	if connID == "" {
		return errors.New("initialize response missing Acp-Connection-Id")
	}
	initResp, err := readMessage(resp.Body, defaultMaxMessageBytes)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.connID = connID
	c.mu.Unlock()
	c.startStream("")
	c.enqueue(initResp)
	return nil
}

func (c *Client) newPost(ctx context.Context, msg *jsonrpc.Message, sessionID string) (*http.Request, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if connID := c.connectionID(); connID != "" {
		req.Header.Set(HeaderConnectionID, connID)
	}
	if sessionID != "" {
		req.Header.Set(HeaderSessionID, sessionID)
	}
	c.addAuth(req)
	return req, nil
}

func (c *Client) startStream(sessionID string) {
	c.mu.Lock()
	if _, ok := c.streamCancel[sessionID]; ok {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.streamCancel[sessionID] = cancel
	c.mu.Unlock()

	go c.streamLoop(ctx, sessionID)
}

func (c *Client) streamLoop(ctx context.Context, sessionID string) {
	for {
		err := c.runStream(ctx, sessionID)
		if err == nil || ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (c *Client) runStream(ctx context.Context, sessionID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set(HeaderConnectionID, c.connectionID())
	if sessionID != "" {
		req.Header.Set(HeaderSessionID, sessionID)
	}
	if lastID := c.streamLastEventID(sessionID); lastID != 0 {
		req.Header.Set("Last-Event-Id", fmt.Sprint(lastID))
	}
	req.Header.Set("Accept", "text/event-stream")
	c.addAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusGone {
			c.Close()
			return nil
		}
		return responseError(resp)
	}
	if mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || mediaType != "text/event-stream" {
		return errors.New("unexpected stream content type")
	}
	return c.readSSE(ctx, sessionID, resp.Body)
}

func (c *Client) readSSE(ctx context.Context, sessionID string, body io.Reader) error {
	reader := bufio.NewReaderSize(body, 64<<10)
	var data []byte
	var id string
	for {
		line, err := wire.ReadLine(reader, defaultMaxMessageBytes)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(line) == 0 {
			if len(data) > 0 {
				if err := c.handleSSEData(sessionID, id, data); err != nil {
					return err
				}
				data = nil
				id = ""
			}
			continue
		}
		if line[0] == ':' {
			continue
		}
		name, value, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			continue
		}
		switch string(name) {
		case "id":
			id = string(bytes.TrimPrefix(value, []byte(" ")))
			continue
		case "data":
		default:
			continue
		}
		value = bytes.TrimPrefix(value, []byte(" "))
		if len(data)+len(value) > defaultMaxMessageBytes {
			return fmt.Errorf("message exceeds %d bytes", defaultMaxMessageBytes)
		}
		data = append(data, value...)
	}
}

func (c *Client) handleSSEData(scope string, id string, data []byte) error {
	msg, err := jsonrpc.ParseMessage(data)
	if err != nil {
		return err
	}
	if id != "" {
		lastID, err := parseLastEventID(id)
		if err != nil {
			return err
		}
		c.setStreamLastEventID(scope, lastID)
	}
	if msg.IsRequest() && msg.ID != nil {
		c.rememberResponseScope(*msg.ID, scope)
	}
	if scope == "" {
		if sessionID := jsonrpc.SessionIDFromMessage(msg); sessionID != "" {
			c.startStream(sessionID)
		}
	}
	c.enqueue(msg)
	return nil
}

func (c *Client) enqueue(msg *jsonrpc.Message) {
	select {
	case c.recv <- msg.Clone():
	case <-c.done:
	}
}

func (c *Client) addAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

func (c *Client) connectionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connID
}

func (c *Client) rememberResponseScope(id json.RawMessage, sessionID string) {
	key, err := jsonrpc.IDKey(id)
	if err != nil {
		return
	}
	c.mu.Lock()
	c.responseScope[key] = sessionID
	c.mu.Unlock()
}

func (c *Client) takeResponseScope(id json.RawMessage) string {
	key, err := jsonrpc.IDKey(id)
	if err != nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	sessionID := c.responseScope[key]
	delete(c.responseScope, key)
	return sessionID
}

func (c *Client) streamLastEventID(sessionID string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastEventID[sessionID]
}

func (c *Client) setStreamLastEventID(sessionID string, id uint64) {
	c.mu.Lock()
	if id > c.lastEventID[sessionID] {
		c.lastEventID[sessionID] = id
	}
	c.mu.Unlock()
}

func responseError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("unexpected HTTP status %s: %s", resp.Status, strings.TrimSpace(string(body)))
}

func newHTTPClient(h2c bool) *http.Client {
	jar, _ := cookiejar.New(nil)
	if h2c {
		return &http.Client{
			Jar: jar,
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					var dialer net.Dialer
					return dialer.DialContext(ctx, network, addr)
				},
			},
		}
	}
	return &http.Client{
		Jar:       jar,
		Transport: &http2.Transport{},
	}
}
