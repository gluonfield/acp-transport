package streamhttp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

const (
	HeaderConnectionID = "Acp-Connection-Id"
	HeaderSessionID    = "Acp-Session-Id"

	defaultMaxMessageBytes = 10 << 20
	defaultMaxQueuedEvents = 1024
	defaultSSEBuffer       = 64
)

type BackendFactory func(context.Context) (jsonrpc.MessageConn, error)

type Server struct {
	Backend         BackendFactory
	Token           string
	MaxMessageBytes int64
	MaxQueuedEvents int

	mu          sync.Mutex
	connections map[string]*connState
}

func (s *Server) Close() error {
	s.mu.Lock()
	conns := make([]*connState, 0, len(s.connections))
	for id, c := range s.connections {
		conns = append(conns, c)
		delete(s.connections, id)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.close()
	}
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor < 2 {
		http.Error(w, "streamable ACP requires HTTP/2", http.StatusHTTPVersionNotSupported)
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="acp"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodPost:
		s.handlePost(w, r)
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r.Header.Get("Content-Type")) {
		http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	msg, err := readMessage(r.Body, s.maxMessageBytes())
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, jsonrpc.ErrBatchRequest) {
			status = http.StatusNotImplemented
		}
		http.Error(w, err.Error(), status)
		return
	}

	connID := r.Header.Get(HeaderConnectionID)
	if connID == "" {
		s.handleInitialize(w, r, msg)
		return
	}

	c := s.connection(connID)
	if c == nil {
		http.Error(w, "unknown ACP connection", http.StatusNotFound)
		return
	}

	sessionID := r.Header.Get(HeaderSessionID)
	bodySessionID := jsonrpc.SessionIDFromMessage(msg)
	if bodySessionID != "" && sessionID == "" {
		http.Error(w, "session-scoped requests require Acp-Session-Id", http.StatusBadRequest)
		return
	}
	if sessionID != "" && !c.hasSession(sessionID) {
		http.Error(w, "unknown ACP session", http.StatusNotFound)
		return
	}
	if bodySessionID != "" && sessionID != bodySessionID {
		http.Error(w, "Acp-Session-Id does not match JSON-RPC sessionId", http.StatusBadRequest)
		return
	}

	if err := c.sendToBackend(r.Context(), msg, sessionID); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleInitialize(w http.ResponseWriter, r *http.Request, msg *jsonrpc.Message) {
	if !msg.IsRequest() || msg.Method != "initialize" {
		http.Error(w, "first request must be initialize", http.StatusBadRequest)
		return
	}
	if s.Backend == nil {
		http.Error(w, "backend is not configured", http.StatusInternalServerError)
		return
	}

	connCtx, cancel := context.WithCancel(context.Background())
	backend, err := s.Backend(connCtx)
	if err != nil {
		cancel()
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	connID, err := newID()
	if err != nil {
		cancel()
		_ = backend.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c := newConnState(connID, backend, cancel, s.maxQueuedEvents())
	if err := c.setInit(msg); err != nil {
		cancel()
		_ = backend.Close()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.putConnection(connID, c)
	go func() {
		c.run()
		s.deleteConnection(connID)
	}()

	if err := c.backend.Send(r.Context(), msg); err != nil {
		c.close()
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp, err := c.waitInit(r.Context())
	if err != nil {
		c.close()
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set(HeaderConnectionID, connID)
	http.SetCookie(w, &http.Cookie{Name: "acp_connection_id", Value: connID, Path: "/acp", HttpOnly: true, SameSite: http.SameSiteLaxMode})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if !acceptsSSE(r.Header.Get("Accept")) {
		http.Error(w, "accept must include text/event-stream", http.StatusNotAcceptable)
		return
	}
	connID := r.Header.Get(HeaderConnectionID)
	if connID == "" {
		http.Error(w, "missing Acp-Connection-Id", http.StatusBadRequest)
		return
	}
	c := s.connection(connID)
	if c == nil {
		http.Error(w, "unknown ACP connection", http.StatusNotFound)
		return
	}

	sessionID := r.Header.Get(HeaderSessionID)
	if sessionID != "" && !c.hasSession(sessionID) {
		http.Error(w, "unknown ACP session", http.StatusNotFound)
		return
	}
	lastID, err := parseLastEventID(r.Header.Get("Last-Event-Id"))
	if err != nil {
		http.Error(w, "invalid Last-Event-Id", http.StatusBadRequest)
		return
	}
	ch, detach, err := c.attachStream(sessionID, lastID)
	if err != nil {
		if err == ErrEventHistoryGone {
			http.Error(w, err.Error(), http.StatusGone)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer detach()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			if _, err := fmt.Fprintf(w, "id: %d\nevent: message\ndata: %s\n\n", ev.id, ev.data); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	connID := r.Header.Get(HeaderConnectionID)
	if connID == "" {
		http.Error(w, "missing Acp-Connection-Id", http.StatusBadRequest)
		return
	}
	c := s.connection(connID)
	if c == nil {
		http.Error(w, "unknown ACP connection", http.StatusNotFound)
		return
	}
	c.close()
	s.deleteConnection(connID)
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) authorized(r *http.Request) bool {
	if s.Token == "" {
		return true
	}
	return r.Header.Get("Authorization") == "Bearer "+s.Token
}

func (s *Server) connection(id string) *connState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections[id]
}

func (s *Server) putConnection(id string, c *connState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connections == nil {
		s.connections = make(map[string]*connState)
	}
	s.connections[id] = c
}

func (s *Server) deleteConnection(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connections, id)
}

func (s *Server) maxMessageBytes() int64 {
	if s.MaxMessageBytes > 0 {
		return s.MaxMessageBytes
	}
	return defaultMaxMessageBytes
}

func (s *Server) maxQueuedEvents() int {
	if s.MaxQueuedEvents > 0 {
		return s.MaxQueuedEvents
	}
	return defaultMaxQueuedEvents
}

func readMessage(r io.Reader, max int64) (*jsonrpc.Message, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("message exceeds %d bytes", max)
	}
	return jsonrpc.ParseMessage(b)
}

func isJSON(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}

func acceptsSSE(value string) bool {
	for part := range strings.SplitSeq(value, ",") {
		mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err == nil && mediaType == "text/event-stream" {
			return true
		}
	}
	return false
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
