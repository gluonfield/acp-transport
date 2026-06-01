package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const Version = "2.0"

var (
	ErrInvalidMessage = errors.New("invalid json-rpc message")
	ErrBatchRequest   = errors.New("json-rpc batch requests are not supported")
)

type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message)
}

type Message struct {
	JSONRPC string           `json:"jsonrpc,omitempty"`
	ID      *json.RawMessage `json:"id,omitempty"`
	Method  string           `json:"method,omitempty"`
	Params  json.RawMessage  `json:"params,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *Error           `json:"error,omitempty"`
}

func ParseMessage(b []byte) (*Message, error) {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return nil, io.EOF
	}
	if trimmed[0] == '[' {
		return nil, ErrBatchRequest
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var msg Message
	if err := dec.Decode(&msg); err != nil {
		return nil, err
	}
	if err := ensureSingleJSONValue(dec); err != nil {
		return nil, err
	}
	if err := msg.Validate(); err != nil {
		return nil, err
	}
	return &msg, nil
}

func ensureSingleJSONValue(dec *json.Decoder) error {
	var trailing any
	if err := dec.Decode(&trailing); err == nil {
		return fmt.Errorf("%w: trailing data", ErrInvalidMessage)
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func NewRequest(id json.RawMessage, method string, params any) (*Message, error) {
	msg := &Message{
		JSONRPC: Version,
		ID:      cloneRawPtr(id),
		Method:  method,
	}
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		msg.Params = b
	}
	return msg, msg.Validate()
}

func NewNotification(method string, params any) (*Message, error) {
	msg := &Message{
		JSONRPC: Version,
		Method:  method,
	}
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return nil, err
		}
		msg.Params = b
	}
	return msg, msg.Validate()
}

func NewResult(id json.RawMessage, result any) (*Message, error) {
	msg := &Message{
		JSONRPC: Version,
		ID:      cloneRawPtr(id),
	}
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		msg.Result = b
	} else {
		msg.Result = []byte("null")
	}
	return msg, msg.Validate()
}

func NewRawResult(id json.RawMessage, result json.RawMessage) (*Message, error) {
	msg := &Message{
		JSONRPC: Version,
		ID:      cloneRawPtr(id),
		Result:  cloneRaw(result),
	}
	return msg, msg.Validate()
}

func NewErrorResponse(id json.RawMessage, rpcErr *Error) (*Message, error) {
	if rpcErr == nil {
		rpcErr = InternalError("internal error", nil)
	}
	msg := &Message{
		JSONRPC: Version,
		ID:      cloneRawPtr(id),
		Error:   rpcErr,
	}
	return msg, msg.Validate()
}

func (m *Message) Validate() error {
	if m == nil {
		return fmt.Errorf("%w: nil message", ErrInvalidMessage)
	}
	if m.JSONRPC != "" && m.JSONRPC != Version {
		return fmt.Errorf("%w: jsonrpc must be %q", ErrInvalidMessage, Version)
	}
	hasMethod := m.Method != ""
	hasID := m.ID != nil
	hasResult := len(bytes.TrimSpace(m.Result)) > 0
	hasError := m.Error != nil

	switch {
	case hasMethod:
		if hasResult || hasError {
			return fmt.Errorf("%w: request/notification cannot contain result or error", ErrInvalidMessage)
		}
		if hasID {
			if _, err := IDKey(*m.ID); err != nil {
				return err
			}
		}
	case hasID:
		if hasResult == hasError {
			return fmt.Errorf("%w: response must contain exactly one of result or error", ErrInvalidMessage)
		}
	default:
		return fmt.Errorf("%w: message must be request, notification, or response", ErrInvalidMessage)
	}

	return nil
}

func (m *Message) IsRequest() bool {
	return m != nil && m.Method != "" && m.ID != nil
}

func (m *Message) IsNotification() bool {
	return m != nil && m.Method != "" && m.ID == nil
}

func (m *Message) IsResponse() bool {
	return m != nil && m.Method == "" && m.ID != nil
}

func (m *Message) Clone() *Message {
	if m == nil {
		return nil
	}
	cp := *m
	if m.ID != nil {
		cp.ID = cloneRawPtr(*m.ID)
	}
	cp.Params = cloneRaw(m.Params)
	cp.Result = cloneRaw(m.Result)
	if m.Error != nil {
		errCopy := *m.Error
		errCopy.Data = cloneRaw(m.Error.Data)
		cp.Error = &errCopy
	}
	return &cp
}

func (m *Message) MarshalJSONLine() ([]byte, error) {
	cp := m.Clone()
	if cp.JSONRPC == "" {
		cp.JSONRPC = Version
	}
	if err := cp.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(cp)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func InvalidRequest(message string, data any) *Error {
	return newError(-32600, message, data)
}

func MethodNotFound(method string) *Error {
	return newError(-32601, "method not found", map[string]any{"method": method})
}

func InvalidParams(message string, data any) *Error {
	return newError(-32602, message, data)
}

func InternalError(message string, data any) *Error {
	return newError(-32603, message, data)
}

func newError(code int, message string, data any) *Error {
	e := &Error{Code: code, Message: message}
	if data != nil {
		if b, err := json.Marshal(data); err == nil {
			e.Data = b
		}
	}
	return e
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneRawPtr(raw json.RawMessage) *json.RawMessage {
	cp := cloneRaw(raw)
	return &cp
}
