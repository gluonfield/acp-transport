package jsonrpc

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrClosed = errors.New("json-rpc connection closed")

// IsClosed reports whether err signals an ordinary transport shutdown — a local
// Close (ErrClosed), the peer ending the stream (io.EOF), or context
// cancellation — rather than a genuine fault. It is the canonical predicate for
// "this connection is just done"; prefer it over ad-hoc errors.Is chains.
func IsClosed(err error) bool {
	return errors.Is(err, ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, context.Canceled)
}

type MessageConn interface {
	Send(context.Context, *Message) error
	Receive(context.Context) (*Message, error)
	Close() error
}

type channelConn struct {
	in       <-chan *Message
	out      chan<- *Message
	close    func()
	done     <-chan struct{}
	closeMux sync.Once
}

func NewChannelConn(in <-chan *Message, out chan<- *Message, closeFn func()) MessageConn {
	done := make(chan struct{})
	if closeFn == nil {
		closeFn = func() { close(done) }
	} else {
		orig := closeFn
		var once sync.Once
		closeFn = func() {
			once.Do(func() {
				orig()
				close(done)
			})
		}
	}
	return &channelConn{in: in, out: out, close: closeFn, done: done}
}

func Pipe(buffer int) (MessageConn, MessageConn) {
	aToB := make(chan *Message, buffer)
	bToA := make(chan *Message, buffer)
	done := make(chan struct{})
	var once sync.Once
	closeAll := func() {
		once.Do(func() {
			close(done)
		})
	}
	a := &channelConn{in: bToA, out: aToB, close: closeAll, done: done}
	b := &channelConn{in: aToB, out: bToA, close: closeAll, done: done}
	return a, b
}

func (c *channelConn) Send(ctx context.Context, msg *Message) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	case c.out <- msg.Clone():
		return nil
	}
}

func (c *channelConn) Receive(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClosed // local close; matches stdio.Conn
	case msg, ok := <-c.in:
		if !ok {
			return nil, io.EOF // peer ended the stream
		}
		return msg.Clone(), nil
	}
}

func (c *channelConn) Close() error {
	c.closeMux.Do(c.close)
	return nil
}
