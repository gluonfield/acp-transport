package stdio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/gluonfield/acp-transport/internal/wire"
	"github.com/gluonfield/acp-transport/jsonrpc"
)

type Conn struct {
	in  io.Reader
	out io.Writer

	writeMu sync.Mutex
	msgs    chan *jsonrpc.Message
	done    chan struct{}
	once    sync.Once
	errMu   sync.Mutex
	err     error
}

// New starts a Conn that reads newline-delimited JSON-RPC messages from in and
// writes them to out. Close interrupts a read blocked in progress only if in is
// an io.Closer (e.g. an *os.File pipe); with a reader that cannot be closed, the
// internal read loop may stay blocked until in reaches EOF on its own.
func New(in io.Reader, out io.Writer) *Conn {
	c := &Conn{
		in:   in,
		out:  out,
		msgs: make(chan *jsonrpc.Message, 64),
		done: make(chan struct{}),
	}
	go c.readLoop()
	return c
}

func (c *Conn) Send(ctx context.Context, msg *jsonrpc.Message) error {
	line, err := msg.MarshalJSONLine()
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.closeErr()
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.out.Write(line)
	if err != nil {
		c.closeWith(err)
	}
	return err
}

func (c *Conn) Receive(ctx context.Context) (*jsonrpc.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg := <-c.msgs:
		return msg, nil
	case <-c.done:
		// Closed. Deliver any message buffered before close, then report closure.
		select {
		case msg := <-c.msgs:
			return msg, nil
		default:
			return nil, c.closeErr()
		}
	}
}

func (c *Conn) Close() error {
	c.closeWith(io.ErrClosedPipe)
	if closer, ok := c.in.(io.Closer); ok {
		_ = closer.Close()
	}
	if closer, ok := c.out.(io.Closer); ok {
		_ = closer.Close()
	}
	return nil
}

func (c *Conn) readLoop() {
	reader := bufio.NewReaderSize(c.in, 64<<10)
	for {
		raw, err := wire.ReadLine(reader, wire.MaxMessageBytes)
		if err != nil {
			c.closeWith(err)
			return
		}
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		msg, err := jsonrpc.ParseMessage(line)
		if err != nil {
			c.closeWith(err)
			return
		}
		select {
		case c.msgs <- msg:
		case <-c.done:
			return
		}
	}
}

func (c *Conn) closeWith(err error) {
	if err == nil {
		err = io.EOF
	}
	c.once.Do(func() {
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		// Only close done. readLoop is a live sender on c.msgs, so closing
		// c.msgs here would let readLoop's "case c.msgs <- msg" select arm
		// panic with "send on closed channel" when it arrives at the select
		// after close. readLoop observes shutdown via c.done instead.
		close(c.done)
	})
}

func (c *Conn) closeErr() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()
	if c.err == nil {
		return io.EOF
	}
	if errors.Is(c.err, io.ErrClosedPipe) {
		return jsonrpc.ErrClosed
	}
	return c.err
}
