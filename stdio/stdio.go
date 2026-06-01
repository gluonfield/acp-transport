package stdio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

const maxMessageBytes = 10 << 20

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
	case msg, ok := <-c.msgs:
		if !ok {
			return nil, c.closeErr()
		}
		return msg, nil
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
	scanner := bufio.NewScanner(c.in)
	scanner.Buffer(make([]byte, 0, 64<<10), maxMessageBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
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
	if err := scanner.Err(); err != nil {
		c.closeWith(err)
		return
	}
	c.closeWith(io.EOF)
}

func (c *Conn) closeWith(err error) {
	if err == nil {
		err = io.EOF
	}
	c.once.Do(func() {
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		close(c.done)
		close(c.msgs)
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
