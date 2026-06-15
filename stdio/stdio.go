package stdio

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gluonfield/acp-transport/jsonrpc"
)

const maxMessageBytes = 64 << 20

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
	reader := bufio.NewReaderSize(c.in, 64<<10)
	for {
		raw, err := readLine(reader, maxMessageBytes)
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

func readLine(reader *bufio.Reader, max int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > max {
			return nil, fmt.Errorf("message exceeds %d bytes", max)
		}
		if err == nil {
			return trimLineEnd(line), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return trimLineEnd(line), nil
		}
		return nil, err
	}
}

func trimLineEnd(line []byte) []byte {
	line = bytes.TrimSuffix(line, []byte{'\n'})
	return bytes.TrimSuffix(line, []byte{'\r'})
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
