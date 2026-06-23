package stdio

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// gatedReader serves chunks in order, but blocks the read that would serve
// chunks[gateBefore] until gate is closed, closing atGate once it is blocked.
// That pins readLoop at a precise point: every chunk before gateBefore is
// parsed and buffered, then readLoop is held just before the next read. A
// gateBefore at or past len(chunks) holds readLoop after consuming everything.
type gatedReader struct {
	chunks     [][]byte
	gateBefore int
	idx        int
	atGate     chan struct{}
	gate       chan struct{}
	once       sync.Once
}

func newGatedReader(gateBefore int, chunks ...[]byte) *gatedReader {
	return &gatedReader{
		chunks:     chunks,
		gateBefore: gateBefore,
		atGate:     make(chan struct{}),
		gate:       make(chan struct{}),
	}
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.idx == r.gateBefore {
		r.once.Do(func() { close(r.atGate) })
		<-r.gate
	}
	if r.idx >= len(r.chunks) {
		<-r.gate // nothing left to serve; block instead of returning EOF
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.idx])
	r.idx++
	return n, nil
}

// TestCloseDuringFreshReadDoesNotPanic is a regression test for a "send on
// closed channel" panic in readLoop. closeWith must not close c.msgs while
// readLoop is still a live sender on it: if Close() ran while readLoop was
// about to (re)evaluate its "case c.msgs <- msg" select arm, both that arm and
// the c.done arm were ready at once and the runtime sometimes picked the send,
// panicking on the closed channel. This drove Jaz crashes when a Codex session
// was torn down mid-turn while its subprocess kept emitting on stdout.
//
// Each trial positions readLoop to arrive at the select AFTER Close(), which is
// the dangerous interleaving. On the buggy code a trial panics ~50% of the
// time, so running many sequentially crashes the binary almost immediately; on
// the fixed code every trial completes cleanly.
func TestCloseDuringFreshReadDoesNotPanic(t *testing.T) {
	a := []byte(`{"jsonrpc":"2.0","method":"a"}` + "\n")
	b := []byte(`{"jsonrpc":"2.0","method":"b"}` + "\n")

	for i := 0; i < 200; i++ {
		r := newGatedReader(1, a, b)
		c := New(r, io.Discard)

		<-r.atGate    // readLoop buffered "a" and is blocked reading "b"
		c.Close()     // closes c.done (and, on the buggy code, c.msgs)
		close(r.gate) // release "b": readLoop now reaches the select post-close

		// Give the released readLoop a moment to hit the select and either
		// panic (old code) or return cleanly (fixed code).
		time.Sleep(time.Millisecond)
		_ = c.Close()
	}
}

// TestReceiveDrainsBufferedThenReportsClose verifies the fixed Receive still
// hands back a message that was buffered before Close() before reporting the
// connection as closed.
func TestReceiveDrainsBufferedThenReportsClose(t *testing.T) {
	a := []byte(`{"jsonrpc":"2.0","method":"a"}` + "\n")
	r := newGatedReader(1, a)
	c := New(r, io.Discard)
	defer c.Close()
	t.Cleanup(func() { close(r.gate) }) // release readLoop's blocked read

	<-r.atGate // "a" is buffered; readLoop is blocked reading the next line
	c.Close()

	ctx := context.Background()
	msg, err := c.Receive(ctx)
	if err != nil {
		t.Fatalf("first Receive after close: want buffered message, got err %v", err)
	}
	if msg == nil || msg.Method != "a" {
		t.Fatalf("first Receive after close: want method %q, got %+v", "a", msg)
	}

	if _, err := c.Receive(ctx); err == nil {
		t.Fatal("second Receive after close: want closure error, got nil")
	}
}
