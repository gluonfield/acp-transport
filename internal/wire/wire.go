// Package wire holds low-level newline-framing helpers shared by the stdio and
// streamhttp transports, so the line-reading logic lives in exactly one place.
package wire

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// MaxMessageBytes is the default ceiling for a single framed message.
const MaxMessageBytes = 64 << 20

// ReadLine reads one newline-delimited frame from reader and returns it with the
// trailing CR/LF removed. It returns an error if the frame exceeds max bytes.
func ReadLine(reader *bufio.Reader, max int) ([]byte, error) {
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
