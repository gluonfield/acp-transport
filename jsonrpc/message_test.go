package jsonrpc

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestIDKeyCanonicalizesNumbersAndStrings(t *testing.T) {
	tests := map[string]string{
		`1`:        "1",
		`1.0`:      "1",
		`1e0`:      "1",
		`"a"`:      `"a"`,
		`"\u0061"`: `"a"`,
		`null`:     "null",
	}
	for raw, want := range tests {
		got, err := IDKey(json.RawMessage(raw))
		if err != nil {
			t.Fatalf("IDKey(%s): %v", raw, err)
		}
		if got != want {
			t.Fatalf("IDKey(%s) = %s, want %s", raw, got, want)
		}
	}
}

func TestParseMessageRejectsBatch(t *testing.T) {
	_, err := ParseMessage([]byte(`[{"jsonrpc":"2.0","method":"x"}]`))
	if !errors.Is(err, ErrBatchRequest) {
		t.Fatalf("ParseMessage batch error = %v, want %v", err, ErrBatchRequest)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	msg, err := NewRequest(json.RawMessage(`"abc"`), "session/new", map[string]any{"cwd": "/tmp"})
	if err != nil {
		t.Fatal(err)
	}
	line, err := msg.MarshalJSONLine()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseMessage(line)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "session/new" || got.ID == nil {
		t.Fatalf("unexpected message: %#v", got)
	}
}
