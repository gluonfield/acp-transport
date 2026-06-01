package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
)

func IDKey(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", fmt.Errorf("%w: empty id", ErrInvalidMessage)
	}

	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	if err := ensureSingleJSONValue(dec); err != nil && err != io.EOF {
		return "", err
	}

	switch id := v.(type) {
	case nil:
		return "null", nil
	case string:
		b, err := json.Marshal(id)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case json.Number:
		return numberIDKey(id.String())
	default:
		return "", fmt.Errorf("%w: id must be string, number, or null", ErrInvalidMessage)
	}
}

func numberIDKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: empty numeric id", ErrInvalidMessage)
	}

	rat, ok := new(big.Rat).SetString(raw)
	if !ok {
		return "", fmt.Errorf("%w: invalid numeric id", ErrInvalidMessage)
	}
	if rat.Denom().Cmp(big.NewInt(1)) != 0 {
		return "", fmt.Errorf("%w: fractional numeric ids are not supported", ErrInvalidMessage)
	}
	return rat.Num().String(), nil
}
