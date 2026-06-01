package jsonrpc

import (
	"encoding/json"
)

func SessionIDFromMessage(msg *Message) string {
	if msg == nil {
		return ""
	}
	for _, raw := range []json.RawMessage{msg.Params, msg.Result} {
		if id := sessionIDFromRaw(raw); id != "" {
			return id
		}
	}
	return ""
}

func sessionIDFromRaw(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var envelope struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	return envelope.SessionID
}
