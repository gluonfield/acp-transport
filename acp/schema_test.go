package acp

import (
	"encoding/json"
	"os"
	"testing"
)

func TestGeneratedMethodsMatchMeta(t *testing.T) {
	b, err := os.ReadFile("../testdata/acp-schema/meta.json")
	if err != nil {
		t.Fatal(err)
	}
	var meta struct {
		Version       int               `json:"version"`
		AgentMethods  map[string]string `json:"agentMethods"`
		ClientMethods map[string]string `json:"clientMethods"`
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Version != ProtocolVersionNumber {
		t.Fatalf("ProtocolVersionNumber = %d, want %d", ProtocolVersionNumber, meta.Version)
	}
	if AgentMethodInitialize != meta.AgentMethods["initialize"] {
		t.Fatalf("AgentMethodInitialize = %q", AgentMethodInitialize)
	}
	if AgentMethodSessionPrompt != meta.AgentMethods["session_prompt"] {
		t.Fatalf("AgentMethodSessionPrompt = %q", AgentMethodSessionPrompt)
	}
	if ClientMethodSessionUpdate != meta.ClientMethods["session_update"] {
		t.Fatalf("ClientMethodSessionUpdate = %q", ClientMethodSessionUpdate)
	}
}

func TestGeneratedTypesMarshal(t *testing.T) {
	req := InitializeRequest{ProtocolVersion: ProtocolVersionNumber}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"protocolVersion":1}` {
		t.Fatalf("InitializeRequest JSON = %s", b)
	}
}
