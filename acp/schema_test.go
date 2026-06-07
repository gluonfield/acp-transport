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

func TestGeneratedEnumsCoverSchemaOneOfConstants(t *testing.T) {
	if PlanEntryStatusInProgress != "in_progress" {
		t.Fatalf("PlanEntryStatusInProgress = %q", PlanEntryStatusInProgress)
	}
	if PlanEntryPriorityHigh != "high" {
		t.Fatalf("PlanEntryPriorityHigh = %q", PlanEntryPriorityHigh)
	}
	if PermissionOptionKindAllowOnce != "allow_once" {
		t.Fatalf("PermissionOptionKindAllowOnce = %q", PermissionOptionKindAllowOnce)
	}
	if ToolCallStatusCompleted != "completed" {
		t.Fatalf("ToolCallStatusCompleted = %q", ToolCallStatusCompleted)
	}
	if ToolKindSwitchMode != "switch_mode" {
		t.Fatalf("ToolKindSwitchMode = %q", ToolKindSwitchMode)
	}
	if StopReasonCancelled != "cancelled" {
		t.Fatalf("StopReasonCancelled = %q", StopReasonCancelled)
	}
}

func TestDecodeSessionUpdate(t *testing.T) {
	raw := json.RawMessage(`{
		"sessionUpdate": "plan",
		"entries": [
			{"content": "Read code", "priority": "high", "status": "completed"},
			{"content": "Patch UI", "priority": "medium", "status": "in_progress"}
		]
	}`)
	decoded, err := DecodeSessionUpdate(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, ok := decoded.(PlanSessionUpdate)
	if !ok {
		t.Fatalf("decoded type = %T, want PlanSessionUpdate", decoded)
	}
	if plan.SessionUpdateKind() != SessionUpdatePlan {
		t.Fatalf("kind = %q", plan.SessionUpdateKind())
	}
	if len(plan.Entries) != 2 || plan.Entries[1].Status != PlanEntryStatusInProgress {
		t.Fatalf("unexpected entries %#v", plan.Entries)
	}

	unknown, err := DecodeSessionUpdate(json.RawMessage(`{"sessionUpdate":"future_update","value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unknown.(UnknownSessionUpdate); !ok {
		t.Fatalf("unknown decoded as %T", unknown)
	}
}

func TestDecodeTextContentBlock(t *testing.T) {
	block, err := DecodeContentBlock(ContentBlock(`{"type":"text","text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	text, ok := block.(TextContentBlock)
	if !ok {
		t.Fatalf("decoded type = %T, want TextContentBlock", block)
	}
	if text.Text != "hello" || text.ContentKind() != ContentBlockText {
		t.Fatalf("unexpected text block %#v", text)
	}
}
