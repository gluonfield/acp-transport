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

	rawMessage := json.RawMessage(`{
		"sessionUpdate": "agent_message_chunk",
		"messageId": "message-1",
		"content": {"type": "text", "text": "hello"}
	}`)
	decoded, err = DecodeSessionUpdate(rawMessage)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := decoded.(AgentMessageChunkUpdate)
	if !ok {
		t.Fatalf("decoded type = %T, want AgentMessageChunkUpdate", decoded)
	}
	if message.MessageID != "message-1" {
		t.Fatalf("messageId = %q, want message-1", message.MessageID)
	}
}

func TestDecodeContentChunkRequiresMessageID(t *testing.T) {
	tests := []SessionUpdateKind{
		SessionUpdateUserMessageChunk,
		SessionUpdateAgentMessageChunk,
		SessionUpdateAgentThoughtChunk,
	}
	for _, kind := range tests {
		t.Run(string(kind), func(t *testing.T) {
			raw := json.RawMessage(`{
				"sessionUpdate": "` + string(kind) + `",
				"content": {"type": "text", "text": "hello"}
			}`)
			if _, err := DecodeSessionUpdate(raw); err == nil {
				t.Fatal("DecodeSessionUpdate succeeded without messageId")
			}
		})
	}
}

func TestDecodeRequestPermissionToolCallContent(t *testing.T) {
	raw := json.RawMessage(`{
		"sessionId": "session-1",
		"toolCall": {
			"toolCallId": "request-user-input-call-1",
			"kind": "other",
			"status": "pending",
			"title": "Clarifying questions",
			"content": [
				{
					"type": "content",
					"content": {
						"type": "text",
						"text": "1. What should this page be for?"
					}
				}
			],
			"rawInput": {
				"call_id": "call-1"
			},
			"_meta": {
				"jaz.codex_request_user_input": {
					"call_id": "call-1",
					"turn_id": "turn-1",
					"questions": [
						{
							"id": "audience",
							"question": "Who is the audience?",
							"options": [
								{"label": "kids", "description": "Use simpler copy."}
							]
						}
					]
				}
			}
		},
		"options": [
			{"optionId": "__jaz_user_input_submit__", "name": "Submit answers", "kind": "allow_once"},
			{"optionId": "__jaz_user_input_cancel__", "name": "Cancel", "kind": "reject_once"}
		]
	}`)
	var req RequestPermissionRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.SessionID != "session-1" {
		t.Fatalf("session id = %q", req.SessionID)
	}
	if len(req.ToolCall.Content) != 1 {
		t.Fatalf("tool call content length = %d, want 1", len(req.ToolCall.Content))
	}
	if string(req.ToolCall.Content[0]) == "" {
		t.Fatal("tool call content was not preserved")
	}
}

func TestDecodeCreateElicitationResponseContent(t *testing.T) {
	raw := json.RawMessage(`{
		"action": "accept",
		"content": {
			"name": "Ada",
			"count": 3,
			"choices": ["fast", "safe"]
		}
	}`)
	var resp CreateElicitationResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Action != "accept" {
		t.Fatalf("action = %q", resp.Action)
	}
	if string(resp.Content["name"]) != `"Ada"` {
		t.Fatalf("content[name] = %s", resp.Content["name"])
	}
}

func TestDecodeCreateElicitationRequestMode(t *testing.T) {
	raw := json.RawMessage(`{
		"mode": "form",
		"sessionId": "session-1",
		"message": "Need input",
		"requestedSchema": {
			"type": "object",
			"properties": {
				"name": {"type": "string", "title": "Name"}
			}
		}
	}`)
	var req CreateElicitationRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Mode != "form" || req.SessionID == nil || *req.SessionID != "session-1" {
		t.Fatalf("request = %#v", req)
	}
	if req.RequestedSchema == nil || req.RequestedSchema.Properties["name"].Type != "string" {
		t.Fatalf("schema = %#v", req.RequestedSchema)
	}

	raw = json.RawMessage(`{
		"mode": "url",
		"message": "Complete sign in",
		"elicitationId": "auth-1",
		"url": "https://example.test/auth"
	}`)
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	if req.Mode != "url" || req.ElicitationID == nil || *req.ElicitationID != "auth-1" || req.URL != "https://example.test/auth" {
		t.Fatalf("url request = %#v", req)
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
