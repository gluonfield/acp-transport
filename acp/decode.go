package acp

import (
	"encoding/json"
	"fmt"
)

type SessionUpdateKind string

const (
	SessionUpdateUserMessageChunk  SessionUpdateKind = "user_message_chunk"
	SessionUpdateAgentMessageChunk SessionUpdateKind = "agent_message_chunk"
	SessionUpdateAgentThoughtChunk SessionUpdateKind = "agent_thought_chunk"
	SessionUpdateToolCall          SessionUpdateKind = "tool_call"
	SessionUpdateToolCallUpdate    SessionUpdateKind = "tool_call_update"
	SessionUpdatePlan              SessionUpdateKind = "plan"
	SessionUpdateAvailableCommands SessionUpdateKind = "available_commands_update"
	SessionUpdateCurrentMode       SessionUpdateKind = "current_mode_update"
	SessionUpdateConfigOption      SessionUpdateKind = "config_option_update"
	SessionUpdateSessionInfo       SessionUpdateKind = "session_info_update"
)

type DecodedSessionUpdate interface {
	SessionUpdateKind() SessionUpdateKind
	RawJSON() json.RawMessage
}

type sessionUpdateBase struct {
	SessionUpdate SessionUpdateKind `json:"sessionUpdate"`
	raw           json.RawMessage
}

func (u sessionUpdateBase) SessionUpdateKind() SessionUpdateKind {
	return u.SessionUpdate
}
func (u sessionUpdateBase) RawJSON() json.RawMessage { return append(json.RawMessage(nil), u.raw...) }

type UserMessageChunkUpdate struct {
	sessionUpdateBase
	ContentChunk
}

type AgentMessageChunkUpdate struct {
	sessionUpdateBase
	ContentChunk
}

type AgentThoughtChunkUpdate struct {
	sessionUpdateBase
	ContentChunk
}

type ToolCallSessionUpdate struct {
	sessionUpdateBase
	ToolCall
}

type ToolCallUpdateSessionUpdate struct {
	sessionUpdateBase
	ToolCallUpdate
}

type PlanSessionUpdate struct {
	sessionUpdateBase
	Plan
}

type AvailableCommandsSessionUpdate struct {
	sessionUpdateBase
	AvailableCommandsUpdate
}

type CurrentModeSessionUpdate struct {
	sessionUpdateBase
	CurrentModeUpdate
}

type ConfigOptionSessionUpdate struct {
	sessionUpdateBase
	ConfigOptionUpdate
}

type SessionInfoSessionUpdate struct {
	sessionUpdateBase
	SessionInfoUpdate
}

type UnknownSessionUpdate struct {
	sessionUpdateBase
}

func DecodeSessionUpdate(raw json.RawMessage) (DecodedSessionUpdate, error) {
	var env struct {
		SessionUpdate SessionUpdateKind `json:"sessionUpdate"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	base := sessionUpdateBase{SessionUpdate: env.SessionUpdate, raw: append(json.RawMessage(nil), raw...)}
	switch env.SessionUpdate {
	case SessionUpdateUserMessageChunk:
		var out UserMessageChunkUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateAgentMessageChunk:
		var out AgentMessageChunkUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateAgentThoughtChunk:
		var out AgentThoughtChunkUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateToolCall:
		var out ToolCallSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateToolCallUpdate:
		var out ToolCallUpdateSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdatePlan:
		var out PlanSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateAvailableCommands:
		var out AvailableCommandsSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateCurrentMode:
		var out CurrentModeSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateConfigOption:
		var out ConfigOptionSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	case SessionUpdateSessionInfo:
		var out SessionInfoSessionUpdate
		if err := decodeSessionUpdate(raw, base, &out, &out.sessionUpdateBase); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return UnknownSessionUpdate{sessionUpdateBase: base}, nil
	}
}

func decodeSessionUpdate(raw json.RawMessage, base sessionUpdateBase, out any, targetBase *sessionUpdateBase) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return err
	}
	*targetBase = base
	return nil
}

type ContentBlockKind string

const (
	ContentBlockText         ContentBlockKind = "text"
	ContentBlockImage        ContentBlockKind = "image"
	ContentBlockAudio        ContentBlockKind = "audio"
	ContentBlockResourceLink ContentBlockKind = "resource_link"
	ContentBlockResource     ContentBlockKind = "resource"
)

func (b ContentBlock) MarshalJSON() ([]byte, error) {
	if len(b) == 0 {
		return []byte("null"), nil
	}
	return json.RawMessage(b).MarshalJSON()
}

func (b *ContentBlock) UnmarshalJSON(data []byte) error {
	raw := append(json.RawMessage(nil), data...)
	*b = ContentBlock(raw)
	return nil
}

type DecodedContentBlock interface {
	ContentKind() ContentBlockKind
}

type TextContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	TextContent
}

func (b TextContentBlock) ContentKind() ContentBlockKind { return b.Kind }

type ImageContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	ImageContent
}

func (b ImageContentBlock) ContentKind() ContentBlockKind { return b.Kind }

type AudioContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	AudioContent
}

func (b AudioContentBlock) ContentKind() ContentBlockKind { return b.Kind }

type ResourceLinkContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	ResourceLink
}

func (b ResourceLinkContentBlock) ContentKind() ContentBlockKind { return b.Kind }

type EmbeddedResourceContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	EmbeddedResource
}

func (b EmbeddedResourceContentBlock) ContentKind() ContentBlockKind { return b.Kind }

type UnknownContentBlock struct {
	Kind ContentBlockKind `json:"type"`
	Raw  json.RawMessage  `json:"-"`
}

func (b UnknownContentBlock) ContentKind() ContentBlockKind { return b.Kind }

func DecodeContentBlock(raw ContentBlock) (DecodedContentBlock, error) {
	data := json.RawMessage(raw)
	var env struct {
		Type ContentBlockKind `json:"type"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	switch env.Type {
	case ContentBlockText:
		var out TextContentBlock
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	case ContentBlockImage:
		var out ImageContentBlock
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	case ContentBlockAudio:
		var out AudioContentBlock
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	case ContentBlockResourceLink:
		var out ResourceLinkContentBlock
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	case ContentBlockResource:
		var out EmbeddedResourceContentBlock
		if err := json.Unmarshal(data, &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return UnknownContentBlock{Kind: env.Type, Raw: append(json.RawMessage(nil), data...)}, nil
	}
}

const (
	RequestPermissionOutcomeCancelled = "cancelled"
	RequestPermissionOutcomeSelected  = "selected"
)

func (o RequestPermissionOutcome) MarshalJSON() ([]byte, error) {
	if len(o) == 0 {
		return []byte("null"), nil
	}
	return json.RawMessage(o).MarshalJSON()
}

func (o *RequestPermissionOutcome) UnmarshalJSON(data []byte) error {
	raw := append(json.RawMessage(nil), data...)
	*o = RequestPermissionOutcome(raw)
	return nil
}

func CancelledPermissionOutcome() RequestPermissionOutcome {
	return RequestPermissionOutcome(mustRaw(map[string]any{"outcome": RequestPermissionOutcomeCancelled}))
}

func SelectedPermissionOutcomeFor(optionID PermissionOptionID) RequestPermissionOutcome {
	return RequestPermissionOutcome(mustRaw(map[string]any{"outcome": RequestPermissionOutcomeSelected, "optionId": optionID}))
}

func RequestPermissionResponseCancelled() RequestPermissionResponse {
	return RequestPermissionResponse{Outcome: CancelledPermissionOutcome()}
}

func RequestPermissionResponseSelected(optionID PermissionOptionID) RequestPermissionResponse {
	return RequestPermissionResponse{Outcome: SelectedPermissionOutcomeFor(optionID)}
}

func mustRaw(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal ACP helper value: %v", err))
	}
	return raw
}
