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

func (u *sessionUpdateBase) setRaw(raw json.RawMessage) {
	u.raw = append(json.RawMessage(nil), raw...)
}

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

// sessionUpdatePtr constrains *T to a session-update value whose embedded base
// can be stamped with the original raw JSON after decoding.
type sessionUpdatePtr[T any] interface {
	*T
	DecodedSessionUpdate
	setRaw(json.RawMessage)
}

// decodeUpdateAs unmarshals raw into a fresh T, records the raw bytes on its
// embedded base, and returns it as a value (matching the concrete types callers
// type-assert on).
func decodeUpdateAs[T DecodedSessionUpdate, PT sessionUpdatePtr[T]](raw json.RawMessage) (DecodedSessionUpdate, error) {
	out := new(T)
	if err := json.Unmarshal(raw, out); err != nil {
		return nil, err
	}
	PT(out).setRaw(raw)
	value := *out
	if err := validateDecodedSessionUpdate(value); err != nil {
		return nil, err
	}
	return value, nil
}

func validateDecodedSessionUpdate(update DecodedSessionUpdate) error {
	switch u := update.(type) {
	case UserMessageChunkUpdate:
		return validateContentChunkMessageID(u.SessionUpdateKind(), u.ContentChunk)
	case AgentMessageChunkUpdate:
		return validateContentChunkMessageID(u.SessionUpdateKind(), u.ContentChunk)
	case AgentThoughtChunkUpdate:
		return validateContentChunkMessageID(u.SessionUpdateKind(), u.ContentChunk)
	default:
		return nil
	}
}

func validateContentChunkMessageID(kind SessionUpdateKind, chunk ContentChunk) error {
	if chunk.MessageID == "" {
		return fmt.Errorf("%s messageId is required", kind)
	}
	return nil
}

var sessionUpdateDecoders = map[SessionUpdateKind]func(json.RawMessage) (DecodedSessionUpdate, error){
	SessionUpdateUserMessageChunk:  decodeUpdateAs[UserMessageChunkUpdate],
	SessionUpdateAgentMessageChunk: decodeUpdateAs[AgentMessageChunkUpdate],
	SessionUpdateAgentThoughtChunk: decodeUpdateAs[AgentThoughtChunkUpdate],
	SessionUpdateToolCall:          decodeUpdateAs[ToolCallSessionUpdate],
	SessionUpdateToolCallUpdate:    decodeUpdateAs[ToolCallUpdateSessionUpdate],
	SessionUpdatePlan:              decodeUpdateAs[PlanSessionUpdate],
	SessionUpdateAvailableCommands: decodeUpdateAs[AvailableCommandsSessionUpdate],
	SessionUpdateCurrentMode:       decodeUpdateAs[CurrentModeSessionUpdate],
	SessionUpdateConfigOption:      decodeUpdateAs[ConfigOptionSessionUpdate],
	SessionUpdateSessionInfo:       decodeUpdateAs[SessionInfoSessionUpdate],
}

func DecodeSessionUpdate(raw json.RawMessage) (DecodedSessionUpdate, error) {
	var env struct {
		SessionUpdate SessionUpdateKind `json:"sessionUpdate"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	if decode, ok := sessionUpdateDecoders[env.SessionUpdate]; ok {
		return decode(raw)
	}
	base := sessionUpdateBase{SessionUpdate: env.SessionUpdate}
	base.setRaw(raw)
	return UnknownSessionUpdate{sessionUpdateBase: base}, nil
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

func decodeContentAs[T DecodedContentBlock](data []byte) (DecodedContentBlock, error) {
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

var contentBlockDecoders = map[ContentBlockKind]func([]byte) (DecodedContentBlock, error){
	ContentBlockText:         decodeContentAs[TextContentBlock],
	ContentBlockImage:        decodeContentAs[ImageContentBlock],
	ContentBlockAudio:        decodeContentAs[AudioContentBlock],
	ContentBlockResourceLink: decodeContentAs[ResourceLinkContentBlock],
	ContentBlockResource:     decodeContentAs[EmbeddedResourceContentBlock],
}

func DecodeContentBlock(raw ContentBlock) (DecodedContentBlock, error) {
	data := json.RawMessage(raw)
	var env struct {
		Type ContentBlockKind `json:"type"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	if decode, ok := contentBlockDecoders[env.Type]; ok {
		return decode(data)
	}
	return UnknownContentBlock{Kind: env.Type, Raw: append(json.RawMessage(nil), data...)}, nil
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
