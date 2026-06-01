package main

import (
	"encoding/json"
	"testing"

	"github.com/gluonfield/acp-transport/acp"
)

func TestRequestPermissionResultUsesACPOutcomeEnvelope(t *testing.T) {
	result, rpcErr := requestPermissionResult(mustJSON(t, acp.RequestPermissionRequest{
		Options: []acp.PermissionOption{{OptionID: "allow_once", Name: "Allow once"}},
	}), "allow")
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}

	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]map[string]string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["outcome"]["outcome"] != "selected" || got["outcome"]["optionId"] != "allow_once" {
		t.Fatalf("unexpected permission result: %s", raw)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
