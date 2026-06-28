package main

import "testing"

func TestObjectPropertiesFlattensElicitationRequestModes(t *testing.T) {
	sch := readJSON[schema]("../../../testdata/acp-schema/schema.json")
	defs = sch.Defs
	props := objectProperties(defs["CreateElicitationRequest"])
	for _, key := range []string{"message", "mode", "requestedSchema", "sessionId", "toolCallId", "requestId", "elicitationId", "url"} {
		if props[key] == nil {
			t.Fatalf("CreateElicitationRequest missing flattened property %q; keys = %#v", key, props)
		}
	}
}
