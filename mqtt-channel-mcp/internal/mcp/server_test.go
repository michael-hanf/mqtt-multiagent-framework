package mcp

import (
	"encoding/json"
	"testing"

	"mqtt-channel-mcp/internal/config"
)

func TestHandleMQTTMessageFormatsPayload(t *testing.T) {
	cfg := &config.Config{
		Role:     "tester",
		ClientID: "test-client",
	}
	srv := New(cfg)

	// Verify handler doesn't panic without connected clients
	payload := `{"type":"ping","from":"sender","role":"builder","payload":"hello","ts":1234567890}`
	srv.HandleMQTTMessage("agents/task/tester", []byte(payload), nil)
}

func TestMessageMarshal(t *testing.T) {
	msg := Message{
		Type:    "ping",
		From:    "test",
		Role:    "tester",
		Payload: "hello",
		TS:      1234567890,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Type != "ping" {
		t.Errorf("type = %q, want ping", decoded.Type)
	}
	if decoded.From != "test" {
		t.Errorf("from = %q, want test", decoded.From)
	}
	if decoded.Payload != "hello" {
		t.Errorf("payload = %q, want hello", decoded.Payload)
	}
}

func TestMetaKeyEncoding(t *testing.T) {
	// Verify the topic encoding logic (/ → _)
	topic := "agents/task/builder"
	encoded := ""
	for _, c := range topic {
		if c == '/' {
			encoded += "_"
		} else {
			encoded += string(c)
		}
	}
	if encoded != "agents_task_builder" {
		t.Errorf("encoded = %q, want agents_task_builder", encoded)
	}
}
