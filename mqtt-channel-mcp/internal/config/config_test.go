package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	os.WriteFile(path, []byte(`{
		"broker": "mqtt://localhost:1883",
		"role": "tester",
		"subscribeTopics": ["agents/task/tester"],
		"clientId": "test-client"
	}`), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Broker != "mqtt://localhost:1883" {
		t.Errorf("broker = %q, want mqtt://localhost:1883", cfg.Broker)
	}
	if cfg.Role != "tester" {
		t.Errorf("role = %q, want tester", cfg.Role)
	}
	if cfg.ClientID != "test-client" {
		t.Errorf("clientId = %q, want test-client", cfg.ClientID)
	}
}

func TestLoadMissingBroker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	os.WriteFile(path, []byte(`{
		"role": "tester",
		"subscribeTopics": ["agents/task/tester"],
		"clientId": "test"
	}`), 0644)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
}

func TestEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.json")
	os.WriteFile(path, []byte(`{
		"broker": "mqtt://original:1883",
		"role": "original",
		"subscribeTopics": ["agents/task/original"],
		"clientId": "original"
	}`), 0644)

	t.Setenv("MQTT_BROKER", "mqtt://override:1883")
	t.Setenv("MQTT_ROLE", "override-role")
	t.Setenv("MQTT_CLIENT_ID", "override-id")
	t.Setenv("MQTT_SUBSCRIBE_TOPICS", "topic/a,topic/b")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Broker != "mqtt://override:1883" {
		t.Errorf("broker = %q, want mqtt://override:1883", cfg.Broker)
	}
	if cfg.Role != "override-role" {
		t.Errorf("role = %q, want override-role", cfg.Role)
	}
	if cfg.ClientID != "override-id" {
		t.Errorf("clientId = %q, want override-id", cfg.ClientID)
	}
	if len(cfg.SubscribeTopics) != 2 || cfg.SubscribeTopics[0] != "topic/a" {
		t.Errorf("subscribeTopics = %v, want [topic/a, topic/b]", cfg.SubscribeTopics)
	}
}

func TestLoadFileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
