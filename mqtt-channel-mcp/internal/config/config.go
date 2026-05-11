package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Auth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LogConfig struct {
	Enabled       bool     `json:"enabled"`
	Path          string   `json:"path"`
	Topics        []string `json:"topics"`
	MaxPayloadLen int      `json:"maxPayloadLen"` // 0 = unlimited
}

type Config struct {
	Broker               string    `json:"broker"`
	Role                 string    `json:"role"`
	SubscribeTopics      []string  `json:"subscribeTopics"`
	ResponseTopicDefault string    `json:"responseTopicDefault"`
	PublishTopicPrefix   string    `json:"publishTopicPrefix"`
	ClientID             string    `json:"clientId"`
	Auth                 Auth      `json:"auth"`
	Log                  LogConfig `json:"log"`
	HeartbeatInterval    int       `json:"heartbeatInterval"` // seconds, 0 = default (30s)
	Features             []string  `json:"features,omitempty"` // active capabilities, e.g. ["mqtt-mcp", "obsidian-vault"]
}

// Load reads config from a JSON file and applies env-var overrides.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config read: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config parse: %w", err)
	}

	applyEnvOverrides(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		cfg.Broker = v
	}
	if v := os.Getenv("MQTT_ROLE"); v != "" {
		cfg.Role = v
	}
	if v := os.Getenv("MQTT_SUBSCRIBE_TOPICS"); v != "" {
		cfg.SubscribeTopics = strings.Split(v, ",")
	}
	if v := os.Getenv("MQTT_CLIENT_ID"); v != "" {
		cfg.ClientID = v
	}
}

func validate(cfg *Config) error {
	if cfg.Broker == "" {
		return fmt.Errorf("config: broker is required")
	}
	if cfg.Role == "" {
		return fmt.Errorf("config: role is required")
	}
	if len(cfg.SubscribeTopics) == 0 {
		return fmt.Errorf("config: subscribeTopics must not be empty")
	}
	if cfg.ClientID == "" {
		return fmt.Errorf("config: clientId is required")
	}
	return nil
}
