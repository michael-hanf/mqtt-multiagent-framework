package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
	gomcp "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"mqtt-channel-mcp/internal/config"
	mqttclient "mqtt-channel-mcp/internal/mqtt"
)

// Message is the structured JSON format exchanged over MQTT.
type Message struct {
	Type    string `json:"type"`
	From    string `json:"from"`
	Role    string `json:"role"`
	Payload string `json:"payload"`
	TS      int64  `json:"ts"`
}

// notifyJob holds a single MCP notification to be delivered.
type notifyJob struct {
	method string
	params map[string]any
}

// notifyInterval is the minimum delay between consecutive MCP notifications.
// Prevents Claude Code from dropping rapid-fire notifications (broadcast race fix).
const notifyInterval = 50 * time.Millisecond

// dedupTTL is how long a seen message key is retained before expiry.
const dedupTTL = 200 * time.Millisecond

// Server wraps the MCP server and bridges MQTT ↔ Claude Code.
type Server struct {
	mcp       *server.MCPServer
	mqtt      *mqttclient.Client
	cfg       *config.Config
	notifyCh  chan notifyJob
	dedupMu   sync.Mutex
	dedupSeen map[string]time.Time
}

// New creates the MCP server with the mqtt_publish tool.
func New(cfg *config.Config) *Server {
	s := &Server{
		cfg:       cfg,
		notifyCh:  make(chan notifyJob, 64),
		dedupSeen: make(map[string]time.Time),
	}

	s.mcp = server.NewMCPServer(
		"mqtt-channel-mcp",
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithExperimental(map[string]any{
			"claude/channel": map[string]any{},
		}),
	)

	// Register mqtt_publish tool
	publishTool := gomcp.NewTool("mqtt_publish",
		gomcp.WithDescription("Publish a message to an MQTT topic. Supports MQTT 5.0 request/response pattern."),
		gomcp.WithString("topic", gomcp.Required(), gomcp.Description("MQTT topic to publish to")),
		gomcp.WithString("payload", gomcp.Required(), gomcp.Description("Message payload (string or JSON)")),
		gomcp.WithString("type", gomcp.Required(), gomcp.Description("Message type: ping, pong, task, result, tunnel_request, tunnel_status")),
		gomcp.WithString("response_topic", gomcp.Description("MQTT 5.0 response topic for request/response pattern")),
		gomcp.WithString("correlation_data", gomcp.Description("MQTT 5.0 correlation data for matching responses")),
	)
	s.mcp.AddTool(publishTool, s.handlePublish)

	// Register mqtt_query tool
	queryTool := gomcp.NewTool("mqtt_query",
		gomcp.WithDescription("Subscribe to an MQTT topic and collect retained/incoming messages for a short timeout. Useful for reading agent presence (agents/presence/#) or other retained state."),
		gomcp.WithString("topic", gomcp.Required(), gomcp.Description("MQTT topic or wildcard to query (e.g. agents/presence/#)")),
		gomcp.WithNumber("timeout", gomcp.Description("Seconds to wait for messages (default: 2)")),
	)
	s.mcp.AddTool(queryTool, s.handleQuery)

	return s
}

// Start launches the background notification worker and dedup cleaner.
// Must be called before HandleMQTTMessage is used. Stops when ctx is cancelled.
func (s *Server) Start(ctx context.Context) {
	go s.notifyWorker(ctx)
	go s.dedupCleaner(ctx)
}

// isDuplicate returns true if an identical message (same from+ts) was seen
// within dedupTTL. Records the key on first sight.
func (s *Server) isDuplicate(payload []byte) bool {
	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil || msg.From == "" {
		return false // unparseable — let it through
	}
	key := fmt.Sprintf("%s:%d", msg.From, msg.TS)
	s.dedupMu.Lock()
	defer s.dedupMu.Unlock()
	if _, seen := s.dedupSeen[key]; seen {
		return true
	}
	s.dedupSeen[key] = time.Now()
	return false
}

// dedupCleaner periodically removes expired entries from the dedup map.
func (s *Server) dedupCleaner(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().Add(-dedupTTL)
			s.dedupMu.Lock()
			for key, seen := range s.dedupSeen {
				if seen.Before(cutoff) {
					delete(s.dedupSeen, key)
				}
			}
			s.dedupMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

// notifyWorker drains notifyCh and forwards each job to all MCP clients.
// A fixed delay between sends prevents Claude Code from dropping rapid-fire
// notifications (broadcast race condition fix).
func (s *Server) notifyWorker(ctx context.Context) {
	for {
		select {
		case job, ok := <-s.notifyCh:
			if !ok {
				return
			}
			s.mcp.SendNotificationToAllClients(job.method, job.params)
			select {
			case <-time.After(notifyInterval):
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// SetMQTTClient sets the MQTT client after it's connected.
func (s *Server) SetMQTTClient(client *mqttclient.Client) {
	s.mqtt = client
}

// MCPServer returns the underlying MCP server for stdio serving.
func (s *Server) MCPServer() *server.MCPServer {
	return s.mcp
}

// HandleMQTTMessage is called when an MQTT message arrives.
// It pushes it into Claude Code via notifications/claude/channel.
func (s *Server) HandleMQTTMessage(topic string, payload []byte, props *paho.PublishProperties) {
	// Drop internal system topics — presence heartbeats and similar infra messages
	// must not be forwarded to Claude Code as channel notifications.
	if strings.HasPrefix(topic, "agents/presence/") {
		log.Printf("[mcp] dropping presence topic: %s", topic)
		return
	}
	if s.isDuplicate(payload) {
		log.Printf("[mcp] dedup: dropping duplicate from topic=%s", topic)
		return
	}
	log.Printf("[mcp] incoming MQTT: topic=%s len=%d", topic, len(payload))

	meta := map[string]string{
		"topic": topic,
		"role":  s.cfg.Role,
	}

	// Forward MQTT 5.0 properties as meta
	if props != nil {
		if props.ResponseTopic != "" {
			meta["response_topic"] = props.ResponseTopic
		}
		if len(props.CorrelationData) > 0 {
			meta["correlation_data"] = string(props.CorrelationData)
		}
	}

	params := map[string]any{
		"channel": "mqtt-channel",
		"content": string(payload),
		"source":  topic,
		"meta":    meta,
	}

	select {
	case s.notifyCh <- notifyJob{method: "notifications/claude/channel", params: params}:
	default:
		log.Printf("[mcp] notify queue full, dropping message from topic=%s", topic)
	}
}

// handlePublish handles the mqtt_publish tool call from Claude Code.
func (s *Server) handlePublish(ctx context.Context, req gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not connected"), nil
	}

	topic := req.GetString("topic", "")
	payloadStr := req.GetString("payload", "")
	msgType := req.GetString("type", "task")
	responseTopic := req.GetString("response_topic", "")
	correlationData := req.GetString("correlation_data", "")

	if topic == "" {
		return gomcp.NewToolResultError("topic is required"), nil
	}

	msg := Message{
		Type:    msgType,
		From:    s.cfg.ClientID,
		Role:    s.cfg.Role,
		Payload: payloadStr,
		TS:      time.Now().UnixMilli(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}

	// Auto-set response_topic from config if not explicitly provided
	if responseTopic == "" && s.cfg.ResponseTopicDefault != "" {
		responseTopic = s.cfg.ResponseTopicDefault
		log.Printf("[mcp] auto-set response_topic=%s from config", responseTopic)
	}

	// Build MQTT 5.0 publish properties
	var props *paho.PublishProperties
	if responseTopic != "" || correlationData != "" {
		props = &paho.PublishProperties{}
		if responseTopic != "" {
			props.ResponseTopic = responseTopic
		}
		if correlationData != "" {
			props.CorrelationData = []byte(correlationData)
		}
	}

	if err := s.mqtt.Publish(ctx, topic, data, props); err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("mqtt publish: %v", err)), nil
	}

	log.Printf("[mcp] published: topic=%s type=%s", topic, msgType)
	return gomcp.NewToolResultText(fmt.Sprintf("Published to %s (type=%s, correlation=%s)", topic, msgType, correlationData)), nil
}

// handleQuery handles the mqtt_query tool call from Claude Code.
func (s *Server) handleQuery(ctx context.Context, req gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not connected"), nil
	}

	topic := req.GetString("topic", "")
	if topic == "" {
		return gomcp.NewToolResultError("topic is required"), nil
	}
	timeoutSec := int(req.GetFloat("timeout", 2))
	if timeoutSec < 1 {
		timeoutSec = 2
	}

	results, err := s.mqtt.Query(ctx, topic, timeoutSec)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("mqtt query: %v", err)), nil
	}

	data, err := json.Marshal(results)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}

	log.Printf("[mcp] query: topic=%s results=%d", topic, len(results))
	return gomcp.NewToolResultText(string(data)), nil
}
