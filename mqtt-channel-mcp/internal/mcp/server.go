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
	Type          string `json:"type"`
	From          string `json:"from"`
	Role          string `json:"role"`
	Payload       string `json:"payload"`
	TS            int64  `json:"ts"`
	ResponseTopic string `json:"response_topic,omitempty"` // N3: included in body for MQTT 3.1.1 compat
	Re            string `json:"re,omitempty"`             // N3: reply reference ("Re: <subject>")
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
		gomcp.WithString("re", gomcp.Description("Reply reference, e.g. 'Re: <original subject>'. Included in JSON body for MQTT 3.1.1 interoperability.")),
		gomcp.WithBoolean("retain", gomcp.Description("Publish as retained message — broker keeps last value (use for presence updates). Default: false")),
		gomcp.WithNumber("qos", gomcp.Description("QoS level: 0 = fire-and-forget, 1 = at-least-once (default). Use 0 for logs/status, 1 for tasks/results.")),
	)
	s.mcp.AddTool(publishTool, s.handlePublish)

	// Register mqtt_request tool
	requestTool := gomcp.NewTool("mqtt_request",
		gomcp.WithDescription("Publish a message and wait for a response — request/reply in one call. Blocks until a response arrives on response_topic or timeout expires."),
		gomcp.WithString("topic", gomcp.Required(), gomcp.Description("MQTT topic to publish to")),
		gomcp.WithString("payload", gomcp.Required(), gomcp.Description("Message payload (string or JSON)")),
		gomcp.WithString("type", gomcp.Required(), gomcp.Description("Message type: task, ping, etc.")),
		gomcp.WithString("response_topic", gomcp.Description("Topic to wait for response on. Defaults to agent's own task topic.")),
		gomcp.WithNumber("timeout", gomcp.Description("Seconds to wait for response (default: 10)")),
	)
	s.mcp.AddTool(requestTool, s.handleRequest)

	// Register mqtt_who tool
	whoTool := gomcp.NewTool("mqtt_who",
		gomcp.WithDescription("Return all agents currently online (retained presence). Faster than mqtt_query + manual filtering."),
	)
	s.mcp.AddTool(whoTool, s.handleWho)

	// Register mqtt_status tool
	statusTool := gomcp.NewTool("mqtt_status",
		gomcp.WithDescription("Return current MQTT connection status: connected/disconnected, broker URL, client ID."),
	)
	s.mcp.AddTool(statusTool, s.handleStatus)

	// Register mqtt_subscriptions tool
	subscriptionsTool := gomcp.NewTool("mqtt_subscriptions",
		gomcp.WithDescription("Return the list of topics this agent is currently subscribed to."),
	)
	s.mcp.AddTool(subscriptionsTool, s.handleSubscriptions)

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
// checkTopicAllowed returns an error string if allowedPublishPrefixes is configured
// and the given topic does not start with any of the allowed prefixes.
// Returns "" when the topic is permitted.
func (s *Server) checkTopicAllowed(topic string) string {
	if len(s.cfg.AllowedPublishPrefixes) == 0 {
		return "" // no restriction configured
	}
	for _, prefix := range s.cfg.AllowedPublishPrefixes {
		if strings.HasPrefix(topic, prefix) {
			return ""
		}
	}
	return fmt.Sprintf("topic %q is not in allowedPublishPrefixes — rejected by agent policy", topic)
}

func (s *Server) handlePublish(ctx context.Context, req gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not connected"), nil
	}

	topic := req.GetString("topic", "")
	payloadStr := req.GetString("payload", "")
	msgType := req.GetString("type", "task")
	responseTopic := req.GetString("response_topic", "")
	correlationData := req.GetString("correlation_data", "")
	re := req.GetString("re", "")
	retain := req.GetBool("retain", false)
	qos := byte(req.GetFloat("qos", 1))
	if qos > 1 {
		qos = 1
	}

	if topic == "" {
		return gomcp.NewToolResultError("topic is required"), nil
	}
	if errMsg := s.checkTopicAllowed(topic); errMsg != "" {
		return gomcp.NewToolResultError(errMsg), nil
	}

	// Auto-set response_topic from config if not explicitly provided
	if responseTopic == "" && s.cfg.ResponseTopicDefault != "" {
		responseTopic = s.cfg.ResponseTopicDefault
		log.Printf("[mcp] auto-set response_topic=%s from config", responseTopic)
	}

	msg := Message{
		Type:          msgType,
		From:          s.cfg.ClientID,
		Role:          s.cfg.Role,
		Payload:       payloadStr,
		TS:            time.Now().UnixMilli(),
		ResponseTopic: responseTopic, // N3: also in body for MQTT 3.1.1 compat
		Re:            re,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
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

	if err := s.mqtt.Publish(ctx, topic, data, props, retain, qos); err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("mqtt publish: %v", err)), nil
	}

	log.Printf("[mcp] published: topic=%s type=%s retain=%v qos=%d", topic, msgType, retain, qos)
	return gomcp.NewToolResultText(fmt.Sprintf("Published to %s (type=%s, retain=%v, qos=%d)", topic, msgType, retain, qos)), nil
}

// handleRequest handles the mqtt_request tool call — publish + wait for response.
func (s *Server) handleRequest(ctx context.Context, req gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not connected"), nil
	}

	topic := req.GetString("topic", "")
	payloadStr := req.GetString("payload", "")
	msgType := req.GetString("type", "task")
	timeoutSec := int(req.GetFloat("timeout", 10))
	if timeoutSec < 1 {
		timeoutSec = 10
	}

	if topic == "" {
		return gomcp.NewToolResultError("topic is required"), nil
	}
	if errMsg := s.checkTopicAllowed(topic); errMsg != "" {
		return gomcp.NewToolResultError(errMsg), nil
	}

	// Default response_topic: agent's own task topic
	responseTopic := req.GetString("response_topic", "")
	if responseTopic == "" {
		responseTopic = s.cfg.ResponseTopicDefault
	}
	if responseTopic == "" {
		return gomcp.NewToolResultError("response_topic is required (or set responseTopicDefault in config)"), nil
	}

	msg := Message{
		Type:          msgType,
		From:          s.cfg.ClientID,
		Role:          s.cfg.Role,
		Payload:       payloadStr,
		TS:            time.Now().UnixMilli(),
		ResponseTopic: responseTopic, // N3: also in body for MQTT 3.1.1 compat
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}

	log.Printf("[mcp] request: topic=%s response_topic=%s timeout=%ds", topic, responseTopic, timeoutSec)

	result, err := s.mqtt.Request(ctx, topic, data, nil, false, 1, responseTopic, timeoutSec)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("mqtt request: %v", err)), nil
	}

	log.Printf("[mcp] request completed: topic=%s", topic)
	return gomcp.NewToolResultText(string(result)), nil
}

// handleWho returns all currently online agents via retained presence.
func (s *Server) handleWho(ctx context.Context, _ gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not connected"), nil
	}
	agents, err := s.mqtt.Who(ctx)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("mqtt who: %v", err)), nil
	}
	if agents == nil {
		agents = []mqttclient.AgentPresence{}
	}
	data, err := json.Marshal(agents)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}
	log.Printf("[mcp] who: %d agents online", len(agents))
	return gomcp.NewToolResultText(string(data)), nil
}

// handleStatus returns the current MQTT connection state.
func (s *Server) handleStatus(ctx context.Context, _ gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not initialized"), nil
	}
	status := s.mqtt.Status()
	data, err := json.Marshal(status)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}
	return gomcp.NewToolResultText(string(data)), nil
}

// handleSubscriptions returns the list of active topic subscriptions.
func (s *Server) handleSubscriptions(_ context.Context, _ gomcp.CallToolRequest) (*gomcp.CallToolResult, error) {
	if s.mqtt == nil {
		return gomcp.NewToolResultError("MQTT client not initialized"), nil
	}
	subs := s.mqtt.Subscriptions()
	data, err := json.Marshal(subs)
	if err != nil {
		return gomcp.NewToolResultError(fmt.Sprintf("json marshal: %v", err)), nil
	}
	return gomcp.NewToolResultText(string(data)), nil
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
	if errMsg := s.checkTopicAllowed(topic); errMsg != "" {
		return gomcp.NewToolResultError(errMsg), nil
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
