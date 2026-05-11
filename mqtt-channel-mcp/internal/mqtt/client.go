package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"mqtt-channel-mcp/internal/config"
	"mqtt-channel-mcp/internal/logger"
)

// MessageHandler is called when an MQTT message arrives.
type MessageHandler func(topic string, payload []byte, props *paho.PublishProperties)

// Client wraps autopaho for MQTT 5.0 with automatic reconnect.
type Client struct {
	cm              *autopaho.ConnectionManager
	cfg             *config.Config
	handler         MessageHandler
	logger          *logger.MQTTLogger
	router          *paho.StandardRouter
	mu              sync.Mutex
	connected       bool
	permanentTopics []string // topics from config — must never be unsubscribed by Query
}

// IsConnected returns true if the MQTT connection is currently up.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// New creates a new MQTT 5.0 client. Call Connect() to start.
func New(cfg *config.Config, handler MessageHandler) *Client {
	c := &Client{
		cfg:     cfg,
		handler: handler,
		router:  paho.NewStandardRouter(),
	}
	if cfg.Log.Enabled && cfg.Log.Path != "" {
		topics := cfg.Log.Topics
		if len(topics) == 0 {
			topics = []string{"agents/#"}
		}
		c.logger = logger.New(cfg.Log.Path, topics, cfg.Log.MaxPayloadLen)
		log.Printf("[mqtt] logging enabled → %s (topics: %v)", cfg.Log.Path, topics)
	}
	return c
}

// Connect establishes the MQTT connection and subscribes to configured topics.
func (c *Client) Connect(ctx context.Context) error {
	brokerURL, err := url.Parse(c.cfg.Broker)
	if err != nil {
		return fmt.Errorf("mqtt: invalid broker URL %q: %w", c.cfg.Broker, err)
	}

	// Register log-only handler for log topics (separate from task topics)
	if c.logger != nil {
		for _, topic := range c.cfg.Log.Topics {
			t := topic
			c.router.RegisterHandler(t, func(p *paho.Publish) {
				from := ""
				if p.Properties != nil && p.Properties.User != nil {
					for _, u := range p.Properties.User {
						if u.Key == "from" {
							from = u.Value
							break
						}
					}
				}
				c.logger.Log(p.Topic, from, "", p.Payload)
			})
		}
	}

	c.permanentTopics = make([]string, len(c.cfg.SubscribeTopics))
	copy(c.permanentTopics, c.cfg.SubscribeTopics)

	for _, topic := range c.cfg.SubscribeTopics {
		t := topic // capture
		c.router.RegisterHandler(t, func(p *paho.Publish) {
			if c.handler != nil {
				c.handler(p.Topic, p.Payload, p.Properties)
			}
		})
	}

	presenceTopic := "agents/presence/" + c.cfg.ClientID

	// buildPresencePayload returns a fresh presence payload with current timestamp.
	// Timestamp allows consumers to detect stale retained presence (lease pattern).
	buildPresencePayload := func() []byte {
		p, _ := json.Marshal(map[string]any{
			"status":               "online",
			"clientId":             c.cfg.ClientID,
			"role":                 c.cfg.Role,
			"ts":                   time.Now().UnixMilli(),
			"task":                 nil,
			"heartbeat_interval_s": heartbeatInterval(c.cfg.HeartbeatInterval),
			"features":             c.cfg.Features,
		})
		return p
	}

	// buildOfflinePayload returns the LWT payload published by the broker on disconnect.
	// Using a status field instead of empty payload keeps the retained message alive,
	// allowing consumers to distinguish offline from never-seen.
	buildOfflinePayload := func() []byte {
		p, _ := json.Marshal(map[string]any{
			"status":               "offline",
			"clientId":             c.cfg.ClientID,
			"role":                 c.cfg.Role,
			"ts":                   time.Now().UnixMilli(),
			"task":                 nil,
			"heartbeat_interval_s": heartbeatInterval(c.cfg.HeartbeatInterval),
			"features":             c.cfg.Features,
		})
		return p
	}

	cliCfg := autopaho.ClientConfig{
		BrokerUrls:                    []*url.URL{brokerURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: true,
		SessionExpiryInterval:         3600,
		// Exponential backoff: min 1s, max 60s, initial max 2s, factor 2.0.
		ReconnectBackoff: autopaho.NewExponentialBackoff(1*time.Second, 60*time.Second, 2*time.Second, 2.0),
		// Last Will: broker publishes offline status when client disconnects unexpectedly.
		// Retained=true keeps the message alive so consumers always find a presence entry.
		WillMessage: &paho.WillMessage{
			Topic:   presenceTopic,
			Payload: buildOfflinePayload(),
			QoS:     1,
			Retain:  true,
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			c.mu.Lock()
			c.connected = true
			c.mu.Unlock()
			log.Printf("[mqtt] connected to %s", c.cfg.Broker)
			// Use a fresh context for subscribe — the outer ctx may be stale on reconnect
			subCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// Build subscription list: log wildcards first, then task topics only
			// if not already covered — avoids double-delivery from overlapping subscriptions.
			topicSet := make(map[string]bool)
			if c.logger != nil {
				for _, t := range c.cfg.Log.Topics {
					topicSet[t] = true
				}
			}
			for _, t := range c.cfg.SubscribeTopics {
				if !coveredByAny(topicSet, t) {
					topicSet[t] = true
				}
			}
			subs := make([]paho.SubscribeOptions, 0, len(topicSet))
			for topic := range topicSet {
				subs = append(subs, paho.SubscribeOptions{Topic: topic, QoS: 1, NoLocal: true})
			}
			if _, err := cm.Subscribe(subCtx, &paho.Subscribe{Subscriptions: subs}); err != nil {
				log.Printf("[mqtt] subscribe error: %v", err)
			} else {
				log.Printf("[mqtt] subscribed: %v", topicSet)
			}
			// Announce presence with fresh timestamp (retained lease)
			if _, err := cm.Publish(subCtx, &paho.Publish{
				Topic:   presenceTopic,
				QoS:     1,
				Retain:  true,
				Payload: buildPresencePayload(),
			}); err != nil {
				log.Printf("[mqtt] presence publish error: %v", err)
			} else {
				log.Printf("[mqtt] presence announced: %s", presenceTopic)
			}
		},
		OnConnectError: func(err error) {
			c.mu.Lock()
			c.connected = false
			c.mu.Unlock()
			log.Printf("[mqtt] connection error: %v", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: c.cfg.ClientID,
			Router:   c.router,
		},
	}

	// Optional auth
	if c.cfg.Auth.Username != "" {
		cliCfg.ConnectUsername = c.cfg.Auth.Username
		cliCfg.ConnectPassword = []byte(c.cfg.Auth.Password)
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		return fmt.Errorf("mqtt: connect failed: %w", err)
	}

	c.mu.Lock()
	c.cm = cm
	c.mu.Unlock()

	// Wait for initial connection
	if err := cm.AwaitConnection(ctx); err != nil {
		return fmt.Errorf("mqtt: await connection: %w", err)
	}

	// Start heartbeat goroutine — publishes retained presence every heartbeatInterval
	// seconds so consumers can detect stale entries via ts age.
	// Runs for the lifetime of ctx; safe across reconnects (cm handles connection state).
	go func() {
		interval := time.Duration(heartbeatInterval(c.cfg.HeartbeatInterval)) * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.mu.Lock()
				connected := c.connected
				localCM := c.cm
				c.mu.Unlock()
				if !connected || localCM == nil {
					continue
				}
				pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if _, err := localCM.Publish(pubCtx, &paho.Publish{
					Topic:   presenceTopic,
					QoS:     1,
					Retain:  true,
					Payload: buildPresencePayload(),
				}); err != nil {
					log.Printf("[mqtt] heartbeat error: %v", err)
				}
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Publish sends a message with optional MQTT 5.0 properties.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte, props *paho.PublishProperties) error {
	c.mu.Lock()
	cm := c.cm
	c.mu.Unlock()

	if cm == nil {
		return fmt.Errorf("mqtt: not connected")
	}

	_, err := cm.Publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        1,
		Payload:    payload,
		Properties: props,
	})
	if err == nil && c.logger != nil {
		c.logger.Log(topic, "", "", payload)
	}
	return err
}

// Query subscribes to a topic, collects retained/incoming messages for the given
// timeout, then unsubscribes and returns the payloads. Useful for reading retained
// messages like agents/presence/#.
//
// Uses AddOnPublishReceived instead of RegisterHandler so that permanent router
// handlers are never overwritten or deleted.
func (c *Client) Query(ctx context.Context, topic string, timeoutSec int) ([]json.RawMessage, error) {
	c.mu.Lock()
	cm := c.cm
	connected := c.connected
	c.mu.Unlock()
	if cm == nil || !connected {
		return nil, fmt.Errorf("mqtt: not connected")
	}

	var mu sync.Mutex
	var results []json.RawMessage

	// Register a temporary receive handler via AddOnPublishReceived.
	// The returned removeFn deactivates it without touching the router.
	removeFn := cm.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		if !mqttMatch(topic, pr.Packet.Topic) {
			return false, nil
		}
		if len(pr.Packet.Payload) > 0 {
			mu.Lock()
			results = append(results, json.RawMessage(pr.Packet.Payload))
			mu.Unlock()
		}
		return false, nil // don't stop other handlers
	})
	defer removeFn()

	subCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	// Only subscribe if not already covered by a permanent topic subscription.
	// Permanent topics are managed by Connect() and must never be unsubscribed here.
	isPermanent := c.isPermanentTopic(topic)
	if !isPermanent {
		if _, err := cm.Subscribe(subCtx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}},
		}); err != nil {
			return nil, fmt.Errorf("mqtt query subscribe: %w", err)
		}
		defer cm.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{topic}})
	}

	<-subCtx.Done()

	mu.Lock()
	defer mu.Unlock()
	return results, nil
}

// heartbeatInterval returns the configured interval or the default of 30s.
func heartbeatInterval(configured int) int {
	if configured > 0 {
		return configured
	}
	return 30
}

// isPermanentTopic returns true if the given topic is covered by any of the
// permanently subscribed topics (from config). These must never be unsubscribed.
func (c *Client) isPermanentTopic(topic string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.permanentTopics {
		if p == topic || mqttMatch(p, topic) {
			return true
		}
	}
	return false
}

// coveredByAny returns true if topic is matched by any wildcard in the set.
func coveredByAny(topicSet map[string]bool, topic string) bool {
	for pattern := range topicSet {
		if mqttMatch(pattern, topic) {
			return true
		}
	}
	return false
}

// mqttMatch implements MQTT wildcard matching (# and +).
func mqttMatch(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tp := strings.Split(topic, "/")
	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tp) {
			return false
		}
		if p != "+" && p != tp[i] {
			return false
		}
	}
	return len(pp) == len(tp)
}

// Disconnect gracefully shuts down the MQTT connection.
func (c *Client) Disconnect(ctx context.Context) error {
	c.mu.Lock()
	cm := c.cm
	c.mu.Unlock()

	if cm == nil {
		return nil
	}
	return cm.Disconnect(ctx)
}
