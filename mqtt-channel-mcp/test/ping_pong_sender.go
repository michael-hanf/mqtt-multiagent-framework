// Standalone test script — sends a ping via MQTT and listens for pong.
// Usage: go run test/ping_pong_sender.go -broker mqtt://localhost:1883 -topic agents/task/builder
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"flag"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Message struct {
	Type    string `json:"type"`
	From    string `json:"from"`
	Role    string `json:"role"`
	Payload string `json:"payload"`
	TS      int64  `json:"ts"`
	TSSent  int64  `json:"ts_sent,omitempty"` // Original send time for RTT calculation
}

var sentTime int64

func main() {
	broker := flag.String("broker", "mqtt://localhost:1883", "MQTT broker URL")
	topic := flag.String("topic", "agents/task/builder", "Topic to send ping to")
	replyTopic := flag.String("reply", "agents/reply/ping-test", "Topic to listen for pong")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	brokerURL, err := url.Parse(*broker)
	if err != nil {
		log.Fatalf("invalid broker URL: %v", err)
	}

	router := paho.NewStandardRouter()
	router.RegisterHandler(*replyTopic, func(p *paho.Publish) {
		var msg Message
		if err := json.Unmarshal(p.Payload, &msg); err != nil {
			fmt.Printf("[recv] raw: %s\n", p.Payload)
			return
		}
		fmt.Printf("[recv] type=%s from=%s role=%s payload=%s\n", msg.Type, msg.From, msg.Role, msg.Payload)
		if msg.Type == "pong" {
			fmt.Println("✓ Pong received! Test passed.")
			cancel()
		}
	})

	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		BrokerUrls: []*url.URL{brokerURL},
		KeepAlive:  30,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			log.Printf("connected, subscribing to %s", *replyTopic)
			cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: *replyTopic, QoS: 1},
				},
			})

			// Send ping
			msg := Message{
				Type:    "ping",
				From:    "ping-test",
				Role:    "tester",
				Payload: "hello from ping_pong_sender",
				TS:      time.Now().UnixMilli(),
			}
			data, _ := json.Marshal(msg)

			cm.Publish(ctx, &paho.Publish{
				Topic:   *topic,
				QoS:     1,
				Payload: data,
				Properties: &paho.PublishProperties{
					ResponseTopic:   *replyTopic,
					CorrelationData: []byte("ping-test-001"),
				},
			})
			log.Printf("ping sent to %s (response_topic=%s)", *topic, *replyTopic)
		},
		OnConnectError: func(err error) {
			log.Printf("connection error: %v", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: "ping-pong-sender",
			Router:   router,
		},
	})
	if err != nil {
		log.Fatalf("mqtt connect: %v", err)
	}

	if err := cm.AwaitConnection(ctx); err != nil {
		log.Fatalf("await connection: %v", err)
	}

	// Wait for pong or timeout
	select {
	case <-ctx.Done():
		log.Println("done")
	case <-time.After(30 * time.Second):
		log.Println("timeout waiting for pong")
	}

	cm.Disconnect(ctx)
}
