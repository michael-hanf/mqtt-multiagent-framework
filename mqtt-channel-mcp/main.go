package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mark3labs/mcp-go/server"

	"mqtt-channel-mcp/internal/config"
	mcpserver "mqtt-channel-mcp/internal/mcp"
	mqttclient "mqtt-channel-mcp/internal/mqtt"
)

func main() {
	// All logging to stderr — stdout is the MCP JSON-RPC channel
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	configPath := flag.String("config", "config/local.json", "path to config file")
	flag.Parse()

	// 1. Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("[main] config loaded: role=%s clientId=%s broker=%s", cfg.Role, cfg.ClientID, cfg.Broker)

	// 2. Create MCP server (not yet serving)
	mcpSrv := mcpserver.New(cfg)

	// 3. Create MQTT client with MCP message handler
	mqtt := mqttclient.New(cfg, mcpSrv.HandleMQTTMessage)

	// 4. Wire MQTT client into MCP server (for mqtt_publish tool)
	mcpSrv.SetMQTTClient(mqtt)

	// 5. Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5a. Start MCP notification worker (must run before MQTT connects)
	mcpSrv.Start(ctx)

	// 6. Connect MQTT (after MCP server is wired)
	go func() {
		if err := mqtt.Connect(ctx); err != nil {
			log.Printf("[main] mqtt connect: %v", err)
		}
	}()

	// 7. Serve MCP over stdio (blocks until shutdown)
	log.Printf("[main] starting MCP stdio server")
	go func() {
		if err := server.ServeStdio(mcpSrv.MCPServer()); err != nil {
			log.Printf("[main] mcp server: %v", err)
			cancel()
		}
	}()

	// 8. Wait for signal or context cancellation (e.g. stdio closed by CC)
	select {
	case sig := <-sigCh:
		log.Printf("[main] received signal %v, shutting down", sig)
		cancel()
	case <-ctx.Done():
		log.Printf("[main] context cancelled, shutting down")
	}

	if err := mqtt.Disconnect(ctx); err != nil {
		log.Printf("[main] mqtt disconnect: %v", err)
	}
	log.Printf("[main] shutdown complete")
}
