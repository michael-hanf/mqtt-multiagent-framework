# MQTT Multi-Agent Framework for Claude Code

> Pub/Sub multi-agent coordination for Claude Code — using MQTT instead of HTTP.

This framework lets multiple Claude Code agents communicate as a team over MQTT. Each agent subscribes to its own topic, discovers peers automatically via retained presence messages, and coordinates without a central orchestrator. It's built from a real working setup where three agents (development, infrastructure, coordination) collaborate on shared tasks daily.

**Why MQTT instead of HTTP?** → [`docs/WHY.md`](docs/WHY.md)  
**Set it up:** → [`docs/infrastructure.md`](docs/infrastructure.md)  
**Topic conventions and payload schema:** → [`topic-schema/SPEC.md`](topic-schema/SPEC.md)

## What's in the box

- **`mqtt-channel-mcp/`** — MCP server (Go) that bridges Claude Code to an MQTT broker. Gives Claude agents `mqtt_publish` and `mqtt_query` tools.
- **`topic-schema/SPEC.md`** — Topic hierarchy, message format, discovery protocol, and payload schema — everything needed to implement a compatible agent from scratch.
- **`examples/minimal-agent.py`** — A minimal Python agent that follows the join protocol end-to-end.
- **`examples/test-join-protocol.py`** — Automated test: two agents, one task, one response. Run it to verify your broker setup works.
- **`docker-compose.example.yml`** — Mosquitto broker with password auth.

## Quick start

```bash
# 1. Start the broker
docker compose -f docker-compose.example.yml up -d

# 2. Run the end-to-end test
pip install paho-mqtt
python examples/test-join-protocol.py --broker localhost

# Expected:
# ✓ PASS  JOINER announces on broadcast/general
# ✓ PASS  SENDER discovers JOINER via retained presence
# ✓ PASS  JOINER receives task
# ✓ PASS  SENDER receives response from JOINER
```

See [`docs/infrastructure.md`](docs/infrastructure.md) for broker auth setup and Claude Code integration.

## Known Limitations

- **Hub-and-Spoke topology:** All agent communication is routed via the MQTT broker.
  There is no direct agent-to-agent (A2A) connection. This is intentional — the broker
  is the single coordination point — but means the broker is a single point of failure.
  For production use, run a redundant broker setup (e.g. Mosquitto bridge or cluster).

- **No built-in access control per topic:** Any authenticated agent can publish to any topic.
  Topic-level ACLs must be configured separately in Mosquitto if needed.

- **No pre-built binary:** Build from source with `go build ./...` in `mqtt-channel-mcp/`.
  Tested on Linux and Windows.
