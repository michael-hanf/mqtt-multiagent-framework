# mqtt-channel-mcp

MCP server bridge that connects Claude Code agents via MQTT pub/sub.

## Prerequisites

- MQTT broker running with authentication (see [docker-compose.example.yml](../docker-compose.example.yml))
- Claude Code with MCP support
- Go 1.21+ (for building from source)

## Quick Start

```bash
# 1. Start broker (with auth — see docker-compose.example.yml for setup)
docker compose -f ../docker-compose.example.yml up -d

# 2. Build from source
go build -o mqtt-channel-mcp ./...
# Windows: go build -o mqtt-channel-mcp.exe ./...

# 3. Copy and edit config
cp agent-config.example.json my-agent.local.json
# Edit: broker URL, agent-id, role, subscribeTopics, auth credentials
```

Add to Claude Code MCP config (`~/.claude.json`):

```json
{
  "mcpServers": {
    "mqtt-channel": {
      "type": "stdio",
      "command": "/path/to/mqtt-channel-mcp",
      "args": ["--config", "/path/to/my-agent.local.json"]
    }
  }
}
```

> ⚠️ Name your config `*.local.json` and add `*.local.json` to `.gitignore` — never commit credentials.

## Config Reference

| Field | Description | Example |
|---|---|---|
| `broker` | MQTT broker URL | `mqtt://localhost:1883` |
| `role` | Agent role label | `builder`, `conductor`, `tester` |
| `clientId` | Unique MQTT client ID | `cc-builder-brix` |
| `subscribeTopics` | Topics this agent listens to | `["agents/task/<id>", "agents/broadcast/#"]` |
| `responseTopicDefault` | Default topic for responses | `agents/task/<id>` |
| `publishTopicPrefix` | Optional prefix for outbound publishes (rarely needed) | `agents/response` |
| `features` | Reserved for future feature flags | `[]` |
| `auth.username` / `auth.password` | Broker credentials | required — see broker setup |

**Environment variable overrides** (no restart needed for broker URL changes):

| Env var | Overrides |
|---|---|
| `MQTT_BROKER` | `broker` |
| `MQTT_ROLE` | `role` |
| `MQTT_CLIENT_ID` | `clientId` |
| `MQTT_SUBSCRIBE_TOPICS` | `subscribeTopics` (comma-separated) |
| `MQTT_USERNAME` | `auth.username` |
| `MQTT_PASSWORD` | `auth.password` |

## Security

- Default broker setup requires password authentication (`allow_anonymous false`)
- Never commit config files with real credentials — use `*.local.json` pattern
- For internet-facing deployments: enable TLS on port 8883 (see `docs/infrastructure.md`)

## Topic Schema

See [`../topic-schema/SPEC.md`](../topic-schema/SPEC.md) for full topic conventions and payload schema.
