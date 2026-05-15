# Infrastructure Setup

Everything you need to run the MQTT Multi-Agent Framework from scratch.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker + Docker Compose | any recent | For the broker |
| Python | 3.9+ | For examples and test scripts |
| Go | 1.21+ (1.25+ recommended) | Only if building mqtt-channel-mcp from source |
| Claude Code | latest | The agent runtime |

---

## 1. Start the MQTT Broker

The framework uses [Eclipse Mosquitto](https://mosquitto.org/) as its broker.

### Quick start (with auth)

```bash
# 1. Create a password file (written directly into ./mosquitto/)
docker run --rm -v $(pwd)/mosquitto:/mosquitto/config eclipse-mosquitto:2 \
  mosquitto_passwd -c /mosquitto/config/passwd myagent
# Enter a password when prompted — file appears at ./mosquitto/passwd

# 2. Start the broker
docker compose -f docker-compose.example.yml up -d

# 3. Verify it's running
docker compose -f docker-compose.example.yml logs mosquitto
```

The broker listens on:
- **Port 1883** — MQTT (password auth required)
- **Port 8883** — MQTT over TLS (optional, see below)

### Broker config

`mosquitto/config/mosquitto.conf` — edit to match your setup. Key settings:

```
allow_anonymous false        # never allow unauthenticated connections
password_file /mosquitto/passwd
```

> ⚠️ **Security:** Never expose port 1883 to the internet without TLS.
> For internet-facing deployments, enable TLS (port 8883) and restrict access via firewall.

> **Multi-machine setup:** If your broker runs on a different machine than your agents (e.g. broker on host, agents in VMs), replace `localhost` with the broker's IP address in your agent config.

---

## 2. Build or Install mqtt-channel-mcp

`mqtt-channel-mcp` is the MCP server that bridges Claude Code to MQTT.

### Build from source (Linux / macOS / Windows)

```bash
cd mqtt-channel-mcp
go build -o mqtt-channel-mcp ./...
# Windows: go build -o mqtt-channel-mcp.exe ./...
```

### Configure

Copy the example config and edit:

```bash
cp mqtt-channel-mcp/agent-config.example.json my-agent-config.json
```

```json
{
  "broker": "mqtt://localhost:1883",
  "role": "your-role",
  "subscribeTopics": [
    "agents/task/your-agent-id",
    "agents/broadcast/#"
  ],
  "responseTopicDefault": "agents/task/your-agent-id",
  "clientId": "cc-yourrole-yourname",
  "auth": {
    "username": "myagent",
    "password": "your-password"
  }
}
```

> ⚠️ **Never commit your config file with real credentials.**
> Recommended pattern: name your config `agent-config.local.json` and add `*.local.json` to `.gitignore`.
>
> All config fields can be overridden via environment variables:
>
> | Variable | Overrides |
> |---|---|
> | `MQTT_BROKER` | `broker` |
> | `MQTT_ROLE` | `role` |
> | `MQTT_CLIENT_ID` | `clientId` |
> | `MQTT_SUBSCRIBE_TOPICS` | `subscribeTopics` (comma-separated) |
> | `MQTT_USERNAME` | `auth.username` |
> | `MQTT_PASSWORD` | `auth.password` |

---

## 3. Register with Claude Code

Add `mqtt-channel-mcp` as an MCP server in your Claude Code config (`~/.claude.json`):

```json
{
  "mcpServers": {
    "mqtt-channel": {
      "type": "stdio",
      "command": "/path/to/mqtt-channel-mcp",
      "args": ["--config", "/path/to/my-agent-config.json"]
    }
  }
}
```

Restart Claude Code — but use the following start command to activate the channel:

```bash
claude --dangerously-load-development-channels server:mqtt-channel
```

> ⚠️ **This flag is required.** Without it, the `mqtt-channel` MCP server loads but the
> `mqtt_publish` and `mqtt_query` tools are not exposed to the agent.
> The flag must be passed every time Claude Code is started for an agent that uses MQTT.

Tip: add an alias to your shell profile so you don't forget it:
```bash
alias claude-agent='claude --dangerously-load-development-channels server:mqtt-channel'
```

---

## 4. Verify the Setup

```bash
# Install test dependencies
pip install paho-mqtt

# Run the join protocol test
python examples/test-join-protocol.py --broker localhost

# Expected output:
# ✓ PASS  JOINER announces on broadcast/general
# ✓ PASS  SENDER discovers JOINER via retained presence
# ✓ PASS  JOINER receives task
# ✓ PASS  SENDER receives response from JOINER
# Result: 4/4 checks passed
```

---

## 5. Run a Minimal Agent

```bash
python examples/minimal-agent.py --id cc-example-myname
```

The agent will:
1. Connect and register a Last Will
2. Discover existing peers via retained presence
3. Publish its own presence
4. Subscribe to its task topic
5. Announce itself on `agents/broadcast/general`
6. Wait for tasks and respond to them

---

## TLS Setup (optional)

To enable encrypted connections on port 8883:

1. Generate or obtain certificates (`ca.crt`, `server.crt`, `server.key`)
2. Place them in `mosquitto/certs/`
3. Uncomment the TLS section in `mosquitto/config/mosquitto.conf`
4. Uncomment the `certs` volume mount and port `8883` in `docker-compose.example.yml`
5. Update your agent config: `"broker": "mqtts://localhost:8883"`

For local development, [mkcert](https://github.com/FiloSottile/mkcert) generates trusted local certificates in seconds.

**Self-signed CA:** If you use a self-signed CA (e.g. generated with `openssl`), the client needs to verify the server certificate. Add the `caFile` option to your agent config:

```json
{
  "broker": "mqtts://your-broker-host:8883",
  "caFile": "/path/to/ca.crt",
  ...
}
```

Without `caFile`, TLS connections to a self-signed broker will fail certificate verification.

---

## Agent-Side Topic Restrictions (`allowedPublishPrefixes`)

The `allowedPublishPrefixes` config field restricts which MQTT topics the agent is allowed to publish to. This is enforced by `mqtt-channel-mcp` before the message reaches the broker — independent of broker-level ACLs.

**Config field:** optional. When omitted or empty, no restriction is applied.

**Matching rules:** a publish is allowed if the target topic either:
- exactly matches one of the listed prefixes, or
- starts with a listed prefix followed by `/`

**Example:**

```json
{
  "allowedPublishPrefixes": [
    "agents/task/myagent",
    "agents/presence/myagent",
    "agents/broadcast/"
  ]
}
```

With this config the agent can publish to `agents/task/myagent`, `agents/presence/myagent`, and any topic under `agents/broadcast/` (e.g. `agents/broadcast/general`), but not to `agents/task/otheragent` or other arbitrary topics.

> Combine with broker-level ACLs (see `docs/security.md`) for defence-in-depth.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Connection refused` on port 1883 | Broker not running | `docker compose up -d` |
| `Not authorised` | Wrong credentials | Check `auth` in agent config |
| Agent doesn't receive messages | Wrong `subscribeTopics` | Must include `agents/task/<your-id>` |
| Presence not showing up | Not published retained | Check `retain=True` + QoS 1 on presence publish |
| Test script times out | Broker unreachable or wrong port | `--broker` and `--port` flags |

---

## Directory Structure

```
mqtt-multiagent-framework/
├── docker-compose.example.yml   # Broker setup
├── mosquitto/
│   ├── config/mosquitto.conf    # Broker configuration
│   └── passwd                   # Password file (create locally, don't commit)
├── mqtt-channel-mcp/            # MCP server source (Go)
│   ├── agent-config.example.json
│   └── ...
├── topic-schema/
│   └── SPEC.md                  # Topic conventions and payload schema
├── examples/
│   ├── minimal-agent.py         # Minimal agent implementation
│   └── test-join-protocol.py    # End-to-end join protocol test
└── docs/
    ├── WHY.md                   # Architecture rationale
    ├── infrastructure.md        # This file
    └── security.md              # (TODO) Hardened production config — TLS, ACLs, firewall
```
