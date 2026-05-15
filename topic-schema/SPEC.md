# MQTT Topic Schema Spec v0.1

> Status: Draft v0.1

---

## 1. Purpose

This document specifies the topic schema and conventions for the MQTT Multi-Agent Framework for Claude Code. The goal is a schema that any team can adopt without prior knowledge of the original setup.

---

## 2. Topic Hierarchy

```
agents/
├── task/<agent-id>        # Direct message to a specific agent
├── broadcast/             # Messages to all agents
│   ├── general            # General announcements
│   ├── help               # Peer-help requests (any agent can respond)
│   └── discussion            # Discussion threads (named channel)
├── presence/<agent-id>    # Retained: agent status (online/offline/busy)
└── help/<agent-id>        # Direct help request to a specific agent
```

---

## 3. Standard Message Format

```json
{
  "type": "task | result | ping | pong | discussion",
  "from": "<agent-id>",
  "payload": "<string or JSON-encoded string>",
  "ts": <unix-ms>
}
```

**Optional fields:**
- `response_topic` — MQTT 5.0 request/response pattern
- `correlation_data` — for response matching
- `re` — subject line (human-readable)

---

## 4. Agent IDs

Format: `cc-<role>-<name>`

Examples:
- `cc-builder-alice`
- `cc-tester-bob`
- `cc-conductor-charlie`

---

## 5. Presence / Discovery

### 5.1 Retained Presence

Each agent publishes a **retained** message to `agents/presence/<agent-id>` (QoS 1) on startup.
The broker holds this message — a newly connected agent immediately receives the last known status of all peers upon subscribing.

**Presence payload:**
```json
{
  "from": "cc-conductor-charlie",
  "status": "online",
  "role": "coordinator",
  "platform": "Linux",
  "capabilities": ["file-system", "vm-control", "mqtt-broker"],
  "subscriptions": ["agents/task/charlie", "agents/broadcast/#"],
  "ts": 1778508532000
}
```

**Status values:** `online` | `offline` | `busy`

### 5.2 Last Will (Offline Notification)

Each agent SHOULD configure a **Last Will** on MQTT connect:

- **Topic:** `agents/presence/<agent-id>`
- **Payload:** `{"from": "<agent-id>", "status": "offline", "ts": 0}`
- **Retain:** true, **QoS:** 1

→ On unexpected disconnect (crash, network failure), the broker updates the presence automatically.

### 5.3 Join Protocol for New Agents

```
1. Connect to broker (default: localhost:1883)
2. Register Last Will (see 5.2)
3. Subscribe to agents/presence/# → wait 500ms → build peer registry
4. Publish own presence (retained) to agents/presence/<own-id>
5. Subscribe to agents/task/<own-id> for direct messages
6. Optional: announce on agents/broadcast/general:
   {"type": "announce", "from": "<id>", "payload": "joined"}
```

No explicit approval required — publishing presence is the join signal.

---

## 6. Payload Schema

### 6.1 Required Fields

```json
{
  "type": "task | result | ping | pong | discussion | announce",
  "from": "<agent-id>",
  "payload": "<string or JSON-encoded string>",
  "ts": 1778508532000
}
```

### 6.2 Full Example (recommended)

```json
{
  "type": "task",
  "from": "cc-conductor-charlie",
  "re": "Transfer file to server",
  "session": "file-transfer-42",
  "response_topic": "agents/result/charlie",
  "payload": {
    "action": "copy",
    "source": "/home/user/report.pdf",
    "destination": "/tmp/report.pdf"
  },
  "ts": 1778508532000
}
```

### 6.3 Minimum Implementation for Compatible Agents

A third-party agent MUST implement the following to be compatible:

| Requirement | Description |
|---|---|
| Required fields | `type`, `from`, `payload`, `ts` |
| Presence | Retained publish to `agents/presence/<id>` on startup |
| Subscribe | At minimum `agents/task/<own-id>` and `agents/broadcast/#` |
| Response | For tasks with `response_topic`: send reply to that topic |
| Last Will | Recommended but not mandatory |

### 6.4 MQTT 3.1.1 Compatibility

`response_topic` is an MQTT 5.0 property. For 3.1.1 brokers:
- Include `response_topic` inside the JSON payload (not as an MQTT property)
- Use the `session` field for response correlation

---

## 7. Routing Table

Complete overview of topics every agent needs to know:

| Topic | Direction | QoS | Retained | Purpose |
|---|---|---|---|---|
| `agents/task/<agent-id>` | → Receive | 1 | No | Direct message to this agent |
| `agents/broadcast/general` | ↔ Send/Receive | 0 | No | General announcements |
| `agents/broadcast/help` | ↔ Send/Receive | 1 | No | Peer-help requests (any agent can respond) |
| `agents/broadcast/discussion` | ↔ Send/Receive | 0 | No | Discussion threads |
| `agents/broadcast/#` | → Subscribe | — | — | Wildcard: receive all broadcasts |
| `agents/presence/<agent-id>` | → Publish | 1 | **Yes** | Own presence (status, capabilities) |
| `agents/presence/#` | → Subscribe | — | — | Discover all agents (discovery) |
| `agents/help/<agent-id>` | → Receive | 1 | No | Direct help request to this agent |
| `agents/result/<agent-id>` | → Receive | 1 | No | Task results (when response_topic is set) |

**Minimum subscribe set** (required for every agent):
```
agents/task/<own-id>      # Receive direct tasks
agents/broadcast/#        # Receive all broadcasts
agents/presence/#         # Discovery (on startup, 500ms window)
```

---

## 8. Payload Limits

MQTT and Mosquitto impose no application-level limit. The **recommended convention**:

- **Normal case:** ≤ 64 KB JSON
- **Absolute limit:** ≤ 256 MB (Mosquitto default `message_size_limit`)

For larger data, send a file path or reference in the payload — not the content itself:

```json
{
  "type": "result",
  "from": "cc-tester-bob",
  "re": "Result reference",
  "payload": {
    "ref": "file",
    "path": "/shared/results/report.json",
    "size_kb": 1240
  }
}
```

> **Note:** Message truncation in Claude Code sessions is caused by Claude Code's internal
> monitor notification system — not by the MQTT channel. MQTT messages are never truncated.

---

## 9. Conventions

- **QoS 0** for fire-and-forget (logs, status updates)
- **QoS 1** for tasks and results (at-least-once delivery)
- **Retained** only for presence topics
- **No sensitive data** in payloads (no keys, no internal IPs)
- Timestamps always as `ts` in Unix milliseconds

---

## 10. Minimal Setup for New Teams

1. Start an MQTT broker: see [`../docker-compose.example.yml`](../docker-compose.example.yml) for a broker with password auth
2. Build and configure `mqtt-channel-mcp`: see [`../mqtt-channel-mcp/README.md`](../mqtt-channel-mcp/README.md)
3. Choose an agent ID following the schema in Section 4
4. Publish retained presence to `agents/presence/<id>`

> **Security:** Mosquitto without auth is only suitable for local/isolated setups.
> For any networked deployment: enable TLS + password auth. See [`../docs/security.md`](../docs/security.md).

---

*v0.1 — 2026-05-11*
