# Why MQTT for Multi-Agent Coordination?

> A practical architecture note from building a real multi-agent team.

---

## The Problem with HTTP-based Agent Protocols

In 2026, every major agent interoperability protocol — Google's A2A, IBM's ACP, W3C's ANP — chose HTTP as their transport layer. It's a reasonable default: HTTP is everywhere, tooling is mature, and every developer knows it.

But HTTP has a fundamental shape problem for multi-agent systems:

**HTTP is request/response.** Agent A must know where Agent B lives to talk to it. Every connection is direct, bilateral, and synchronous by default. With N agents, you get up to N×N potential connections to manage. Adding a new agent means updating routing tables. Losing an agent means broken connections.

This works fine when you have two or three services with known addresses. It starts to break when you want:
- Agents that can join or leave at runtime
- Broadcast messages that reach whoever is listening
- Loose coupling between agents that don't know about each other
- Async communication without polling

---

## The Insight: Agents Are Not Services

A service has a stable address, a documented API, and an SLA. It's designed to be called.

An agent is different. It has goals, context, and autonomy. It might be busy, offline, or in the middle of something else. It publishes what it knows and listens for what it needs. The right mental model isn't a REST endpoint — it's a team member in a shared workspace.

That's exactly what MQTT was designed for.

---

## Why MQTT

MQTT is a publish/subscribe protocol built for IoT: many independent devices, intermittent connectivity, minimal overhead, no central coordination logic. It turns out these properties map directly onto multi-agent teams:

| IoT Problem | Agent Problem |
|---|---|
| Many devices, unknown topology | Many agents, dynamic team composition |
| Devices join/leave unpredictably | Agents start/stop between sessions |
| Broadcast to all sensors | Broadcast to all agents (`agents/broadcast/#`) |
| Device-to-device via broker | Agent-to-agent via broker |
| Retained messages for last known state | Retained presence for discovery |

The broker handles routing. Agents only know topics, not each other's addresses. Adding a new agent is a single subscribe call — nothing else needs to change.

---

## The Architecture in Practice

```
┌─────────────────────────────────────────────────┐
│                  MQTT Broker                     │
│                                                  │
│  agents/task/{id}      ← direct messages        │
│  agents/broadcast/#    ← team-wide              │
│  agents/presence/{id}  ← retained, discovery    │
│  agents/discuss/{topic}← async discussion       │
└─────────────────────────────────────────────────┘
        ↑              ↑              ↑
    Agent A         Agent B        Agent C
  (any platform)  (any platform) (any platform)
```

**Loose coupling by design.** Agent A publishes a task. Whoever is subscribed to that topic receives it. Agent A doesn't need to know if Agent B is online, what platform it runs on, or how it's implemented.

**Discovery without a registry.** Each agent publishes a retained presence message on startup. New agents subscribe to `agents/presence/#` and immediately receive the last known status of every peer — no service registry, no bootstrap server. (Technical details: [`topic-schema/SPEC.md` §5](../topic-schema/SPEC.md))

**Broadcast is trivial.** `agents/broadcast/general` reaches everyone. No routing logic needed.

---

## Conductor-free Coordination

Most multi-agent frameworks assume a central orchestrator: one agent that knows everything, assigns tasks, and collects results. This creates a single point of failure and a bottleneck.

This framework takes a different approach: **capability-based roles with peer-to-peer coordination.**

Each agent advertises its capabilities in its presence message. Any agent can send a task to any other agent directly. When help is needed, an agent broadcasts to `agents/broadcast/help` — whoever can help, responds.

There is no conductor. Coordination emerges from the protocol. Roles exist, but they are capability-based, not authority-based. A coordinator role emerges from what an agent can do, not from a hardcoded hierarchy.

**Escalation Ladder:** When an agent can't resolve something alone, it follows a defined escalation path:
1. Try to solve it
2. Ask a specific peer directly (`agents/help/{id}`)
3. Broadcast to all if no specific peer is available (`agents/broadcast/help`)
4. Only escalate to a human if no agent can help

This keeps humans out of routine coordination while ensuring nothing gets stuck.

---

## Known Limitations

**Hub-and-spoke problem:** Agents communicate through the broker, not directly. In the current implementation, if two agents want to have a bilateral conversation, all messages go through the broker. This is by design for loose coupling, but means the broker is a single point of failure.

**No built-in acknowledgement:** MQTT QoS 1 guarantees at-least-once delivery to the broker, not to the agent application. If an agent crashes after receiving a message but before processing it, the message is lost. Design your tasks to be idempotent where possible.

**Payload size:** No hard limit in the framework. Mosquitto's default is unlimited. Keep JSON payloads under 64KB. For larger data, transfer the content separately (shared filesystem, file reference) and send only a reference via MQTT.

**Authentication:** This framework ships without authentication by default — intended for local or trusted networks. See `docs/security.md` for a hardened Mosquitto configuration.

---

## Anthropic Does This Too

In May 2026, Boris Cherny (creator of Claude Code at Anthropic) confirmed in a Sequoia interview:

> *"We have no more manually written code anywhere at the company. Claudes interact with one another over Slack while they dogfood the exact same setup."*

Slack is pub/sub. Channels are topics. The pattern is identical — Anthropic just uses Slack as the broker instead of MQTT. MQTT gives you the same architecture without the vendor dependency, with lower latency, and with a protocol designed for exactly this use case.

---

## What You Get

A multi-agent setup built on this framework gives you:

- **Zero-config agent discovery** via retained presence
- **Broadcast** to all agents in one publish
- **Dynamic team composition** — add or remove agents without reconfiguring others  
- **Platform independence** — agents can run on any OS, in any language, as long as they speak MQTT
- **Testable join protocol** — `examples/test-join-protocol.py` verifies the full flow automatically
- **Full topic reference** — see [`topic-schema/SPEC.md`](../topic-schema/SPEC.md) for the complete topic hierarchy and payload schema

---

*Built from a real working setup. The design decisions and open questions that shaped this framework are tracked in [`topic-schema/SPEC.md`](../topic-schema/SPEC.md).*
