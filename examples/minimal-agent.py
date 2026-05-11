"""
Minimal Agent — MQTT Multi-Agent Framework Join-Protocol Test
=============================================================

This script implements the minimal requirements from topic-schema/SPEC.md
to verify that a foreign agent can successfully join the team and receive tasks.

Requirements: pip install paho-mqtt

Usage:
    python minimal-agent.py [--broker mqtt://localhost:1883] [--id my-agent]

What it does (follows SPEC.md Section 5.3 Join Protocol):
    1. Connect to broker + register Last Will
    2. Subscribe to agents/presence/# → discover peers (500ms)
    3. Publish own presence (retained)
    4. Subscribe to agents/task/<own-id>
    5. Announce join on agents/broadcast/general
    6. Wait for tasks and respond to them
"""

import argparse
import json
import time
import threading
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Missing dependency: pip install paho-mqtt")
    raise

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_BROKER = "localhost"
DEFAULT_PORT   = 1883
DEFAULT_ID     = "cc-example-minimal"
DEFAULT_ROLE   = "example"

# ── Helpers ───────────────────────────────────────────────────────────────────

def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def publish_json(client, topic: str, payload: dict, retain: bool = False, qos: int = 1):
    client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
    print(f"  → [{topic}] {json.dumps(payload)[:120]}")

# ── Callbacks ─────────────────────────────────────────────────────────────────

peers = {}
peer_discovery_done = threading.Event()

def on_connect(client, userdata, _flags, rc, _properties=None):
    if rc == 0:
        print(f"[{now_ms()}] Connected to broker")
        # Step 2: Subscribe presence for discovery
        client.subscribe("agents/presence/#", qos=1)
        print("  Subscribed to agents/presence/# — discovering peers (500ms)...")
        threading.Timer(0.5, lambda: on_discovery_complete(client, userdata)).start()
    else:
        print(f"Connection failed: rc={rc}")

def on_discovery_complete(client, userdata):
    agent_id = userdata["agent_id"]
    print(f"\n[{now_ms()}] Discovery complete — found {len(peers)} peer(s):")
    for pid, pdata in peers.items():
        print(f"  • {pid}: role={pdata.get('role','?')} status={pdata.get('status','?')}")

    # Step 3: Publish own presence (retained)
    presence = {
        "from": agent_id,
        "status": "online",
        "role": DEFAULT_ROLE,
        "platform": "Python minimal-agent (example)",
        "capabilities": ["echo"],
        "subscriptions": [f"agents/task/{agent_id}", "agents/broadcast/#"],
        "ts": now_ms()
    }
    print(f"\n[{now_ms()}] Publishing own presence (retained)...")
    publish_json(client, f"agents/presence/{agent_id}", presence, retain=True)

    # Step 4: Subscribe own task topic
    client.subscribe(f"agents/task/{agent_id}", qos=1)
    print(f"  Subscribed to agents/task/{agent_id}")

    # Step 5: Announce join
    announce = {
        "type": "announce",
        "from": agent_id,
        "payload": "joined",
        "capabilities": ["echo"],
        "ts": now_ms()
    }
    print(f"\n[{now_ms()}] Announcing join on agents/broadcast/general...")
    publish_json(client, "agents/broadcast/general", announce, qos=0)

    print(f"\n[{now_ms()}] ✓ Join protocol complete. Waiting for tasks on agents/task/{agent_id} ...\n")
    peer_discovery_done.set()

def on_message(client, userdata, msg):
    agent_id = userdata["agent_id"]
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode())
    except Exception:
        payload = {"raw": msg.payload.decode()}

    # Presence updates during discovery
    if topic.startswith("agents/presence/"):
        peer_id = topic.split("/")[-1]
        if peer_id != agent_id:
            peers[peer_id] = payload
        return

    # Incoming task
    if topic == f"agents/task/{agent_id}":
        print(f"\n[{now_ms()}] ← TASK received from {payload.get('from','?')}:")
        print(f"  re: {payload.get('re','(no subject)')}")
        print(f"  payload: {json.dumps(payload.get('payload', {}))[:200]}")

        # Respond if response_topic is set
        response_topic = payload.get("response_topic")
        if response_topic:
            response = {
                "type": "result",
                "from": agent_id,
                "re": f"Re: {payload.get('re', 'task')}",
                "payload": {
                    "status": "ok",
                    "echo": payload.get("payload"),
                    "message": "minimal-agent received and processed task successfully"
                },
                "ts": now_ms()
            }
            print(f"\n[{now_ms()}] → Responding to {response_topic}...")
            publish_json(client, response_topic, response)
        else:
            print("  (no response_topic — task is fire-and-forget)")

def on_disconnect(_client, _userdata, rc, _properties=None):
    print(f"\n[{now_ms()}] Disconnected (rc={rc})")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Minimal MQTT Multi-Agent Framework agent")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Broker hostname (default: localhost)")
    parser.add_argument("--port",   default=DEFAULT_PORT, type=int)
    parser.add_argument("--id",     default=DEFAULT_ID,   help="Agent ID (default: cc-example-minimal)")
    args = parser.parse_args()

    agent_id = args.id
    print(f"MQTT Multi-Agent Framework — Minimal Agent")
    print(f"Agent ID : {agent_id}")
    print(f"Broker   : {args.broker}:{args.port}")
    print()

    client = mqtt.Client(
        client_id=agent_id,
        protocol=mqtt.MQTTv5,
        userdata={"agent_id": agent_id}
    )

    # Last Will (SPEC.md Section 5.2)
    last_will = json.dumps({"from": agent_id, "status": "offline", "ts": 0})
    client.will_set(f"agents/presence/{agent_id}", last_will, qos=1, retain=True)

    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    try:
        peer_discovery_done.wait(timeout=5)
        print("Press Ctrl+C to disconnect.\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{now_ms()}] Shutting down...")
        # Publish offline presence before disconnect
        publish_json(client, f"agents/presence/{agent_id}",
                     {"from": agent_id, "status": "offline", "ts": now_ms()},
                     retain=True)
        client.disconnect()
        client.loop_stop()

if __name__ == "__main__":
    main()
