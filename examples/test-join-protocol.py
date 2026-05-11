"""
Join Protocol End-to-End Test
==============================

Runs two agents in the same process:
  - SENDER:   connects, waits for JOINER to appear via presence, sends a task, waits for response
  - JOINER:   follows SPEC.md Section 5.3 join protocol, responds to tasks

Pass criteria (all must succeed within timeout):
  1. JOINER publishes retained presence on agents/presence/<id>
  2. JOINER announces on agents/broadcast/general
  3. SENDER receives JOINER presence via discovery
  4. SENDER sends task → JOINER receives it → JOINER responds
  5. SENDER receives response within TIMEOUT_S seconds

Usage:
    python test-join-protocol.py [--broker localhost] [--timeout 10]
"""

import argparse
import json
import threading
import sys
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: pip install paho-mqtt")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

JOINER_ID = "cc-test-joiner"
SENDER_ID = "cc-test-sender"
DISCOVERY_WAIT_S = 0.5

# ── Shared state ──────────────────────────────────────────────────────────────

results = {
    "joiner_presence_seen":  threading.Event(),
    "joiner_announced":      threading.Event(),
    "task_received_by_joiner": threading.Event(),
    "response_received":     threading.Event(),
}

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def pub(client, topic, payload, retain=False, qos=1):
    client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

# ── JOINER ────────────────────────────────────────────────────────────────────

def make_joiner(_broker, _port):
    c = mqtt.Client(client_id=JOINER_ID, protocol=mqtt.MQTTv5)
    c.will_set(
        f"agents/presence/{JOINER_ID}",
        json.dumps({"from": JOINER_ID, "status": "offline", "ts": 0}),
        qos=1, retain=True
    )

    def on_connect(client, _ud, _flags, rc, _props=None):
        if rc != 0:
            print(f"  [JOINER] connect failed rc={rc}"); return
        client.subscribe("agents/presence/#", qos=1)
        threading.Timer(DISCOVERY_WAIT_S, lambda: joiner_joined(client)).start()

    def joiner_joined(client):
        pub(client, f"agents/presence/{JOINER_ID}", {
            "from": JOINER_ID, "status": "online", "role": "test-joiner",
            "capabilities": ["echo"],
            "subscriptions": [f"agents/task/{JOINER_ID}", "agents/broadcast/#"],
            "ts": now_ms()
        }, retain=True)
        client.subscribe(f"agents/task/{JOINER_ID}", qos=1)
        pub(client, "agents/broadcast/general", {
            "type": "announce", "from": JOINER_ID,
            "payload": "joined", "capabilities": ["echo"], "ts": now_ms()
        }, qos=0)
        results["joiner_announced"].set()

    def on_message(client, _ud, msg):
        if msg.topic == f"agents/task/{JOINER_ID}":
            results["task_received_by_joiner"].set()
            payload = json.loads(msg.payload)
            rt = payload.get("response_topic")
            if rt:
                pub(client, rt, {
                    "type": "result", "from": JOINER_ID,
                    "re": f"Re: {payload.get('re','')}",
                    "payload": {"status": "ok", "echo": payload.get("payload")},
                    "ts": now_ms()
                })

    c.on_connect = on_connect
    c.on_message = on_message
    return c

# ── SENDER ────────────────────────────────────────────────────────────────────

def make_sender(_broker, _port):
    c = mqtt.Client(client_id=SENDER_ID, protocol=mqtt.MQTTv5)

    def on_connect(client, _ud, _flags, rc, _props=None):
        if rc != 0:
            print(f"  [SENDER] connect failed rc={rc}"); return
        client.subscribe("agents/presence/#", qos=1)
        client.subscribe(f"agents/task/{SENDER_ID}", qos=1)
        client.subscribe("agents/broadcast/general", qos=0)

    def on_message(client, _ud, msg):
        try:
            payload = json.loads(msg.payload)
        except Exception:
            return

        if msg.topic == f"agents/presence/{JOINER_ID}":
            if payload.get("status") == "online":
                results["joiner_presence_seen"].set()
                # Send task as soon as JOINER presence is seen
                threading.Timer(0.1, lambda: send_task(client)).start()

        elif msg.topic == f"agents/task/{SENDER_ID}":
            if payload.get("from") == JOINER_ID:
                results["response_received"].set()

    def send_task(client):
        pub(client, f"agents/task/{JOINER_ID}", {
            "type": "task", "from": SENDER_ID,
            "re": "End-to-end test task",
            "response_topic": f"agents/task/{SENDER_ID}",
            "payload": {"message": "Can you echo this back?"},
            "ts": now_ms()
        })

    c.on_connect = on_connect
    c.on_message = on_message
    return c

# ── Test runner ───────────────────────────────────────────────────────────────

def run_test(broker, port, timeout):
    print(f"\nJoin Protocol End-to-End Test")
    print(f"Broker  : {broker}:{port}")
    print(f"Timeout : {timeout}s per check")
    print(f"{'─'*50}")

    joiner = make_joiner(broker, port)
    sender = make_sender(broker, port)

    joiner.connect(broker, port, keepalive=30)
    sender.connect(broker, port, keepalive=30)
    joiner.loop_start()
    sender.loop_start()

    checks = [
        ("joiner_announced",          "JOINER announces on broadcast/general"),
        ("joiner_presence_seen",      "SENDER discovers JOINER via retained presence"),
        ("task_received_by_joiner",   "JOINER receives task"),
        ("response_received",         "SENDER receives response from JOINER"),
    ]

    passed = 0
    failed = 0

    for key, label in checks:
        ok = results[key].wait(timeout=timeout)
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}  {label}")
        if ok: passed += 1
        else:  failed += 1

    print(f"{'─'*50}")
    print(f"  Result: {passed}/{passed+failed} checks passed")

    # Cleanup
    for client, cid in [(joiner, JOINER_ID), (sender, SENDER_ID)]:
        pub(client, f"agents/presence/{cid}",
            {"from": cid, "status": "offline", "ts": now_ms()}, retain=True)
        client.disconnect()
        client.loop_stop()

    return failed == 0

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker",  default="localhost")
    parser.add_argument("--port",    default=1883, type=int)
    parser.add_argument("--timeout", default=10,   type=int,
                        help="Seconds to wait per check (default: 10)")
    args = parser.parse_args()

    success = run_test(args.broker, args.port, args.timeout)
    sys.exit(0 if success else 1)
