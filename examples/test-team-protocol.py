"""
Team Protocol End-to-End Test
==============================

Tests three-agent coordination across three scenarios:

  Scenario 1 — Discovery
    All three agents publish retained presence. Each agent must see
    the other two via agents/presence/#.

  Scenario 2 — Direct task
    VERA sends a task to BRIX with response_topic set.
    BRIX must receive it and reply to VERA.

  Scenario 3 — Broadcast help
    VERA broadcasts on agents/broadcast/help.
    Both BRIX and CONNI must respond within timeout.

Pass criteria: all 8 checks must succeed within TIMEOUT_S seconds.

Usage:
    python test-team-protocol.py [--broker localhost] [--port 1883] [--timeout 10]
    python test-team-protocol.py --broker 192.168.1.x   # remote broker
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

VERA_ID  = "cc-test-vera"
BRIX_ID  = "cc-test-brix"
CONNI_ID = "cc-test-conni"

KNOWN_IDS = {VERA_ID, BRIX_ID, CONNI_ID}
LABEL = {VERA_ID: "vera", BRIX_ID: "brix", CONNI_ID: "conni"}

DISCOVERY_WAIT_S = 0.8   # time to collect retained presence before publishing own

# ── Shared state ──────────────────────────────────────────────────────────────

checks = {
    # Scenario 1 — Discovery (only VERA and BRIX tracked; CONNI sees both but no check needed)
    "vera_sees_brix":      threading.Event(),
    "vera_sees_conni":     threading.Event(),
    "brix_sees_vera":      threading.Event(),
    "brix_sees_conni":     threading.Event(),
    # Scenario 2 — Direct task
    "brix_got_task":       threading.Event(),
    "vera_got_reply":      threading.Event(),
    # Scenario 3 — Broadcast help
    "brix_answered_help":  threading.Event(),
    "conni_answered_help": threading.Event(),
}

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def pub(client, topic, payload, retain=False, qos=1):
    client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

# ── Agent factory ─────────────────────────────────────────────────────────────

def make_agent(agent_id, broker, port):
    c = mqtt.Client(client_id=agent_id, protocol=mqtt.MQTTv5)

    c.will_set(
        f"agents/presence/{agent_id}",
        json.dumps({"from": agent_id, "status": "offline", "ts": 0}),
        qos=1, retain=True
    )

    def on_connect(client, _ud, _flags, rc, _props=None):
        if rc != 0:
            print(f"  [{agent_id}] connect failed rc={rc}")
            return
        client.subscribe("agents/presence/#", qos=1)
        client.subscribe(f"agents/task/{agent_id}", qos=1)
        client.subscribe("agents/broadcast/#", qos=1)
        threading.Timer(DISCOVERY_WAIT_S, lambda: publish_presence(client)).start()

    def publish_presence(client):
        pub(client, f"agents/presence/{agent_id}", {
            "from": agent_id,
            "status": "online",
            "role": "test-agent",
            "capabilities": ["echo", "help"],
            "subscriptions": [f"agents/task/{agent_id}", "agents/broadcast/#"],
            "ts": now_ms()
        }, retain=True)

    def on_message(client, _ud, msg):
        try:
            payload = json.loads(msg.payload)
        except Exception:
            return
        topic  = msg.topic
        sender = payload.get("from", "")

        # ── Scenario 1: presence discovery ───────────────────────────────────
        if topic.startswith("agents/presence/"):
            if sender in KNOWN_IDS and sender != agent_id and payload.get("status") == "online":
                key = f"{LABEL[agent_id]}_sees_{LABEL[sender]}"
                if key in checks:
                    checks[key].set()

        # ── Scenario 2 + 3 incoming via task topic ───────────────────────────
        elif topic == f"agents/task/{agent_id}":
            msg_type = payload.get("type", "")
            re_field = payload.get("re", "")

            # S2: BRIX receives the direct task from VERA
            if agent_id == BRIX_ID and msg_type == "task" and sender == VERA_ID:
                checks["brix_got_task"].set()
                rt = payload.get("response_topic")
                if rt:
                    pub(client, rt, {
                        "type": "result", "from": agent_id,
                        "re": f"Re: {re_field}",
                        "payload": {"status": "ok", "echo": payload.get("payload")},
                        "ts": now_ms()
                    })

            # S2: VERA receives the reply from BRIX
            if agent_id == VERA_ID and msg_type == "result" and sender == BRIX_ID:
                checks["vera_got_reply"].set()

            # S3: VERA collects help responses from BRIX and CONNI
            if agent_id == VERA_ID and msg_type == "result" and re_field == "Re: help":
                if sender == BRIX_ID:
                    checks["brix_answered_help"].set()
                if sender == CONNI_ID:
                    checks["conni_answered_help"].set()

        # ── Scenario 3: broadcast help → respond ─────────────────────────────
        elif topic == "agents/broadcast/help":
            if sender in KNOWN_IDS and sender != agent_id:
                rt = payload.get("response_topic")
                if rt:
                    pub(client, rt, {
                        "type": "result", "from": agent_id,
                        "re": "Re: help",
                        "payload": {"status": "ok", "message": f"{agent_id} can help"},
                        "ts": now_ms()
                    })

    c.on_connect = on_connect
    c.on_message = on_message
    c.connect(broker, port, keepalive=30)
    c.loop_start()
    return c

# ── Test runner ───────────────────────────────────────────────────────────────

def run_test(broker, port, timeout):
    print(f"\nTeam Protocol End-to-End Test")
    print(f"Broker  : {broker}:{port}")
    print(f"Timeout : {timeout}s per check")
    print(f"Agents  : {VERA_ID}, {BRIX_ID}, {CONNI_ID}")
    print(f"{'─'*55}")

    vera  = make_agent(VERA_ID,  broker, port)
    brix  = make_agent(BRIX_ID,  broker, port)
    conni = make_agent(CONNI_ID, broker, port)  # noqa: F841

    # After presence has propagated, trigger active scenarios
    delay = DISCOVERY_WAIT_S + 0.5

    def scenario_2():
        pub(vera, f"agents/task/{BRIX_ID}", {
            "type": "task", "from": VERA_ID,
            "re": "Direct task test",
            "response_topic": f"agents/task/{VERA_ID}",
            "payload": {"message": "Echo this back"},
            "ts": now_ms()
        })

    def scenario_3():
        pub(vera, "agents/broadcast/help", {
            "type": "task", "from": VERA_ID,
            "re": "Broadcast help test",
            "response_topic": f"agents/task/{VERA_ID}",
            "payload": {"message": "Anyone available?"},
            "ts": now_ms()
        }, qos=1)

    threading.Timer(delay,       scenario_2).start()
    threading.Timer(delay + 1.0, scenario_3).start()

    check_list = [
        ("vera_sees_brix",       "S1  VERA   discovers BRIX via retained presence"),
        ("vera_sees_conni",      "S1  VERA   discovers CONNI via retained presence"),
        ("brix_sees_vera",       "S1  BRIX   discovers VERA via retained presence"),
        ("brix_sees_conni",      "S1  BRIX   discovers CONNI via retained presence"),
        ("brix_got_task",        "S2  BRIX   receives direct task from VERA"),
        ("vera_got_reply",       "S2  VERA   receives reply from BRIX"),
        ("brix_answered_help",   "S3  BRIX   responds to broadcast help"),
        ("conni_answered_help",  "S3  CONNI  responds to broadcast help"),
    ]

    passed = failed = 0
    for key, label in check_list:
        ok = checks[key].wait(timeout=timeout)
        print(f"  {'✓ PASS' if ok else '✗ FAIL'}  {label}")
        if ok: passed += 1
        else:  failed += 1

    print(f"{'─'*55}")
    print(f"  Result: {passed}/{passed + failed} checks passed")

    for client, cid in [(vera, VERA_ID), (brix, BRIX_ID), (conni, CONNI_ID)]:
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
    parser.add_argument("--timeout", default=10,   type=int)
    args = parser.parse_args()

    success = run_test(args.broker, args.port, args.timeout)
    sys.exit(0 if success else 1)
