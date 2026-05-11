# Security Configuration

Hardening guide for the MQTT Multi-Agent Framework beyond the default local setup.

---

## Default Setup Security Posture

The default setup (single-machine, `localhost` broker) is designed for local development:

- Password authentication is **required** (`allow_anonymous false`)
- Broker only reachable from `localhost`
- No TLS — traffic is unencrypted

This is acceptable for local use. For multi-machine or internet-facing deployments, apply the hardening steps below.

---

## 1. TLS Encryption (port 8883)

Encrypts traffic between agents and broker. Required for any setup where the broker is reachable over a network.

```bash
# Option A: Local dev certificates with mkcert (trusted by your system)
brew install mkcert  # or: apt install mkcert
mkcert -install
mkcert localhost 127.0.0.1 your-broker-hostname

# Option B: Self-signed with openssl
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes \
  -subj "/CN=localhost"
```

Place certificate files in `mosquitto/certs/`:
```
mosquitto/certs/
├── ca.crt
├── server.crt
└── server.key
```

Uncomment the TLS section in `mosquitto/config/mosquitto.conf`:
```
listener 8883
cafile /mosquitto/certs/ca.crt
certfile /mosquitto/certs/server.crt
keyfile /mosquitto/certs/server.key
```

Update agent config: `"broker": "mqtts://your-broker-host:8883"`

---

## 2. Topic-Level ACLs

By default, any authenticated agent can publish to any topic. For stricter setups, Mosquitto supports per-user topic ACLs.

Add to `mosquitto/config/mosquitto.conf`:
```
acl_file /mosquitto/config/acl
```

Create `mosquitto/config/acl`:
```
# Agent "myagent" can read/write its own task topic and broadcast
user myagent
topic readwrite agents/task/myagent
topic read agents/broadcast/#
topic readwrite agents/presence/myagent
topic read agents/presence/#
```

> Restart Mosquitto after ACL changes: `docker compose restart mosquitto`

---

## 3. Firewall Rules

For broker on a dedicated host:

```bash
# Allow MQTT (TLS only) from known agent IP ranges
ufw allow from 192.168.1.0/24 to any port 8883
# Deny unencrypted MQTT from outside localhost
ufw deny 1883
```

---

## 4. Credential Management

- Never commit `mosquitto/passwd` or `*.local.json` (covered by `.gitignore`)
- Rotate passwords by re-running `mosquitto_passwd` and restarting the broker
- `MQTT_USERNAME` / `MQTT_PASSWORD` env-var overrides are not yet supported in `mqtt-channel-mcp` — credentials must be in the config file. Track: [feature request]

---

## 5. Multi-Tenant Considerations

If multiple independent teams share one broker:

- Use separate username/password per team
- Apply ACLs so teams cannot read each other's topics
- Use a topic prefix per team: `team-a/agents/...` vs `team-b/agents/...`
- Consider running separate broker instances (simplest isolation)

---

## Threat Model

| Threat | Mitigation |
|---|---|
| Unauthorized broker access | `allow_anonymous false` + password auth |
| Traffic interception | TLS on port 8883 |
| Agent impersonation | Unique `clientId` per agent + ACLs |
| Credential leakage via git | `.gitignore` covers `*.local.json` and `passwd` |
| Broker as single point of failure | Mosquitto bridge or cluster for HA |
