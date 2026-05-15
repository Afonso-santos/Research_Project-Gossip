# Gossip Cluster — AAG P2P Simulation

A 10-node decentralised gossip simulation implementing the **Advanced Adaptive Gossiping (AAG)** protocol with 2-hop neighbourhood routing. Routes a secret CID from Node 1 → Node 7 across a configurable topology using Docker Compose.

---

## Quick Start

```bash
# 1. Build & launch all 10 nodes
docker compose up -d --build

# 2. Wait ~5s for nodes to start, then run the injection script
./scripts/send_secret.sh QmYourCIDHere my-secret-001
```

---

## Project Structure

```
gossip-sim/
├── cmd/
│   └── node/
│       └── main.go              # Entry point — reads env, boots node + HTTP server
├── internal/
│   ├── config/
│   │   └── config.go            # Topology JSON loader + 2-hop table builder
│   ├── gossip/
│   │   └── node.go              # AAG protocol, goroutine message loop, state
│   └── api/
│       └── server.go            # HTTP handlers: /inject /gossip /status /members
├── topology/
│   └── topology.json            # Network graph definition
├── scripts/
│   └── send_secret.sh           # Inject + poll observability script
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── README.md
```

---

## Architecture

### AAG Protocol (2-Hop Neighbourhood)

Each node periodically knows its 2-hop neighbourhood via the topology config. When node X receives a message from sender S:

1. **Mr** = all nodes that received the message from S (= S's direct neighbours)
2. **child(X)** = X's neighbours NOT in Mr (nodes that still need the message)
3. For each child **cᵢ**, count **parents** = nodes in Mr that are also neighbours of cᵢ
4. Compute per-child forwarding probability:
   - If 1 parent → **p = 1.0** (X is the sole relay, must forward)
   - If K parents → **(1−p)^K < (1−τᵣₑₗ)** where τᵣₑₗ = τₐᵣₚ^(1/δ)
5. Forward with **p = max(p₁…pₙ)** across all children

This ensures coverage without flooding, adapting dynamically to topology density.

### Target Behaviour

Node 7 is hardcoded as the **target**. Upon receipt it:
- Records the secret
- Sets a **tombstone** on the secret ID
- Stops forwarding (halts gossip chain)

### Concurrency Model

- Each node runs a **goroutine message loop** (`processLoop`) reading from a buffered channel
- Outgoing gossip fires **one goroutine per peer** (non-blocking fan-out)
- State protected by `sync.RWMutex`

---

## HTTP API

| Method | Endpoint   | Description                                     |
|--------|------------|-------------------------------------------------|
| POST   | `/inject`  | Start gossip from this node                     |
| POST   | `/gossip`  | Receive a gossip message from a peer            |
| GET    | `/status`  | Return full node state (JSON)                   |
| GET    | `/members` | Return cluster membership info                  |

### `/status` Response

```json
{
  "node": "node03",
  "topology": "aag",
  "reconstructed": {
    "my-secret-001": {
      "secret_id": "my-secret-001",
      "cid": "QmYourCIDHere",
      "received_at": 1718000000000
    }
  },
  "shares_buffered": { "my-secret-001": 10 },
  "tombstones": [],
  "two_hop": {
    "node01": ["node03", "node04"],
    "node02": ["node03", "node04"]
  },
  "share_count": 10,
  "total_shares": 10
}
```

### `/inject` Request

```bash
curl -X POST http://localhost:8081/inject \
  -H 'Content-Type: application/json' \
  -d '{"secret_id":"my-secret-001","cid":"QmYourCIDHere"}'
```

---

## Configuration

### topology.json

```json
{
  "topology": "aag",
  "nodes": {
    "node01": { "id": "node01", "neighbors": ["node03", "node04"] },
    ...
  },
  "target": "node07",
  "diameter": 5
}
```

**diameter** controls the per-hop reliability calculation: τᵣₑₗ = 0.99^(1/diameter). Larger networks need a higher diameter value.

### Environment Variables (per container)

| Variable        | Default                   | Description                      |
|-----------------|---------------------------|----------------------------------|
| `NODE_ID`       | `node01`                  | This node's ID                   |
| `PORT`          | `8080`                    | HTTP listen port                 |
| `TOPOLOGY_FILE` | `/etc/gossip/topology.json` | Path to topology config         |
| `TOTAL_SHARES`  | `10`                      | Simulated share count            |
| `PEERS`         | —                         | Comma-separated `id=host:port`   |

---

## Observability Script

```
./scripts/send_secret.sh QmYourCIDHere my-secret-001
================================================
 Gossip Cluster — Secret Injection
 Topology:  aag
 CID:       QmYourCIDHere
 Secret ID: my-secret-001
================================================

── Cluster members ──
{"count":10,"members":["node01",..."node10"],"node":"node01","topology":"aag"}

── Injecting secret into node01 ──
{"secret_id":"my-secret-001","shares":10,"status":"gossiped","threshold":3,"topology":"aag"}

── Propagation (polling every 1s for 10s) ──
T+ 1s  n01:[10/10 ✓] n02:[0/10  ] n03:[0/10  ] ...   (1/10 reconstructed)
T+ 2s  n01:[10/10 ✓] n02:[10/10 ✓] n03:[10/10 ✓] ... (3/10 reconstructed)
...

── Final state (all nodes) ──
  node01    shares=[10/10]  cid=QmYourCIDHere   tomb=False
  node07    shares=[10/10]  cid=QmYourCIDHere   tomb=True   ← target tombstoned
```

---

## Extending

- **Real IPFS**: Replace `HardcodedCID` / `HardcodedSecretID` in `gossip/node.go` with actual `ipfs add` calls via the Kubo HTTP API
- **Shamir Secret Sharing**: Replace `SharesBuffered` with real SSS shares (e.g. `github.com/corvus-ch/shamir`)
- **Dynamic Topology**: Replace `topology.json` with a beacon-based discovery mechanism using the existing 2-hop table infrastructure
- **Metrics**: Add a Prometheus `/metrics` endpoint to `api/server.go`
