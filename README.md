# Gossip Cluster — AAG P2P Simulation
## Grade: 8/10 ⭐️
A decentralised gossip simulation implementing the **Advanced Adaptive Gossiping (AAG)** protocol with 2-hop neighbourhood routing, **dynamic Shamir's Secret Sharing (SSS)**, **hop-by-hop Verifiable Secret Sharing (VSS)**, and a **probabilistic Anti-Rumor revocation mechanism**. Routes a secret CID from an origin node to a configurable target across Docker-based network topologies.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | ≥ 24.0 |
| Docker Compose | ≥ 2.0 |
| Go | 1.22 (build only) |
| `jq` | any recent |
| `curl` | any recent |

---

## Project Structure

```
gossip-sim/
├── cmd/
│   └── node/
│       └── main.go                  # Entry point — reads env, boots node + HTTP server
├── internal/
│   ├── config/
│   │   └── config.go                # Topology JSON loader + 2-hop table builder
│   ├── gossip/
│   │   └── node.go                  # AAG protocol, SSS, VSS, revocation, goroutine loop
│   └── api/
│       └── server.go                # HTTP handlers: /inject /revoke /gossip /status /members
├── topology/
│   ├── topology.json                # Default topology (edit or swap for experiments)
│   ├── linear10.json                # Linear — 10 nodes
│   ├── ring10.json                  # Ring — 10 nodes
│   ├── ring20.json                  # Ring — 20 nodes
│   ├── mesh15.json                  # Mesh — 15 nodes
│   ├── mesh20.json                  # Mesh — 20 nodes
│   ├── bottleneckSmall.json         # Bottleneck — 10 nodes
│   └── bottleneckLarge.json         # Bottleneck — 15 nodes
├── scripts/
│   ├── send_secret.sh               # Upload file to IPFS → inject CID → poll propagation
│   ├── revocation.sh                # Upload file → inject CID → trigger Anti-Rumor race
│   └── generate_compose.sh          # Regenerate docker-compose.yml from any topology file
├── Dockerfile
├── docker-compose.yml               # Default compose (10-node bottleneck topology)
├── go.mod
├── go.sum
└── secret1.txt                      # Example secret file for testing
```

---

## Quick Start

```bash
# 1. Build and launch all nodes + IPFS
docker compose up -d --build

# 2. Wait ~5s for containers to initialise, then run the injection script
./scripts/send_secret.sh secret1.txt

# 3. Watch the hop-by-hop propagation table in the terminal output
```

---

## Running the Different Experiments

All experiments follow the same two-step pattern: **select a topology → run the appropriate script**.

### Step 1 — Select a Topology

Regenerate `docker-compose.yml` for any topology file using the generator script:

```bash
./scripts/generate_compose.sh topology/<topology_file>.json
```

Then restart the cluster:

```bash
docker compose down && docker compose up -d --build
```

### Step 2 — Run the Experiment

---

### Experiment A — Secret Propagation Only (no revocation)

Tests whether the secret CID successfully propagates from the origin node to the target, and how many intermediate nodes reconstruct it along the way.

```bash
./scripts/send_secret.sh <path_to_file> [topology_file]
```

**Examples:**

```bash
# Linear topology, 10 nodes
./scripts/generate_compose.sh topology/linear10.json
docker compose down && docker compose up -d --build
./scripts/send_secret.sh secret1.txt topology/linear10.json

# Ring topology, 20 nodes
./scripts/generate_compose.sh topology/ring20.json
docker compose down && docker compose up -d --build
./scripts/send_secret.sh secret1.txt topology/ring20.json

# Dense mesh, 20 nodes
./scripts/generate_compose.sh topology/mesh20.json
docker compose down && docker compose up -d --build
./scripts/send_secret.sh secret1.txt topology/mesh20.json
```

**What to observe:** the hop-by-hop table printed in the terminal shows per-node fragment accumulation (`[fragments/threshold ✓]`) and the final state of every node. A green CID in the final state table means that node successfully reconstructed the secret.

---

### Experiment B — Probabilistic Anti-Rumor Revocation Race

Tests whether a revocation command (Anti-Rumor) can overtake in-transit fragments and prevent the target from reconstructing the secret. A probabilistic detection delay is computed automatically based on network diameter.

```bash
./scripts/revocation.sh <path_to_file> [topology_file]
```

**Examples:**

```bash
# Ring 10 — shallow network, revocation expected to FAIL
./scripts/generate_compose.sh topology/ring10.json
docker compose down && docker compose up -d --build
./scripts/revocation.sh secret1.txt topology/ring10.json

# Ring 20 — deeper network, revocation expected to SUCCEED
./scripts/generate_compose.sh topology/ring20.json
docker compose down && docker compose up -d --build
./scripts/revocation.sh secret1.txt topology/ring20.json

# Bottleneck 10 — Multipath Convergence Vulnerability scenario
./scripts/generate_compose.sh topology/bottleneckSmall.json
docker compose down && docker compose up -d --build
./scripts/revocation.sh secret1.txt topology/bottleneckSmall.json
```

**What to observe:** the terminal prints a `DETECTION DELAY` value (the simulated probabilistic reaction window), the hop table with `[REV]` markers showing where the Anti-Rumor swept through, and a final verdict:

```
✅ SUCCESS: The Anti-Rumor successfully intercepted and purged Target (nodeXX)!
❌ FAILED:  The Secret reached Target (nodeXX) before the Anti-Rumor caught it!
⚠️  TIE:    The secret didn't reach the target, but neither did the Anti-Rumor.
```

---

### Experiment C — VSS Tamper Detection

The hop-by-hop Hash-Based VSS is always active. To observe it dropping a corrupted fragment, you can manually POST a gossip message with a mismatched `share_commitment`:

```bash
curl -X POST http://localhost:8081/gossip \
  -H 'Content-Type: application/json' \
  -d '{
    "secret_id": "test-tamper",
    "cid": "",
    "sender": "attacker",
    "hop_count": 1,
    "fragment_id": 1,
    "threshold": 2,
    "data": "dGVzdA==",
    "share_commitment": "0000000000000000000000000000000000000000000000000000000000000000",
    "global_commitment": "",
    "is_revocation": false
  }'
```

Then check the Docker logs for the rejection:

```bash
docker logs node01 2>&1 | grep "VSS DROP"
# Expected: [node01] ❌ VSS DROP: Share 1 from attacker failed cryptographic verification!
```

---

## Topology Reference

| File | Topology | Nodes | Expected Revocation Result |
|------|----------|-------|---------------------------|
| `linear10.json` | Linear | 10 | ✅ Succeeds (single path, sufficient depth) |
| `ring10.json` | Ring | 10 | ❌ Fails (too shallow) |
| `ring20.json` | Ring | 20 | ✅ Succeeds (sufficient depth) |
| `mesh15.json` | Mesh | 15 | ✅ Succeeds (with propagation delay advantage) |
| `mesh20.json` | Mesh | 20 | ⚠️ Variable (dense multipath) |
| `bottleneckSmall.json` | Bottleneck | 10 | ❌ Fails (Multipath Convergence Vulnerability) |
| `bottleneckLarge.json` | Bottleneck | 15 | ❌ Fails (Multipath Convergence Vulnerability) |

---

## Architecture

### System Components

The system consists of five interacting components:

1. **Docker Network Simulation** — isolated containers communicating over a custom `gossip-net` bridge; topology defined at runtime via `topology.json`.
2. **Go Routing Logic** — RESTful HTTP API with buffered channels and per-message goroutines to avoid deadlocks under high throughput.
3. **IPFS Storage Layer** — a local Kubo node (port `5001`) stores the actual payload; only the fixed-size CID travels through the gossip mesh.
4. **Dynamic SSS + VSS** — `k`-out-of-`n` Shamir shares are generated dynamically based on originator node degree (`n = |neighbors|`, `k = n/2 + 1`); each share carries a SHA-256 commitment verified hop-by-hop.
5. **Probabilistic Revocation** — standard fragments carry a 200 ms artificial delay per hop; the Anti-Rumor bypasses AAG filters and propagates to all direct neighbors in 10 ms, creating a structural speed advantage.

### AAG Protocol (2-Hop Neighbourhood)

When node X receives a fragment from sender S:

1. **Mr** = all nodes that have already received the message (S's direct neighbours)
2. **child(X)** = X's neighbours NOT in Mr
3. For each child **cᵢ**, count **parents** = nodes in Mr that are also neighbours of cᵢ
4. Compute forwarding probability:
   - 1 parent → **p = 1.0** (X is the sole relay, must forward)
   - K parents → **(1−p)^K < (1−τᵣₑₗ)** where τᵣₑₗ = τₐᵣₚ^(1/diameter), τₐᵣₚ = 0.99
5. Forward with **p = max(p₁ … pₙ)** across all children

### Disjoint Path Filter

To enforce confidentiality, each fragment is pinned to a lane via:

```
Lᵢ = neighbor_index (mod k)
```

This prevents any single intermediate node from organically accumulating the full threshold — except at structural bottlenecks (see Multipath Convergence Vulnerability in the paper).

### Concurrency Model

- Each node runs a **goroutine message loop** (`processLoop`) reading from a buffered channel (capacity 256)
- Outgoing gossip fires **one goroutine per peer** (non-blocking fan-out)
- All state is protected by `sync.RWMutex`

---

## HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/inject` | Start gossip from this node |
| `POST` | `/revoke` | Trigger Anti-Rumor revocation for a secret |
| `POST` | `/gossip` | Receive a gossip message from a peer |
| `GET` | `/status` | Return full node state (JSON) |
| `GET` | `/members` | Return cluster membership info |

### `/inject` Request

```bash
curl -X POST http://localhost:8081/inject \
  -H 'Content-Type: application/json' \
  -d '{"secret_id":"my-secret-001","cid":"QmYourCIDHere"}'
```

### `/revoke` Request

```bash
curl -X POST http://localhost:8081/revoke \
  -H 'Content-Type: application/json' \
  -d '{"secret_id":"my-secret-001"}'
```

### `/status` Response

```json
{
  "node": "node03",
  "topology": "aag",
  "reconstructed": {
    "secret1.txt": {
      "secret_id": "secret1.txt",
      "cid": "QmRealCIDFromIPFS...",
      "received_at": 1718000000000,
      "threshold": 3,
      "global_commitment": "abc123..."
    }
  },
  "shares_buffered": {},
  "tombstones": [],
  "two_hop": {
    "node01": ["node03", "node04"],
    "node02": ["node03", "node04"]
  },
  "share_count": 3,
  "total_shares": 3,
  "hop_log": [
    { "hop": 1, "from_node": "node01", "secret_id": "secret1.txt", "fragment_id": 2, "timestamp_ms": 1718000000200 }
  ]
}
```

---

## Configuration

### topology.json Schema

```json
{
  "topology": "aag",
  "nodes": {
    "node01": { "id": "node01", "neighbors": ["node02", "node03"] },
    "node02": { "id": "node02", "neighbors": ["node01", "node04"] }
  },
  "target": "node07",
  "diameter": 5
}
```

`diameter` controls the per-hop reliability threshold: τᵣₑₗ = 0.99^(1/diameter). Set it to the longest shortest path in your topology.

### Environment Variables (per container)

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | `node01` | This node's ID |
| `PORT` | `8080` | HTTP listen port |
| `TOPOLOGY_FILE` | `/etc/gossip/topology.json` | Path to topology config |
| `TOTAL_SHARES` | `10` | Legacy override (dynamic SSS takes precedence) |
| `PEERS` | — | Comma-separated `id=host:port` peer list |

---

## IPFS Integration

The local Kubo IPFS node is exposed on:

- **API**: `http://localhost:5001`
- **Gateway**: `http://localhost:8199`

To manually upload a file and retrieve its CID:

```bash
curl -X POST -F file=@secret1.txt http://localhost:5001/api/v0/add
# {"Name":"secret1.txt","Hash":"QmXxx...","Size":"..."}
```

To retrieve a file after successful reconstruction:

```bash
http://localhost:8199/ipfs/<CID>
```

---

## Extending

- **Real dynamic topology discovery** — replace the static `topology.json` with a beacon-based discovery mechanism using the existing 2-hop table infrastructure in `config.go`
- **Reputation matrices** — add per-node misbehaviour scoring in `node.go` to detect and circumvent nodes that retain or duplicate fragments excessively
- **Metadata obfuscation** — wrap the gossip layer with Onion Routing to prevent intermediate nodes from correlating fragments belonging to the same CID
- **Dynamic bottleneck detection** — extend `delayedAAGForward` to monitor local node degree dynamically and adjust lane assignments away from high-centrality nodes
- **Metrics** — add a Prometheus `/metrics` endpoint to `api/server.go`
