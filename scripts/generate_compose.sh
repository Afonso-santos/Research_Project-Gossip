#!/usr/bin/env bash
# ============================================================
#  generate_compose.sh — Creates docker-compose.yml dynamically
#  based on the nodes defined in topology.json + an IPFS node
# ============================================================
set -euo pipefail

TOPO_FILE=${1:-"topology/topology.json"}
OUT_FILE="docker-compose.yml"

if [[ ! -f "$TOPO_FILE" ]]; then
  echo "Error: Topology file $TOPO_FILE not found."
  exit 1
fi

echo "📖 Reading $TOPO_FILE..."

# 1. Convert to absolute path so Docker Compose ALWAYS treats it as a file mount
ABS_TOPO_FILE=$(cd "$(dirname "$TOPO_FILE")" && pwd)/$(basename "$TOPO_FILE")

# Extract sorted node names (e.g., node01, node02, ...)
NODES=$(jq -r '.nodes | keys | sort | .[]' "$TOPO_FILE")

# Generate the PEERS string
PEERS=""
for node in $NODES; do
  PEERS="${PEERS}${node}=${node}:8080,"
done
PEERS=${PEERS%,}

echo "⚙️ Generating $OUT_FILE..."

# Write the header AND the new IPFS service (Version attribute removed)
cat <<EOF > "$OUT_FILE"
x-peers: &peers
  PEERS: >-
    $PEERS

x-common: &common
  build: .
  volumes:
    - ${ABS_TOPO_FILE}:/etc/gossip/topology.json:ro
  environment:
    <<: *peers
    TOPOLOGY_FILE: /etc/gossip/topology.json
    TOTAL_SHARES: "10"
  networks:
    - gossip-net
  restart: unless-stopped

services:
  # ── Local IPFS Node ──
  ipfs:
    image: ipfs/kubo:latest
    container_name: ipfs_node
    ports:
      - "5001:5001" # IPFS API
      - "8099:8080" # IPFS Gateway
    networks:
      - gossip-net
    restart: unless-stopped

EOF

# Generate Gossip services sequentially
PORT=8081
for node in $NODES; do
  cat <<EOF >> "$OUT_FILE"
  $node:
    <<: *common
    container_name: $node
    hostname: $node
    environment:
      <<: *peers
      NODE_ID: $node
      PORT: "8080"
      TOPOLOGY_FILE: /etc/gossip/topology.json
      TOTAL_SHARES: "10"
    ports:
      - "$PORT:8080"

EOF
  PORT=$((PORT + 1))
done

# Write the footer
cat <<EOF >> "$OUT_FILE"
networks:
  gossip-net:
    driver: bridge
EOF

echo "✅ Success! Generated $OUT_FILE with an IPFS node and $(echo "$NODES" | wc -w) gossip nodes."