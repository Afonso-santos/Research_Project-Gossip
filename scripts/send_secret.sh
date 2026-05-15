#!/usr/bin/env bash
# ============================================================
#  send_secret.sh — Inject a secret into node01 and parse
#  the localized HopLogs to track propagation rounds.
#
#  Usage:  ./scripts/send_secret.sh [CID] [SECRET_ID]
#  Example: ./scripts/send_secret.sh QmYourCIDHere my-secret-001
# ============================================================

set -euo pipefail

# ── Hardcoded defaults (match the Go constants) ──────────────────
DEFAULT_CID="QmYourCIDHere"
DEFAULT_SECRET="my-secret-001"

CID="${1:-$DEFAULT_CID}"
SECRET_ID="${2:-$DEFAULT_SECRET}"

# ── Node port map (host:port for each node) ──────────────────────
declare -A NODE_PORTS=(
  [node01]="localhost:8081"
  [node02]="localhost:8082"
  [node03]="localhost:8083"
  [node04]="localhost:8084"
  [node05]="localhost:8085"
  [node06]="localhost:8086"
  [node07]="localhost:8087"
  [node08]="localhost:8088"
  [node09]="localhost:8089"
  [node10]="localhost:8090"
)

NODES=(node01 node02 node03 node04 node05 node06 node07 node08 node09 node10)
POLL_DURATION=10

# ── Colors & formatting ──────────────────────────────────────────
BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
YELLOW="\033[33m"
DIM="\033[2m"
RESET="\033[0m"

check_deps() {
  for cmd in curl jq; do
    if ! command -v "$cmd" &>/dev/null; then
      echo "ERROR: '$cmd' is required but not installed." >&2
      exit 1
    fi
  done
}

wait_for_cluster() {
  echo -e "${DIM}Waiting for cluster to be ready...${RESET}"
  local attempts=0
  while true; do
    if curl -sf "http://${NODE_PORTS[node01]}/status" &>/dev/null; then
      break
    fi
    attempts=$((attempts + 1))
    if [[ $attempts -ge 30 ]]; then
      echo "ERROR: node01 not reachable after 30s. Is the cluster running?" >&2
      echo "  Run: docker compose up -d" >&2
      exit 1
    fi
    sleep 1
    printf "."
  done
  echo ""
}

get_status() {
  local node="$1"
  local addr="${NODE_PORTS[$node]}"
  curl -sf --max-time 2 "http://${addr}/status" 2>/dev/null || echo '{}'
}

get_share_info() {
  local status_json="$1"
  local total
  total=$(echo "$status_json" | jq -r '.total_shares // 10')
  local count
  count=$(echo "$status_json" | jq -r '.share_count // 0')
  echo "$count $total"
}

# ── Banner ───────────────────────────────────────────────────────
print_banner() {
  echo ""
  echo -e "${BOLD}================================================${RESET}"
  echo -e "${BOLD}${CYAN} Gossip Cluster — Secret Injection${RESET}"
  echo -e "${BOLD} Topology:  aag${RESET}"
  echo -e "${BOLD} CID:       ${CID}${RESET}"
  echo -e "${BOLD} Secret ID: ${SECRET_ID}${RESET}"
  echo -e "${BOLD}================================================${RESET}"
  echo ""
}

# ── Main ─────────────────────────────────────────────────────────
check_deps
print_banner
wait_for_cluster

# Cluster members
echo -e "${BOLD}── Cluster members ──${RESET}"
members_json=$(curl -sf "http://${NODE_PORTS[node01]}/members" 2>/dev/null || echo '{}')
echo "$members_json" | jq -c .
echo ""

# Inject secret into node01
echo -e "${BOLD}── Injecting secret into node01 ──${RESET}"
inject_resp=$(curl -sf -X POST \
  -H "Content-Type: application/json" \
  -d "{\"secret_id\":\"${SECRET_ID}\",\"cid\":\"${CID}\"}" \
  "http://${NODE_PORTS[node01]}/inject" 2>/dev/null || echo '{"error":"inject failed"}')
echo "$inject_resp" | jq -c .
echo ""

# ── Propagation (Event-Driven Replay) ────────────────────────────
echo -e "${BOLD}── Propagation (hop-event driven) ──${RESET}"

total_nodes=${#NODES[@]}
start_ts=$(date +%s)

# 1. Wait until gossip has settled in real-time
echo -e "${DIM}Waiting for gossip to settle...${RESET}"
prev_done=-1
same_count=0
while true; do
  elapsed=$(( $(date +%s) - start_ts ))
  [[ $elapsed -ge $POLL_DURATION ]] && break

  done_count=0
  for node in "${NODES[@]}"; do
    s=$(get_status "$node")
    read -r c t <<< "$(get_share_info "$s")"
    [[ "$c" == "$t" && "$c" -gt 0 ]] && done_count=$((done_count + 1))
  done

  # Break if everyone has it
  if [[ $done_count -eq $total_nodes ]]; then
    sleep 0.2 # Brief buffer for final logs to flush
    break
  fi

  # Break if network stalled (no new nodes got it for 1 second)
  if [[ $done_count -eq $prev_done && $done_count -gt 0 ]]; then
    same_count=$((same_count + 1))
    [[ $same_count -ge 5 ]] && break
  else
    same_count=0
  fi
  prev_done=$done_count

  sleep 0.2
done
# Erase the "waiting" line
printf "\033[1A\033[2K" 

# 2. Fetch final status snapshot once for all nodes
declare -A final_status
for node in "${NODES[@]}"; do
  final_status[$node]=$(get_status "$node")
done

# Find the max hop number across all nodes
max_hop=0
for node in "${NODES[@]}"; do
  node_max=$(echo "${final_status[$node]}" | jq '[.hop_log[].hop] | max // -1')
  [[ $node_max -gt $max_hop ]] && max_hop=$node_max
done

# Cumulative state tracking
declare -A reconstructed
for node in "${NODES[@]}"; do
  reconstructed[$node]=0
done

# 3. Iterate hop by hop, building the chronological view
for (( hop=0; hop<=max_hop; hop++ )); do

  # For each node, check if it has a hop_log entry at this exact hop
  for node in "${NODES[@]}"; do
    has_event=$(echo "${final_status[$node]}" \
      | jq --argjson h "$hop" '[.hop_log[].hop] | map(select(. == $h)) | length')
    [[ "$has_event" -gt 0 ]] && reconstructed[$node]=1
  done

  # Count cumulative reconstructed nodes
  recon_count=0
  for node in "${NODES[@]}"; do
    [[ "${reconstructed[$node]}" -eq 1 ]] && recon_count=$((recon_count + 1))
  done

  # Build the row using cumulative state (not live status)
  row=""
  for node in "${NODES[@]}"; do
    short="${node/node/n}"
    if [[ "${reconstructed[$node]}" -eq 1 ]]; then
      row+="$(printf '\033[32m%s:[10/10 \xe2\x9c\x93]\033[0m ' "$short")"
    else
      row+="$(printf '\033[2m%s:[0/10  ]\033[0m ' "$short")"
    fi
  done

  printf "Hop %-2d   ${row}  (%d/%d reconstructed)\n" \
    "$hop" "$recon_count" "$total_nodes"
done

unset final_status reconstructed

if [[ $done_count -eq $total_nodes ]]; then
  echo ""
  echo -e "${GREEN}${BOLD}All nodes have received the secret!${RESET}"
else
  echo ""
  echo -e "${YELLOW}Gossip settled, but only ${done_count}/${total_nodes} nodes received the secret.${RESET}"
fi

# Final state summary
echo ""
echo -e "${BOLD}── Final state (all nodes) ──${RESET}"
for node in "${NODES[@]}"; do
  status_json=$(get_status "$node")
  shares_count=$(echo "$status_json" | jq -r '.share_count // 0')
  total_s=$(echo "$status_json" | jq -r '.total_shares // 10')
  cid=$(echo "$status_json" | jq -r --arg sid "$SECRET_ID" \
    '.reconstructed[$sid].cid // ""')
  tomb=$(echo "$status_json" | jq -r \
    'if (.tombstones | length) > 0 then "True" else "False" end')

  printf "  %-8s  shares=[%s/%s]  cid=%-16s  tomb=%s\n" \
    "$node" "$shares_count" "$total_s" "${cid:-N/A}" "$tomb"
done

echo ""
echo -e "${DIM}Simulation complete. Run 'docker compose logs' for node-level detail.${RESET}"
echo ""