#!/usr/bin/env bash
# ============================================================
#  send_secret.sh — Upload a file to IPFS, get the real CID,
#  and inject it into the Gossip cluster dynamically.
# ============================================================

set -euo pipefail

FILE_PATH="${1:-}"
TOPO_FILE="${2:-topology/topology.json}"

if [[ -z "$FILE_PATH" || ! -f "$FILE_PATH" ]]; then
  echo "❌ ERROR: Please provide a valid file to upload!"
  echo "Usage: ./scripts/send_secret.sh <path_to_file> [topology_file]"
  exit 1
fi

if [[ ! -f "$TOPO_FILE" ]]; then
  echo "❌ ERROR: $TOPO_FILE not found!"
  exit 1
fi

# ========================================================
# 📦 1. UPLOAD FILE TO IPFS
# ========================================================
echo "➜ Uploading '$FILE_PATH' to IPFS..."
# Call the IPFS Kubo API running inside Docker on port 5001
IPFS_RESP=$(curl -s -X POST -F file=@"$FILE_PATH" "http://localhost:5001/api/v0/add")
CID=$(echo "$IPFS_RESP" | jq -r '.Hash')
SECRET_ID=$(basename "$FILE_PATH") # Use the file name as the secret ID

if [[ -z "$CID" || "$CID" == "null" ]]; then
    echo "❌ ERROR: Failed to upload to IPFS. Did you add the IPFS node to docker-compose.yml?"
    exit 1
fi

# ========================================================
# 🗺️ 2. DYNAMIC TOPOLOGY PARSING
# ========================================================
TARGET_NODE=$(jq -r '.target' "$TOPO_FILE")
ENTRY_NODE=$(jq -r '.nodes | keys | sort | .[0]' "$TOPO_FILE")

declare -A NODE_PORTS
NODES=()
PORT_COUNTER=8081
for node in $(jq -r '.nodes | keys | sort | .[]' "$TOPO_FILE"); do
    NODES+=("$node")
    NODE_PORTS["$node"]="localhost:$PORT_COUNTER"
    PORT_COUNTER=$((PORT_COUNTER + 1))
done
total_nodes=${#NODES[@]}

POLL_DURATION=10

BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
YELLOW="\033[33m"
DIM="\033[2m"
RESET="\033[0m"

get_status() { curl -sf --max-time 2 "http://${NODE_PORTS[$1]}/status" 2>/dev/null || echo '{}'; }
get_share_info() { echo "$(echo "$1" | jq -r '.share_count // 0') $(echo "$1" | jq -r '.total_shares // 2')"; }

echo -e "\n${BOLD}================================================${RESET}"
echo -e "${BOLD}${CYAN} Gossip Cluster — IPFS Secret Injection${RESET}"
echo -e "${DIM} Entry Node: ${ENTRY_NODE} | Target: ${TARGET_NODE} | Total: ${total_nodes}${RESET}"
echo -e "${BOLD} File Name: ${SECRET_ID}${RESET}"
echo -e "${BOLD} Real CID:  ${GREEN}${CID}${RESET}"
echo -e "${BOLD}================================================${RESET}\n"

echo -e "${DIM}Waiting for cluster to be ready...${RESET}"
while ! curl -sf "http://${NODE_PORTS[$ENTRY_NODE]}/status" &>/dev/null; do sleep 1; done

echo -e "${BOLD}── Injecting real CID into ${ENTRY_NODE} ──${RESET}"
curl -sf -X POST -H "Content-Type: application/json" \
  -d "{\"secret_id\":\"${SECRET_ID}\",\"cid\":\"${CID}\"}" \
  "http://${NODE_PORTS[$ENTRY_NODE]}/inject" > /dev/null

echo -e "\n${BOLD}── Propagation (hop-event driven) ──${RESET}"
start_ts=$(date +%s)
prev_done=-1; same_count=0

while true; do
  elapsed=$(( $(date +%s) - start_ts ))
  [[ $elapsed -ge $POLL_DURATION ]] && break
  done_count=0
  for node in "${NODES[@]}"; do
    s=$(get_status "$node")
    read -r c t <<< "$(get_share_info "$s")"
    [[ "$c" == "$t" && "$c" -gt 0 ]] && done_count=$((done_count + 1))
  done

  if [[ $done_count -eq $total_nodes ]]; then sleep 0.2; break; fi
  if [[ $done_count -eq $prev_done && $done_count -gt 0 ]]; then
    same_count=$((same_count + 1))
    [[ $same_count -ge 5 ]] && break
  else
    same_count=0
  fi
  prev_done=$done_count
  sleep 0.2
done
printf "\033[1A\033[2K" 

declare -A final_status
max_hop=0
for node in "${NODES[@]}"; do
  final_status[$node]=$(get_status "$node")
  node_max=$(echo "${final_status[$node]}" | jq '[.hop_log[]?.hop] | max // -1')
  [[ $node_max -gt $max_hop ]] && max_hop=$node_max
done

for (( hop=0; hop<=max_hop; hop++ )); do
  paths_str=""; recon_count=0; row=""
  for node in "${NODES[@]}"; do
    short_to="${node/node/n}"
    node_paths=$(echo "${final_status[$node]}" | jq -r --argjson h "$hop" '.hop_log[]? | select(.hop == $h and .from_node != "init" and .from_node != "init_rev") | "\(.from_node)|\(.fragment_id)"' 2>/dev/null || true)
    for p in $node_paths; do
       IFS='|' read -r from_node frag_id <<< "$p"
       paths_str+="${from_node/node/n}-[f${frag_id}]->${short_to}  "
    done
    
    # Extract the dynamic threshold for this specific node
    thresh=$(echo "${final_status[$node]}" | jq -r '.total_shares // 2')
    fc=$(echo "${final_status[$node]}" | jq --argjson h "$hop" '[.hop_log[]? | select(.hop <= $h and .fragment_id != -1 and .fragment_id != -99) | .fragment_id] | unique | length')
    is_revoked=$(echo "${final_status[$node]}" | jq -r --argjson h "$hop" 'any(.hop_log[]?; .hop <= $h and .fragment_id == -99)')

    if [[ "$is_revoked" == "true" ]]; then
      row+="$(printf '\033[31m%s:[XXX]\033[0m ' "$short_to")"
    elif [[ "$fc" -ge "$thresh" && "$thresh" -gt 0 ]]; then
      row+="$(printf '\033[32m%s:[%d/%d \xe2\x9c\x93]\033[0m ' "$short_to" "$fc" "$thresh")"
      recon_count=$((recon_count + 1))
    elif [[ "$fc" -gt 0 ]]; then
      row+="$(printf '\033[33m%s:[%d/%d  ]\033[0m ' "$short_to" "$fc" "$thresh")"
    else
      row+="$(printf '\033[2m%s:[0/%d  ]\033[0m ' "$short_to" "$thresh")"
    fi
  done
  printf "Hop %-2d   ${row}  (%d/%d reconstructed)\n" "$hop" "$recon_count" "$total_nodes"
  [[ -n "$paths_str" ]] && echo -e "         ↳ ${CYAN}Paths: ${paths_str}${RESET}"
done

echo -e "\n${BOLD}── Final state (all nodes) ──${RESET}"
for node in "${NODES[@]}"; do
  status_json="${final_status[$node]}"
  shares_count=$(echo "$status_json" | jq -r '.share_count // 0')
  thresh=$(echo "$status_json" | jq -r '.total_shares // 2')
  node_cid=$(echo "$status_json" | jq -r --arg sid "$SECRET_ID" '.reconstructed[$sid].cid // ""')
  tomb=$(echo "$status_json" | jq -r 'if (.tombstones | length) > 0 then "True" else "False" end')
  
  # Highlight the real CID in green when successfully reconstructed
  if [[ "$node_cid" == "$CID" ]]; then
      printf "  %-8s  shares=[%s/%s]  cid=${GREEN}%-46s${RESET}  tomb=%s\n" "$node" "$shares_count" "$thresh" "$node_cid" "$tomb"
  else
      printf "  %-8s  shares=[%s/%s]  cid=%-46s  tomb=%s\n" "$node" "$shares_count" "$thresh" "${node_cid:-N/A}" "$tomb"
  fi
done
echo ""

# Give the final IPFS download link if the target node got it
TARGET_STATUS=$(echo "${final_status[$TARGET_NODE]}")
TARGET_SHARES=$(echo "$TARGET_STATUS" | jq -r '.share_count // 0')
TARGET_THRESH=$(echo "$TARGET_STATUS" | jq -r '.total_shares // 2')

if [[ "$TARGET_SHARES" -ge "$TARGET_THRESH" && "$TARGET_THRESH" -gt 0 ]]; then
    echo -e "${GREEN}🎯 Target ($TARGET_NODE) successfully reconstructed the IPFS CID!${RESET}"
    echo -e "You can view/download the original file via the local IPFS gateway:"
    echo -e "${CYAN}http://localhost:8099/ipfs/${CID}${RESET}\n"
fi