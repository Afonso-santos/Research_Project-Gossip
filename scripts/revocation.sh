#!/usr/bin/env bash
set -euo pipefail

FILE_PATH="${1:-}"
TOPO_FILE="${2:-topology/topology.json}"

if [[ -z "$FILE_PATH" || ! -f "$FILE_PATH" ]]; then
  echo "❌ ERROR: Please provide a valid file to upload!"
  echo "Usage: ./scripts/revocation.sh <path_to_file> [topology_file]"
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
IPFS_RESP=$(curl -s -X POST -F file=@"$FILE_PATH" "http://localhost:5001/api/v0/add")
CID=$(echo "$IPFS_RESP" | jq -r '.Hash')
SECRET_ID=$(basename "$FILE_PATH") 

if [[ -z "$CID" || "$CID" == "null" ]]; then
    echo "❌ ERROR: Failed to upload to IPFS. Did you add the IPFS node to docker-compose.yml?"
    exit 1
fi

# ========================================================
# 🗺️ DYNAMIC TOPOLOGY PARSING
# ========================================================
TARGET_NODE=$(jq -r '.target' "$TOPO_FILE")
ENTRY_NODE=$(jq -r '.nodes | keys | sort | .[0]' "$TOPO_FILE")
DIAMETER=$(jq -r '.diameter' "$TOPO_FILE")

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

BOLD="\033[1m"; CYAN="\033[36m"; GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"; DIM="\033[2m"; RESET="\033[0m"

# ========================================================
# 🧮 PROBABILISTIC DETECTION DELAY
# ========================================================
# Instead of a strict calculation, we simulate a real-world
# probabilistic "Detection Window".

HOPS_TO_TARGET=$(( DIAMETER / 2 + 1 ))
SECRET_HOP_MS=200       
SECRET_ETA=$(( HOPS_TO_TARGET * SECRET_HOP_MS ))

MIN_DELAY=$(( SECRET_ETA + 100 )) # Wait at least 100ms AFTER it arrives
MAX_DELAY=$(( SECRET_ETA + 500 ))

# Generate a random delay to simulate probabilistically detecting a breach
DELAY_MS=$(( RANDOM % (MAX_DELAY - MIN_DELAY + 1) + MIN_DELAY ))
DELAY_SEC=$(awk "BEGIN {print $DELAY_MS/1000}")

echo -e "\n${BOLD}================================================${RESET}"
echo -e "${BOLD}${CYAN} Gossip Cluster — Probabilistic Anti-Rumor Race${RESET}"
echo -e "${DIM} File Name:         ${SECRET_ID}${RESET}"
echo -e "${DIM} Real CID:          ${CID}${RESET}"
echo -e "${DIM} -> Est. Secret ETA:  ${SECRET_ETA}ms${RESET}"
echo -e "${BOLD} DETECTION DELAY:   ${DELAY_MS}ms (Simulated Probabilistic Reaction)${RESET}"
echo -e "${BOLD}================================================${RESET}"

# Wait for cluster
while ! curl -sf "http://${NODE_PORTS[$ENTRY_NODE]}/status" &>/dev/null; do sleep 1; done

# 1. Inject the IPFS Secret
echo -e "\n${YELLOW}➜ Injecting Secret into ${ENTRY_NODE}...${RESET}"
curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"secret_id\":\"${SECRET_ID}\",\"cid\":\"${CID}\"}" \
  "http://${NODE_PORTS[$ENTRY_NODE]}/inject" > /dev/null

# 2. Wait dynamically computed probabilistic time
echo -e "${DIM}⏳ Waiting ${DELAY_MS}ms...${RESET}"
sleep "$DELAY_SEC"

# 3. Inject the Revocation
echo -e "${RED}🚨 Breach Detected! Injecting Revocation (Anti-Rumor) into ${ENTRY_NODE}!${RESET}\n"
curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"secret_id\":\"${SECRET_ID}\"}" \
  "http://${NODE_PORTS[$ENTRY_NODE]}/revoke" > /dev/null

echo -e "${DIM}Waiting for gossip to settle...${RESET}"
start_ts=$(date +%s); prev_done=-1; same_count=0

get_status() { curl -sf --max-time 2 "http://${NODE_PORTS[$1]}/status" 2>/dev/null || echo '{}'; }

while true; do
  elapsed=$(( $(date +%s) - start_ts ))
  [[ $elapsed -ge $POLL_DURATION ]] && break

  done_count=0
  for node in "${NODES[@]}"; do
    s=$(get_status "$node")
    tomb=$(echo "$s" | jq -r 'if (.tombstones | length) > 0 then "True" else "False" end')
    [[ "$tomb" == "True" ]] && done_count=$((done_count + 1))
  done

  if [[ $done_count -eq $total_nodes ]]; then sleep 0.5; break; fi
  if [[ $done_count -eq $prev_done && $done_count -gt 0 ]]; then
    same_count=$((same_count + 1))
    [[ $same_count -ge 10 ]] && break
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

echo -e "${BOLD}── Propagation (hop-event driven) ──${RESET}"
for (( hop=0; hop<=max_hop; hop++ )); do
  paths_str=""; recon_count=0; row=""

  for node in "${NODES[@]}"; do
    short_to="${node/node/n}"
    node_paths=$(echo "${final_status[$node]}" | jq -r --argjson h "$hop" '.hop_log[]? | select(.hop == $h and .from_node != "init" and .from_node != "init_rev") | "\(.from_node)|\(.fragment_id)"' 2>/dev/null || true)
    for p in $node_paths; do
       IFS='|' read -r from_node frag_id <<< "$p"
       short_from="${from_node/node/n}"
       if [[ "$frag_id" == "-99" ]]; then
           paths_str+="${short_from}-${RED}[REV]${RESET}->${short_to}  "
       else
           paths_str+="${short_from}-[f${frag_id}]->${short_to}  "
       fi
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
  
  if [[ "$node_cid" == "$CID" ]]; then
      printf "  %-8s  shares=[%s/%s]  cid=${GREEN}%-46s${RESET}  tomb=%s\n" "$node" "$shares_count" "$thresh" "$node_cid" "$tomb"
  else
      printf "  %-8s  shares=[%s/%s]  cid=%-46s  tomb=%s\n" "$node" "$shares_count" "$thresh" "${node_cid:-N/A}" "$tomb"
  fi
done

# ========================================================
# 🏅 FINAL EVALUATION FIX
# ========================================================
TARGET_STATUS=$(echo "${final_status[$TARGET_NODE]}")
TARGET_SHARES=$(echo "$TARGET_STATUS" | jq -r '.share_count // 0')
TARGET_THRESH=$(echo "$TARGET_STATUS" | jq -r '.total_shares // 2')
TARGET_TOMB=$(echo "$TARGET_STATUS" | jq -r 'if (.tombstones | length) > 0 then "True" else "False" end')

echo -e "\n================================================"
if [[ "$TARGET_SHARES" -ge "$TARGET_THRESH" && "$TARGET_THRESH" -gt 0 ]]; then
    echo -e "${RED}❌ FAILED: The Secret reached Target ($TARGET_NODE) before the Anti-Rumor caught it!${RESET}"
    echo -e "You can view/download the original file via the local IPFS gateway:"
    echo -e "${CYAN}http://localhost:8099/ipfs/${CID}${RESET}"
elif [[ "$TARGET_TOMB" == "True" ]]; then
    echo -e "${GREEN}✅ SUCCESS: The Anti-Rumor successfully intercepted and purged Target ($TARGET_NODE)!${RESET}"
else
    echo -e "${YELLOW}⚠️ TIE: The secret didn't reach the target, but neither did the Anti-Rumor.${RESET}"
fi
echo "================================================"