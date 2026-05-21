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
# 🧮 DYNAMIC DELAY CALCULATION
# ========================================================
HOPS_TO_TARGET=$(( DIAMETER / 2 + 1 ))        
HOPS_WITH_DELAY=$(( HOPS_TO_TARGET - 1 )) 

SECRET_HOP_MS=200       
REVOCATION_HOP_MS=10    

SECRET_ARRIVAL_TIME=$(( HOPS_WITH_DELAY * SECRET_HOP_MS ))
REVOCATION_ARRIVAL_TIME=$(( HOPS_WITH_DELAY * REVOCATION_HOP_MS ))
MAX_DELAY=$(( SECRET_ARRIVAL_TIME - REVOCATION_ARRIVAL_TIME ))

CURL_OVERHEAD=40 
TIPPING_POINT=$(( MAX_DELAY - CURL_OVERHEAD ))
SECRET_WIN_MARGIN=0 
DELAY_MS=$(( TIPPING_POINT + SECRET_WIN_MARGIN ))

[[ $DELAY_MS -lt 0 ]] && DELAY_MS=0
DELAY_SEC=$(awk "BEGIN {print $DELAY_MS/1000}")

echo -e "\n${BOLD}================================================${RESET}"
echo -e "${BOLD}${CYAN} Gossip Cluster — IPFS Rumor vs Anti-Rumor Race${RESET}"
echo -e "${DIM} File Name:         ${SECRET_ID}${RESET}"
echo -e "${DIM} Real CID:          ${CID}${RESET}"
echo -e "${DIM} -> Secret ETA:       ${SECRET_ARRIVAL_TIME}ms ($HOPS_TO_TARGET hops @ ${SECRET_HOP_MS}ms)${RESET}"
echo -e "${DIM} -> Revocation ETA:   ${REVOCATION_ARRIVAL_TIME}ms ($HOPS_TO_TARGET hops @ ${REVOCATION_HOP_MS}ms)${RESET}"
echo -e "${BOLD} COMPUTED DELAY:      ${DELAY_MS}ms${RESET}"
echo -e "${BOLD}================================================${RESET}"

# Wait for cluster
while ! curl -sf "http://${NODE_PORTS[$ENTRY_NODE]}/status" &>/dev/null; do sleep 1; done

# 1. Inject the IPFS Secret
echo -e "\n${YELLOW}➜ Injecting Secret into ${ENTRY_NODE}...${RESET}"
curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"secret_id\":\"${SECRET_ID}\",\"cid\":\"${CID}\"}" \
  "http://${NODE_PORTS[$ENTRY_NODE]}/inject" > /dev/null

# 2. Wait dynamically computed time
echo -e "${DIM}⏳ Waiting ${DELAY_MS}ms...${RESET}"
sleep "$DELAY_SEC"

# 3. Inject the Revocation
echo -e "${RED}🚨 Injecting Revocation (Anti-Rumor) into ${ENTRY_NODE}!${RESET}\n"
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

    fc=$(echo "${final_status[$node]}" | jq --argjson h "$hop" '[.hop_log[]? | select(.hop <= $h and .fragment_id != -1 and .fragment_id != -99) | .fragment_id] | unique | length')
    is_revoked=$(echo "${final_status[$node]}" | jq -r --argjson h "$hop" 'any(.hop_log[]?; .hop <= $h and .fragment_id == -99)')

    if [[ "$is_revoked" == "true" ]]; then
      row+="$(printf '\033[31m%s:[XXX]\033[0m ' "$short_to")"
    elif [[ "$fc" -eq 2 ]]; then
      row+="$(printf '\033[32m%s:[2/2 \xe2\x9c\x93]\033[0m ' "$short_to")"
      recon_count=$((recon_count + 1))
    elif [[ "$fc" -eq 1 ]]; then
      row+="$(printf '\033[33m%s:[1/2  ]\033[0m ' "$short_to")"
    else
      row+="$(printf '\033[2m%s:[0/2  ]\033[0m ' "$short_to")"
    fi
  done
  printf "Hop %-2d   ${row}  (%d/%d reconstructed)\n" "$hop" "$recon_count" "$total_nodes"
  [[ -n "$paths_str" ]] && echo -e "         ↳ ${CYAN}Paths: ${paths_str}${RESET}"
done

echo -e "\n${BOLD}── Final state (all nodes) ──${RESET}"
for node in "${NODES[@]}"; do
  status_json="${final_status[$node]}"
  shares_count=$(echo "$status_json" | jq -r '.share_count // 0')
  node_cid=$(echo "$status_json" | jq -r --arg sid "$SECRET_ID" '.reconstructed[$sid].cid // ""')
  tomb=$(echo "$status_json" | jq -r 'if (.tombstones | length) > 0 then "True" else "False" end')
  
  if [[ "$node_cid" == "$CID" ]]; then
      printf "  %-8s  shares=[%s/2]  cid=${GREEN}%-46s${RESET}  tomb=%s\n" "$node" "$shares_count" "$node_cid" "$tomb"
  else
      printf "  %-8s  shares=[%s/2]  cid=%-46s  tomb=%s\n" "$node" "$shares_count" "${node_cid:-N/A}" "$tomb"
  fi
done

TARGET_STATUS=$(echo "${final_status[$TARGET_NODE]}")
TARGET_SHARES=$(echo "$TARGET_STATUS" | jq -r '.share_count')
IS_TARGET_REVOKED=$(echo "$TARGET_STATUS" | jq -r 'any(.hop_log[]?; .fragment_id == -99)')

echo -e "\n================================================"
if [[ "$IS_TARGET_REVOKED" == "true" ]]; then
    echo -e "${GREEN}✅ SUCCESS: The Anti-Rumor successfully intercepted and purged Target ($TARGET_NODE)!${RESET}"
elif [[ "$TARGET_SHARES" == "2" ]]; then
    echo -e "${RED}❌ FAILED: The Secret reached Target ($TARGET_NODE) before the Anti-Rumor caught it!${RESET}"
    echo -e "You can view/download the original file via the local IPFS gateway:"
    echo -e "${CYAN}http://localhost:8099/ipfs/${CID}${RESET}"
else
    echo -e "${YELLOW}⚠️ TIE: The secret didn't reach the target, but neither did the Anti-Rumor.${RESET}"
fi
echo "================================================"