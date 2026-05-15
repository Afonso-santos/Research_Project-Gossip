package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"gossip-sim/internal/api"
	"gossip-sim/internal/config"
	"gossip-sim/internal/gossip"
)

func main() {
	// --- Configuration from environment ---
	nodeID := getEnv("NODE_ID", "node01")
	port := getEnv("PORT", "8080")
	topoFile := getEnv("TOPOLOGY_FILE", "/etc/gossip/topology.json")
	totalShares, _ := strconv.Atoi(getEnv("TOTAL_SHARES", "10"))
	if totalShares == 0 {
		totalShares = 10
	}

	// Load topology
	topo, err := config.Load(topoFile)
	if err != nil {
		log.Fatalf("failed to load topology: %v", err)
	}

	// Build peer port map from env: PEERS=node01=node01:8080,node02=node02:8081,...
	peerPorts := buildPeerPorts(nodeID)

	// Create gossip node
	node := gossip.NewNode(nodeID, topo, peerPorts, totalShares)

	// Create and start HTTP server
	srv := api.New(node)

	addr := fmt.Sprintf(":%s", port)
	log.Printf("[%s] Starting gossip node on %s | topology=%s | neighbors=%v",
		nodeID, addr, topo.Topology, topo.Nodes[nodeID].Neighbors)

	// Print startup JSON for observability
	startupInfo := map[string]interface{}{
		"node":      nodeID,
		"port":      port,
		"topology":  topo.Topology,
		"neighbors": topo.Nodes[nodeID].Neighbors,
		"target":    topo.Target,
		"peers":     peerPorts,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(startupInfo)

	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatalf("http server: %v", err)
	}
}

// buildPeerPorts reads PEERS env var: "node01=node01:8080,node02=node02:8081"
func buildPeerPorts(selfID string) map[string]string {
	peersEnv := getEnv("PEERS", "")
	result := make(map[string]string)
	if peersEnv == "" {
		return result
	}
	for _, pair := range strings.Split(peersEnv, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		id := strings.TrimSpace(parts[0])
		addr := strings.TrimSpace(parts[1])
		if id != selfID {
			result[id] = addr
		}
	}
	return result
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
