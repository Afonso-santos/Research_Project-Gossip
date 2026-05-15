package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// NodeConfig holds per-node topology info.
type NodeConfig struct {
	ID        string   `json:"id"`
	Neighbors []string `json:"neighbors"`
}

// Topology is the full topology config file.
type Topology struct {
	Topology string                `json:"topology"`
	Nodes    map[string]NodeConfig `json:"nodes"`
	Target   string                `json:"target"`
	Diameter int                   `json:"diameter"`
}

// Load reads and parses the topology JSON file.
func Load(path string) (*Topology, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading topology: %w", err)
	}
	var t Topology
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("parsing topology: %w", err)
	}
	return &t, nil
}

// TwoHopNeighbors builds the 2-hop neighborhood table for a given node.
// Returns map[neighbor] -> []their_neighbors (i.e. what this node knows about 2 hops away).
func (t *Topology) TwoHopNeighbors(nodeID string) map[string][]string {
	result := make(map[string][]string)
	self, ok := t.Nodes[nodeID]
	if !ok {
		return result
	}
	for _, n1 := range self.Neighbors {
		n1cfg, ok := t.Nodes[n1]
		if !ok {
			continue
		}
		result[n1] = n1cfg.Neighbors
	}
	return result
}

// AllNodeIDs returns sorted node IDs.
func (t *Topology) AllNodeIDs() []string {
	ids := make([]string, 0, len(t.Nodes))
	for id := range t.Nodes {
		ids = append(ids, id)
	}
	// simple sort
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[i] > ids[j] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}
