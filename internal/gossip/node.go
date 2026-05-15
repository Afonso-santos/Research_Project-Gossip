package gossip

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"gossip-sim/internal/config"
)

const (
	HardcodedCID      = "QmYourCIDHere"
	HardcodedSecretID = "my-secret-001"
	// τarp: target application-level reception probability
	TauARP = 0.99
)

// HopEvent records a localized gossip jump.
type HopEvent struct {
	Hop       int    `json:"hop"`
	FromNode  string `json:"from_node"`
	SecretID  string `json:"secret_id"`
	Timestamp int64  `json:"timestamp_ms"`
}

// Message is the gossip envelope passed between nodes.
type Message struct {
	CID       string `json:"cid"`
	SecretID  string `json:"secret_id"`
	Target    string `json:"target"`
	Sender    string `json:"sender"`
	HopCount  int    `json:"hop_count"`
	Timestamp int64  `json:"timestamp"`
}

// ShareRecord represents a reconstructed share at a node.
type ShareRecord struct {
	SecretID string `json:"secret_id"`
	CID      string `json:"cid"`
	Received int64  `json:"received_at"`
}

// Status is the JSON response for /status.
type Status struct {
	Node           string                 `json:"node"`
	Topology       string                 `json:"topology"`
	Reconstructed  map[string]ShareRecord `json:"reconstructed"`
	SharesBuffered map[string]int         `json:"shares_buffered"`
	Tombstones     []string               `json:"tombstones"`
	TwoHop         map[string][]string    `json:"two_hop"`
	ShareCount     int                    `json:"share_count"`
	TotalShares    int                    `json:"total_shares"`
	HopLog         []HopEvent             `json:"hop_log"`
}

// Node represents a single gossip node.
type Node struct {
	mu sync.RWMutex

	ID          string
	TopologyStr string
	Target      string
	Diameter    int
	Neighbors   []string
	TwoHopTable map[string][]string // neighbor -> their neighbors
	PeerPorts   map[string]string   // nodeID -> "host:port"

	// State
	Reconstructed  map[string]ShareRecord
	SharesBuffered map[string]int
	Tombstones     map[string]bool
	TotalShares    int
	HopLog         []HopEvent

	// Channel for incoming messages
	incoming chan Message
}

// NewNode creates a Node from config.
func NewNode(id string, topo *config.Topology, peerPorts map[string]string, totalShares int) *Node {
	nodeCfg := topo.Nodes[id]
	twoHop := topo.TwoHopNeighbors(id)

	n := &Node{
		ID:             id,
		TopologyStr:    topo.Topology,
		Target:         topo.Target,
		Diameter:       topo.Diameter,
		Neighbors:      nodeCfg.Neighbors,
		TwoHopTable:    twoHop,
		PeerPorts:      peerPorts,
		Reconstructed:  make(map[string]ShareRecord),
		SharesBuffered: make(map[string]int),
		Tombstones:     make(map[string]bool),
		TotalShares:    totalShares,
		HopLog:         make([]HopEvent, 0),
		incoming:       make(chan Message, 256),
	}
	go n.processLoop()
	return n
}

// Inject starts gossip from this node (called on node01).
func (n *Node) Inject(secretID, cid string) {
	n.mu.Lock()
	n.Reconstructed[secretID] = ShareRecord{
		SecretID: secretID,
		CID:      cid,
		Received: time.Now().UnixMilli(),
	}
	n.SharesBuffered[secretID] = n.TotalShares
	
	// Record the injection as Hop 0
	n.HopLog = append(n.HopLog, HopEvent{
		Hop:       0,
		FromNode:  "init",
		SecretID:  secretID,
		Timestamp: time.Now().UnixMilli(),
	})
	n.mu.Unlock()

	msg := Message{
		CID:       cid,
		SecretID:  secretID,
		Target:    n.Target,
		Sender:    n.ID,
		HopCount:  0,
		Timestamp: time.Now().UnixMilli(),
	}
	n.gossipOut(msg)
}

// Receive enqueues an incoming message.
func (n *Node) Receive(msg Message) {
	n.incoming <- msg
}

// processLoop handles incoming messages with goroutines/channels.
func (n *Node) processLoop() {
	for msg := range n.incoming {
		n.handleMessage(msg)
	}
}

// handleMessage implements the AAG 2-hop neighborhood algorithm.
func (n *Node) handleMessage(msg Message) {
	n.mu.Lock()

	// Deduplicate: already have it?
	if _, seen := n.Reconstructed[msg.SecretID]; seen {
		n.mu.Unlock()
		return
	}
	if n.Tombstones[msg.SecretID] {
		n.mu.Unlock()
		return
	}

	// Record receipt
	n.Reconstructed[msg.SecretID] = ShareRecord{
		SecretID: msg.SecretID,
		CID:      msg.CID,
		Received: time.Now().UnixMilli(),
	}
	n.SharesBuffered[msg.SecretID] = n.TotalShares

	// Record the event hop log
	n.HopLog = append(n.HopLog, HopEvent{
		Hop:       msg.HopCount,
		FromNode:  msg.Sender,
		SecretID:  msg.SecretID,
		Timestamp: time.Now().UnixMilli(),
	})

	isTarget := n.ID == msg.Target
	if isTarget {
		// Target node: tombstone, stop forwarding
		n.Tombstones[msg.SecretID] = true
		log.Printf("[%s] 🎯 TARGET REACHED — tombstoning %s", n.ID, msg.SecretID)
		n.mu.Unlock()
		return
	}

	// Build forwarding message
	fwdMsg := Message{
		CID:       msg.CID,
		SecretID:  msg.SecretID,
		Target:    msg.Target,
		Sender:    n.ID,
		HopCount:  msg.HopCount + 1,
		Timestamp: time.Now().UnixMilli(),
	}

	// AAG: compute which neighbors DIDN'T receive from sender
	// Mr = neighbors of sender (nodes that received msg from sender)
	senderNeighbors := n.twoHopNeighborsOf(msg.Sender)
	mr := toSet(senderNeighbors)
	mr[msg.Sender] = true // sender itself

	n.mu.Unlock()

	// Determine child nodes (my neighbors NOT in Mr)
	var children []string
	for _, nb := range n.Neighbors {
		if !mr[nb] {
			children = append(children, nb)
		}
	}

	if len(children) == 0 {
		// No children that need this — still forward with base probability
		p := n.baseProbability()
		if rand.Float64() < p {
			n.gossipOut(fwdMsg)
		}
		return
	}

	// For each child, compute parent count (nodes in Mr that are also neighbors of child)
	maxProb := 0.0
	for _, child := range children {
		childNeighbors := n.twoHopNeighborsOf(child)
		parentCount := 0
		for _, cn := range childNeighbors {
			if mr[cn] {
				parentCount++
			}
		}
		if parentCount == 0 {
			parentCount = 1
		}
		p := n.aagProbability(parentCount)
		if p > maxProb {
			maxProb = p
		}
	}

	// Forward with max probability
	if rand.Float64() < maxProb {
		n.gossipOut(fwdMsg)
	}
}

// aagProbability computes p_forward per the AAG formula.
func (n *Node) aagProbability(parentCount int) float64 {
	if parentCount <= 1 {
		return 1.0
	}
	delta := float64(n.Diameter)
	if delta < 1 {
		delta = 1
	}
	// τrel: per-hop reliability s.t. τrel^δ = τarp
	tauRel := math.Pow(TauARP, 1.0/delta)
	// p_forward s.t. (1-p)^K = (1-τrel)
	p := 1.0 - math.Pow(1.0-tauRel, 1.0/float64(parentCount))
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (n *Node) baseProbability() float64 {
	return n.aagProbability(len(n.Neighbors))
}

// gossipOut sends message to all 1-hop neighbors concurrently.
func (n *Node) gossipOut(msg Message) {
	for _, nb := range n.Neighbors {
		go func(peer string) {
			addr, ok := n.PeerPorts[peer]
			if !ok {
				return
			}
			body, _ := json.Marshal(msg)
			url := fmt.Sprintf("http://%s/gossip", addr)
			resp, err := http.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf("[%s] -> %s gossip error: %v", n.ID, peer, err)
				return
			}
			resp.Body.Close()
		}(nb)
	}
}

// twoHopNeighborsOf returns the known neighbors of a given node from the 2-hop table.
func (n *Node) twoHopNeighborsOf(nodeID string) []string {
	if nodeID == n.ID {
		return n.Neighbors
	}
	return n.TwoHopTable[nodeID]
}

// GetStatus returns a snapshot of current node state.
func (n *Node) GetStatus() Status {
	n.mu.RLock()
	defer n.mu.RUnlock()

	tombList := make([]string, 0, len(n.Tombstones))
	for k := range n.Tombstones {
		tombList = append(tombList, k)
	}

	shareCount := 0
	if _, ok := n.Reconstructed[HardcodedSecretID]; ok {
		shareCount = n.TotalShares
	}

	// Deep copy maps & slices
	rec := make(map[string]ShareRecord, len(n.Reconstructed))
	for k, v := range n.Reconstructed {
		rec[k] = v
	}
	buf := make(map[string]int, len(n.SharesBuffered))
	for k, v := range n.SharesBuffered {
		buf[k] = v
	}
	twoHop := make(map[string][]string, len(n.TwoHopTable))
	for k, v := range n.TwoHopTable {
		cp := make([]string, len(v))
		copy(cp, v)
		twoHop[k] = cp
	}
	hopLog := make([]HopEvent, len(n.HopLog))
	copy(hopLog, n.HopLog)

	return Status{
		Node:           n.ID,
		Topology:       n.TopologyStr,
		Reconstructed:  rec,
		SharesBuffered: buf,
		Tombstones:     tombList,
		TwoHop:         twoHop,
		ShareCount:     shareCount,
		TotalShares:    n.TotalShares,
		HopLog:         hopLog,
	}
}

// helpers
func toSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}