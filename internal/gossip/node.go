package gossip

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"math"
	mathrand "math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"gossip-sim/internal/config"
)

const (
	HardcodedCID      = "QmYourCIDHere"
	HardcodedSecretID = "my-secret-001"
	TauARP            = 0.99
)

// HopEvent records a localized gossip jump.
type HopEvent struct {
	Hop        int    `json:"hop"`
	FromNode   string `json:"from_node"`
	SecretID   string `json:"secret_id"`
	FragmentID int    `json:"fragment_id"`
	Timestamp  int64  `json:"timestamp_ms"`
}

type Message struct {
	CID          string `json:"cid"`
	SecretID     string `json:"secret_id"`
	Target       string `json:"target"`
	Sender       string `json:"sender"`
	HopCount     int    `json:"hop_count"`
	Timestamp    int64  `json:"timestamp"`
	FragmentID   int    `json:"fragment_id"`
	Data         []byte `json:"data"`
	IsRevocation bool   `json:"is_revocation"` // <-- NEW: Flags this as an Anti-Rumor
}

type ShareRecord struct {
	SecretID  string         `json:"secret_id"`
	CID       string         `json:"cid"`
	Received  int64          `json:"received_at"`
	Fragments map[int][]byte `json:"-"`
}

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

type Node struct {
	mu sync.RWMutex

	ID          string
	TopologyStr string
	Target      string
	Diameter    int
	Neighbors   []string
	TwoHopTable map[string][]string
	PeerPorts   map[string]string

	Reconstructed  map[string]ShareRecord
	SharesBuffered map[string]int
	Tombstones     map[string]bool
	TotalShares    int
	HopLog         []HopEvent

	incoming chan Message
}

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

// Inject starts gossip from this node.
func (n *Node) Inject(secretID, cid string) {
	n.mu.Lock()

	frag0 := make([]byte, len(cid))
	rand.Read(frag0)

	frag1 := make([]byte, len(cid))
	for i := 0; i < len(cid); i++ {
		frag1[i] = cid[i] ^ frag0[i]
	}

	n.Reconstructed[secretID] = ShareRecord{
		SecretID: secretID,
		CID:      cid,
		Received: time.Now().UnixMilli(),
		Fragments: map[int][]byte{
			0: frag0,
			1: frag1,
		},
	}

	n.HopLog = append(n.HopLog, HopEvent{
		Hop:        0,
		FromNode:   "init",
		SecretID:   secretID,
		FragmentID: 0,
		Timestamp:  time.Now().UnixMilli(),
	})
	n.HopLog = append(n.HopLog, HopEvent{
		Hop:        0,
		FromNode:   "init",
		SecretID:   secretID,
		FragmentID: 1,
		Timestamp:  time.Now().UnixMilli(),
	})
	n.mu.Unlock()

	half := len(n.Neighbors) / 2
	if half == 0 {
		half = 1
	}

	group0 := n.Neighbors[:half]
	group1 := n.Neighbors[half:]

	msg0 := Message{
		SecretID:   secretID,
		Target:     n.Target,
		Sender:     n.ID,
		HopCount:   1,
		Timestamp:  time.Now().UnixMilli(),
		FragmentID: 0,
		Data:       frag0,
	}
	n.gossipOutTo(msg0, group0)

	msg1 := Message{
		SecretID:   secretID,
		Target:     n.Target,
		Sender:     n.ID,
		HopCount:   1,
		Timestamp:  time.Now().UnixMilli(),
		FragmentID: 1,
		Data:       frag1,
	}
	n.gossipOutTo(msg1, group1)
}

// NEW: Revoke acts as an Anti-Rumor to hunt down and kill a secret
func (n *Node) Revoke(secretID string) {
	n.mu.Lock()
	
	// Find the current max hop so the Anti-Rumor aligns chronologically in the bash logs
	maxHop := 0
	for _, ev := range n.HopLog {
		if ev.SecretID == secretID && ev.Hop > maxHop {
			maxHop = ev.Hop
		}
	}
	startRevokeHop := maxHop + 1

	// 1. Instantly delete any fragments we hold locally
	delete(n.Reconstructed, secretID)
	n.SharesBuffered[secretID] = 0

	// 2. Permanently tombstone it so we never accept it again
	n.Tombstones[secretID] = true

	// 3. Log the start of the Anti-Rumor in the HopLog so the script sees it
	n.HopLog = append(n.HopLog, HopEvent{
		Hop:        startRevokeHop,
		FromNode:   "init_rev",
		SecretID:   secretID,
		FragmentID: -99, // -99 is our special marker for "Revocation"
		Timestamp:  time.Now().UnixMilli(),
	})
	n.mu.Unlock()

	// 4. Create the Anti-Rumor message
	revokeMsg := Message{
		SecretID:     secretID,
		Target:       n.Target,
		Sender:       n.ID,
		HopCount:     startRevokeHop + 1,
		Timestamp:    time.Now().UnixMilli(),
		FragmentID:   -99,
		IsRevocation: true, 
	}

	// 5. Shout it to ALL neighbors (Bypassing disjoint filters)
	n.gossipOutTo(revokeMsg, n.Neighbors)
}

func (n *Node) Receive(msg Message) {
	n.incoming <- msg
}

func (n *Node) processLoop() {
	for msg := range n.incoming {
		n.handleMessage(msg)
	}
}
func (n *Node) handleMessage(msg Message) {
	n.mu.Lock()

	// ====================================================================
	// REVOCATION INTERCEPTOR (Anti-Rumor handling)
	// ====================================================================
	if msg.IsRevocation {
		// If already tombstoned, ignore to prevent infinite anti-rumor loops
		if n.Tombstones[msg.SecretID] {
			n.mu.Unlock()
			return
		}

		log.Printf("[%s] 🚨 REVOCATION RECEIVED for %s. Purging fragments!", n.ID, msg.SecretID)

		delete(n.Reconstructed, msg.SecretID)
		n.SharesBuffered[msg.SecretID] = 0
		n.Tombstones[msg.SecretID] = true

		// LOG THE ANTI-RUMOR HOP SO THE SCRIPT CAN SEE IT!
		n.HopLog = append(n.HopLog, HopEvent{
			Hop:        msg.HopCount,
			FromNode:   msg.Sender,
			SecretID:   msg.SecretID,
			FragmentID: -99,
			Timestamp:  time.Now().UnixMilli(),
		})
		n.mu.Unlock()

		// Forward the Anti-Rumor aggressively to all neighbors
		fwdMsg := msg
		fwdMsg.Sender = n.ID
		fwdMsg.HopCount = msg.HopCount + 1
		
		// Fast lane for Anti-Rumors (10ms instead of 200ms)
		go func() {
			time.Sleep(10 * time.Millisecond) 
			n.gossipOutTo(fwdMsg, n.Neighbors)
		}()
		return
	}

	// Standard Fragment processing continues here...
	if n.Tombstones[msg.SecretID] {
		n.mu.Unlock()
		return
	}

	record, exists := n.Reconstructed[msg.SecretID]
	if !exists {
		record = ShareRecord{
			SecretID:  msg.SecretID,
			Received:  time.Now().UnixMilli(),
			Fragments: make(map[int][]byte),
		}
	} else if record.Fragments == nil {
		record.Fragments = make(map[int][]byte)
	}

	if _, seenFrag := record.Fragments[msg.FragmentID]; seenFrag {
		n.mu.Unlock()
		return
	}

	record.Fragments[msg.FragmentID] = msg.Data
	isTarget := n.ID == msg.Target

	if len(record.Fragments) == 2 && record.CID == "" {
		f0 := record.Fragments[0]
		f1 := record.Fragments[1]
		recovered := make([]byte, len(f0))
		for i := 0; i < len(f0); i++ {
			recovered[i] = f0[i] ^ f1[i]
		}
		record.CID = string(recovered)

		if isTarget {
			n.Tombstones[msg.SecretID] = true
			log.Printf("[%s] 🎯 TARGET REACHED — Reconstructed CID: %s, tombstoning %s", n.ID, record.CID, msg.SecretID)
		}
	}

	n.Reconstructed[msg.SecretID] = record

	n.HopLog = append(n.HopLog, HopEvent{
		Hop:        msg.HopCount,
		FromNode:   msg.Sender,
		SecretID:   msg.SecretID,
		FragmentID: msg.FragmentID,
		Timestamp:  time.Now().UnixMilli(),
	})

	if isTarget {
		n.mu.Unlock()
		return
	}

	fwdMsg := Message{
		SecretID:   msg.SecretID,
		Target:     msg.Target,
		Sender:     n.ID,
		HopCount:   msg.HopCount + 1,
		Timestamp:  time.Now().UnixMilli(),
		FragmentID: msg.FragmentID,
		Data:       msg.Data,
	}

	n.mu.Unlock()

	go n.delayedAAGForward(fwdMsg)
}
func (n *Node) delayedAAGForward(fwdMsg Message) {
	time.Sleep(200 * time.Millisecond)

	n.mu.Lock()
	mr := make(map[string]bool)
	for _, event := range n.HopLog {
		if event.SecretID == fwdMsg.SecretID && event.FromNode != "init" {
			mr[event.FromNode] = true
			for _, nb := range n.twoHopNeighborsOf(event.FromNode) {
				mr[nb] = true
			}
		}
	}
	n.mu.Unlock()

	var children []string
	for _, nb := range n.Neighbors {
		if !mr[nb] {
			children = append(children, nb)
		}
	}

	// ====================================================================
	// CONFIDENTIALITY UPGRADE: Disjoint Path Filter
	// ====================================================================
	if len(children) > 1 {
		sort.Strings(children) // Ensure deterministic order across all nodes
		var disjointChildren []string
		for i, child := range children {
			// VIP PASS 1: NEVER filter out the final destination!
			if child == fwdMsg.Target {
				disjointChildren = append(disjointChildren, child)
				continue
			}
			
			// Enforce lanes for all other intermediate nodes
			if i%2 == fwdMsg.FragmentID%2 {
				disjointChildren = append(disjointChildren, child)
			}
		}
		// Only apply the filter if it doesn't completely dead-end the fragment
		if len(disjointChildren) > 0 {
			children = disjointChildren
		}
	}

	if len(children) == 0 {
		return
	}

	maxProb := 0.0
	for _, child := range children {
		// VIP PASS 2: If the destination is our direct neighbor,
		// guarantee delivery! Do not rely on probability.
		if child == fwdMsg.Target {
			maxProb = 1.0
			break
		}

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

	if mathrand.Float64() < maxProb {
		n.gossipOutTo(fwdMsg, children)
	}
}
func (n *Node) aagProbability(parentCount int) float64 {
	if parentCount <= 1 {
		return 1.0
	}
	delta := float64(n.Diameter)
	if delta < 1 {
		delta = 1
	}
	tauRel := math.Pow(TauARP, 1.0/delta)
	p := 1.0 - math.Pow(1.0-tauRel, 1.0/float64(parentCount))
	if p > 1.0 {
		return 1.0
	}
	return p
}

func (n *Node) baseProbability() float64 {
	return n.aagProbability(len(n.Neighbors))
}

func (n *Node) gossipOutTo(msg Message, peers []string) {
	for _, nb := range peers {
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

func (n *Node) twoHopNeighborsOf(nodeID string) []string {
	if nodeID == n.ID {
		return n.Neighbors
	}
	return n.TwoHopTable[nodeID]
}

func (n *Node) GetStatus() Status {
	n.mu.RLock()
	defer n.mu.RUnlock()

	tombList := make([]string, 0, len(n.Tombstones))
	for k := range n.Tombstones {
		tombList = append(tombList, k)
	}

	shareCount := 0
	for _, rec := range n.Reconstructed {
		shareCount = len(rec.Fragments)
		break 
	}

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
		TotalShares:    2, 
		HopLog:         hopLog,
	}
}

func toSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}