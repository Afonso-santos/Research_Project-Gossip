package api

import (
	"encoding/json"
	"log"
	"net/http"

	"gossip-sim/internal/gossip"
)

// Server wraps the gossip node with HTTP handlers.
type Server struct {
	node *gossip.Node
	mux  *http.ServeMux
}

// New creates an API server for the given node.
func New(node *gossip.Node) *Server {
	s := &Server{node: node, mux: http.NewServeMux()}
	s.mux.HandleFunc("/inject", s.handleInject)
	s.mux.HandleFunc("/gossip", s.handleGossip)
	s.mux.HandleFunc("/status", s.handleStatus)
	s.mux.HandleFunc("/members", s.handleMembers)
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// handleInject starts the gossip from this node.
// POST /inject  body: {"secret_id":"...","cid":"..."}
// or GET /inject?secret_id=...&cid=...
func (s *Server) handleInject(w http.ResponseWriter, r *http.Request) {
	secretID := gossip.HardcodedSecretID
	cid := gossip.HardcodedCID

	if r.Method == http.MethodPost {
		var req struct {
			SecretID string `json:"secret_id"`
			CID      string `json:"cid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			if req.SecretID != "" {
				secretID = req.SecretID
			}
			if req.CID != "" {
				cid = req.CID
			}
		}
	} else {
		if v := r.URL.Query().Get("secret_id"); v != "" {
			secretID = v
		}
		if v := r.URL.Query().Get("cid"); v != "" {
			cid = v
		}
	}

	log.Printf("[%s] /inject secret=%s cid=%s", s.node.ID, secretID, cid)
	s.node.Inject(secretID, cid)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"secret_id": secretID,
		"status":    "gossiped",
		"shares":    s.node.TotalShares,
		"threshold": 3,
		"topology":  s.node.TopologyStr,
	})
}

// handleGossip receives a gossip message from a peer.
// POST /gossip
func (s *Server) handleGossip(w http.ResponseWriter, r *http.Request) {
	var msg gossip.Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.node.Receive(msg)
	w.WriteHeader(http.StatusAccepted)
}

// handleStatus returns current node state.
// GET /status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.node.GetStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleMembers returns cluster membership info.
// GET /members
func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	members := make([]string, 0, len(s.node.PeerPorts)+1)
	for id := range s.node.PeerPorts {
		members = append(members, id)
	}
	members = append(members, s.node.ID)
	// sort
	for i := 0; i < len(members); i++ {
		for j := i + 1; j < len(members); j++ {
			if members[i] > members[j] {
				members[i], members[j] = members[j], members[i]
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node":     s.node.ID,
		"topology": s.node.TopologyStr,
		"count":    len(members),
		"members":  members,
	})
}
