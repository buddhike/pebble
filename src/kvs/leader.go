package kvs

import (
	"iter"
	"maps"
	"time"

	"github.com/buddhike/pebble/kvs/pb"
)

// leader is a data structure used to keep track of various activites
// going on while a node is holding a leadership role.
type leader struct {
	node                    *Node
	nextIdx                 map[string]int64
	matchIdx                map[string]int64
	sendHeartbeat           map[string]bool
	lastActivity            map[string]time.Time
	readyList               map[string]bool
	peerByID                map[string]Peer
	numOutstandingResponses int
	pendingProposals        map[int64]*Req
}

// cancelPendingProposals sends a message to all outstanding proposal requests
// indicating that the request has been cancelled due to leadership change.
func (s *leader) cancelPendingProposals() {
	for k := range maps.Keys(s.pendingProposals) {
		req := s.pendingProposals[k]
		req.Response <- Res{
			PeerID: s.node.id,
			Msg: &pb.PropseResponse{
				Accepted:      false,
				CurrentLeader: "",
			},
			Req: req,
		}
	}
}

// readyPeers returns a sequence of peers that are ready to accept a request.
// At any given point leader can only have one outstanding request per peer.
func (s *leader) readyPeers() iter.Seq[Peer] {
	return func(yield func(Peer) bool) {
		for k, v := range s.readyList {
			if v {
				if !yield(s.peerByID[k]) {
					return
				}
			}
		}
	}
}

// shouldSendHeartbeat returns true if p is due for a heartbeat.
func (s *leader) shouldSendHeartbeat(p Peer) bool {
	return s.sendHeartbeat[p.ID()]
}

// hasEntriesToSend returns true if there are entries to be sent to p.
func (s *leader) hasEntriesToSend(p Peer) bool {
	return s.node.log.Len() >= s.nextIdx[p.ID()]
}

// pendingEntriesFor returns the entries that are pending to be sent to p.
func (s *leader) pendingEntriesFor(p Peer) []*pb.Entry {
	if s.node.log.Len() < s.nextIdx[p.ID()] {
		return make([]*pb.Entry, 0)
	}
	idx := s.nextIdx[p.ID()]
	entries := make([]*pb.Entry, (s.node.log.Len()-idx)+1)
	for i := range len(entries) {
		entries[i] = s.node.log.Get(idx + int64(i))
	}
	return entries
}

// send send a request to a peer and performs the bookkeeping
// required to track that peer as busy until a response is received.
func (s *leader) send(p Peer, req Req) {
	s.numOutstandingResponses++
	s.readyList[p.ID()] = false
	s.lastActivity[p.ID()] = time.Now()
	s.sendHeartbeat[p.ID()] = false
	go func() { p.Input() <- req }()
}

// receive performs the book keeping required to release a peer
// from busy state so that it's available to receive the next message from
// state machine.
func (s *leader) receive(res Res) {
	s.numOutstandingResponses--
	s.readyList[res.PeerID] = true
}

// scheduleHeartbeats goes through the list of peers and schedules a new
// heartbeat to be sent if required.
func (s *leader) scheduleHeartbeats() {
	for k := range maps.Keys(s.sendHeartbeat) {
		if s.sendHeartbeat[k] {
			continue
		}
		lastActivity := s.lastActivity[k]
		if time.Since(lastActivity) >= s.node.heartbeatTimeout {
			s.sendHeartbeat[k] = true
		}
	}
}

// ackPeerStateFor updates peer's match index and next index based on its
// response to an append entries request.
func (s *leader) ackPeerStateFor(res *Res) {
	resMsg := res.Msg.(*pb.AppendEntriesResponse)
	reqMsg := res.Req.Msg.(*pb.AppendEntriesRequest)
	if len(reqMsg.Entries) > 0 {
		if resMsg.Success {
			s.matchIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index
			s.nextIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index + 1
		} else {
			if s.nextIdx[res.PeerID] > 1 {
				s.nextIdx[res.PeerID] = s.nextIdx[res.PeerID] - 1
			}
		}
	}
}

// getLargestIndexApplicable finds out the largest index from current term that
// is replicated on majority.
// Returns 0 when there's nothing to apply.
func (s *leader) getLargestIndexApplicable() int64 {
	// Find p such that p > commitIndex, a majority of
	// matchIndex[i] (mp) >= p
	p := int64(0)
	mp := 0
	for k := range maps.Keys(s.matchIdx) {
		if s.matchIdx[k] > 0 && (p == 0 || s.matchIdx[k] <= p) {
			p = s.matchIdx[k]
			mp++
		}
	}
	// Update commitIndex if entry at p's term is current term
	quorumSize := len(s.node.peers) + 1
	majority := (quorumSize / 2) + 1
	if s.node.commitIndex < p && mp >= majority {
		e := s.node.log.Get(p)
		if e.Term == s.node.term {
			return p
		}
	}
	return 0
}

// queueProposal indexes a given proposal request by entry index.
func (s *leader) queueProposal(entry *pb.Entry, req *Req) {
	s.pendingProposals[entry.Index] = req
}

// serviceProposals goes through proposal queue and services any
// proposal whose entry index is equal or higher than current commit index.
func (s *leader) serviceProposals() {
	// Reply to proposals
	for k := range maps.Keys(s.pendingProposals) {
		if k <= s.node.commitIndex {
			r := s.pendingProposals[k]
			s.pendingProposals[k].Response <- Res{
				PeerID: s.node.id,
				Req:    r,
				Msg: &pb.PropseResponse{
					Accepted: true,
				},
			}
		}
	}
}

func newLeader(n *Node) *leader {
	s := &leader{
		node:                    n,
		nextIdx:                 make(map[string]int64),
		matchIdx:                make(map[string]int64),
		sendHeartbeat:           make(map[string]bool),
		lastActivity:            make(map[string]time.Time),
		readyList:               make(map[string]bool),
		peerByID:                make(map[string]Peer),
		numOutstandingResponses: 0,
		pendingProposals:        make(map[int64]*Req),
	}

	for _, p := range n.peers {
		s.nextIdx[p.ID()] = n.log.Len() + 1
		s.sendHeartbeat[p.ID()] = true
		s.matchIdx[p.ID()] = 0
		s.peerByID[p.ID()] = p
		s.readyList[p.ID()] = true
	}

	return s
}
