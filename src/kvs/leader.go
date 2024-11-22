package kvs

import (
	"iter"
	"maps"
	"time"

	"github.com/buddhike/pebble/kvs/pb"
)

// Tracks the lifecycle of a read proposal
// Read proposal can be satisfied either when successfulReadbeats >=
// half the number of peers or when completedReadbeats == number of peers.
type readProposal struct {
	// req is the read proposal request
	req *Req
	// successfulReadbeats keeps track of how many readbeats are successfully acked by peers
	// for a given read proposal
	successfulReadbeats int
	// completedReadbeats keeps track of how many readbeats are acked by the peers
	// for a given read proposal
	completedReadbeats int
}

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
	// Maps waiting write proposals to their entry index
	pendingWriteProposals map[int64]*Req
	// Set of pending read proposals for O(1) lookup
	pendingReadProposalsSet map[uint64]*readProposal
	// Maps a readbeat request to the corresponding read proposal
	readbeatToReadProposal map[uint64]*readProposal
	// Maps list of read proposals to be sent as readbeats to peer
	peerReadProposals map[Peer][]*readProposal
}

// cancelPendingProposals sends a message to all outstanding proposal requests
// indicating that the request has been cancelled due to leadership change.
func (l *leader) cancelPendingProposals() {
	l.node.logger.Infof("Start cancelPendingProposals %v", l.pendingWriteProposals)
	for k := range maps.Keys(l.pendingWriteProposals) {
		req := l.pendingWriteProposals[k]
		delete(l.pendingWriteProposals, k)
		req.Response <- Res{
			PeerID: l.node.id,
			Msg: &pb.PropseResponse{
				Accepted:      false,
				CurrentLeader: "",
			},
			Req: req,
		}
	}
	l.node.logger.Infof("End cancelPendingProposals")
}

// readyPeers returns a sequence of peers that are ready to accept a request.
// At any given point leader can only have one outstanding request per peer.
func (l *leader) readyPeers() iter.Seq[Peer] {
	return func(yield func(Peer) bool) {
		for k, v := range l.readyList {
			if v {
				if !yield(l.peerByID[k]) {
					return
				}
			}
		}
	}
}

// shouldSendHeartbeat returns true if p is due for a heartbeat.
func (l *leader) shouldSendHeartbeat(p Peer) bool {
	return l.sendHeartbeat[p.ID()]
}

// hasEntriesToSend returns true if there are entries to be sent to p.
func (l *leader) hasEntriesToSend(p Peer) bool {
	return l.node.log.Len() >= l.nextIdx[p.ID()]
}

// pendingEntriesFor returns the entries that are pending to be sent to p.
func (l *leader) pendingEntriesFor(p Peer) []*pb.Entry {
	if l.node.log.Len() < l.nextIdx[p.ID()] {
		return make([]*pb.Entry, 0)
	}
	idx := l.nextIdx[p.ID()]
	entries := make([]*pb.Entry, (l.node.log.Len()-idx)+1)
	for i := range len(entries) {
		entries[i] = l.node.log.Get(idx + int64(i))
	}
	return entries
}

// send send a request to a peer and performs the bookkeeping
// required to track that peer as busy until a response is received.
func (l *leader) send(p Peer, req Req) {
	l.numOutstandingResponses++
	l.readyList[p.ID()] = false
	l.lastActivity[p.ID()] = time.Now()
	l.sendHeartbeat[p.ID()] = false
	go func() { p.Input() <- req }()
}

// receive performs the book keeping required to release a peer
// from busy state so that it's available to receive the next message from
// state machine.
func (l *leader) receive(res Res) {
	l.numOutstandingResponses--
	l.readyList[res.PeerID] = true
}

// scheduleHeartbeats goes through the list of peers and schedules a new
// heartbeat to be sent if required.
func (l *leader) scheduleHeartbeats() {
	for k := range maps.Keys(l.sendHeartbeat) {
		if l.sendHeartbeat[k] {
			continue
		}
		lastActivity := l.lastActivity[k]
		if time.Since(lastActivity) >= l.node.heartbeatTimeout {
			l.sendHeartbeat[k] = true
		}
	}
}

// updatePeerState updates peer's match index and next index based on its
// response to an append entries request.
func (l *leader) updatePeerState(res *Res) {
	resMsg := res.Msg.(*pb.AppendEntriesResponse)
	reqMsg := res.Req.Msg.(*pb.AppendEntriesRequest)
	if len(reqMsg.Entries) > 0 {
		if resMsg.Success {
			l.matchIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index
			l.nextIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index + 1
		} else {
			if l.nextIdx[res.PeerID] > 1 {
				l.nextIdx[res.PeerID] = l.nextIdx[res.PeerID] - 1
			}
		}
	}
}

// getLargestIndexApplicable finds out the largest index from current term that
// is replicated on majority.
// Returns 0 when there's nothing to apply.
func (l *leader) getLargestIndexApplicable() int64 {
	// Find p such that p > commitIndex, a majority of
	// matchIndex[i] (mp) >= p
	p := int64(0)
	mp := 0
	for k := range maps.Keys(l.matchIdx) {
		if l.matchIdx[k] > 0 && (p == 0 || l.matchIdx[k] <= p) {
			p = l.matchIdx[k]
			mp++
		}
	}
	// Ensure that if entry at p's term is current term
	majority := len(l.node.peers) / 2
	if l.node.commitIndex < p && mp >= majority {
		e := l.node.log.Get(p)
		if e.Term == l.node.term {
			return p
		}
	}
	return 0
}

// queueProposal indexes a given proposal request by entry index.
func (l *leader) queueProposal(entry *pb.Entry, req *Req) {
	l.pendingWriteProposals[entry.Index] = req
}

// queueReadProposal enqueues a new read request to read proposals queue
// Read proposals queue is used to implement linearisable reads.
func (l *leader) queueReadProposal(req *Req) {
	p := &readProposal{
		req: req,
	}
	l.pendingReadProposalsSet[req.id] = p
	for _, peer := range l.node.peers {
		prq := l.peerReadProposals[peer]
		l.peerReadProposals[peer] = append(prq, p)
	}
}

// canSendReadbeat returns true if there are pending readbeats to be sent
// for a given peer.
func (l *leader) canSendReadbeat(peer Peer) bool {
	return len(l.peerReadProposals[peer]) > 0
}

// sendReadbeat sends a readbeat request to a given peer.
func (l *leader) sendReadbeat(peer Peer, rb *Req) {
	peerReadProposalsQ := l.peerReadProposals[peer]
	// TODO: Consider aggregating multiple proposals to a heartbeat
	l.readbeatToReadProposal[rb.id] = peerReadProposalsQ[0]
	l.peerReadProposals[peer] = peerReadProposalsQ[1:]
	l.send(peer, *rb)
}

// serviceProposals goes through proposal queue and services any
// proposal whose entry index is equal or higher than current commit index.
func (l *leader) serviceProposals() {
	// Reply to proposals
	for k := range maps.Keys(l.pendingWriteProposals) {
		if k <= l.node.commitIndex {
			r := l.pendingWriteProposals[k]
			delete(l.pendingWriteProposals, k)
			r.Response <- Res{
				PeerID: l.node.id,
				Req:    r,
				Msg: &pb.PropseResponse{
					Accepted: true,
				},
			}
		}
	}
}

func (l *leader) serviceReadProposals(res *Res) {
	req := res.Req
	rp, ok := l.readbeatToReadProposal[req.id]
	if !ok {
		// This is not a response to a readbeat
		return
	}
	delete(l.readbeatToReadProposal, req.id)
	if _, ok := l.pendingReadProposalsSet[rp.req.id]; !ok {
		// We have received successful acks from majority and removed
		// it from readbeatToReadProposal map. Nothing to do here.
		return
	}

	rp.completedReadbeats++
	msg := res.Msg.(*pb.AppendEntriesResponse)
	if msg.Success {
		rp.successfulReadbeats++
	}
	majority := len(l.node.peers) / 2
	readbeatSuccessFromMajority := rp.successfulReadbeats >= majority
	readbeatAckedByAllPeers := rp.completedReadbeats == len(l.node.peers)
	if readbeatSuccessFromMajority {
		proposal := rp.req.Msg.(*pb.ProposeRequest)
		result := l.node.state.Read(proposal.Key)
		rp.req.Response <- Res{
			PeerID: l.node.id,
			Req:    req,
			Msg: &pb.PropseResponse{
				Accepted:      true,
				CurrentLeader: l.node.id,
				Value:         result,
			},
		}
	} else if readbeatAckedByAllPeers {
		l.node.logger.Infof("rejecting a read proposal")
		rp.req.Response <- Res{
			PeerID: l.node.id,
			Req:    req,
			Msg: &pb.PropseResponse{
				Accepted:      false,
				CurrentLeader: l.node.id,
			},
		}
	}
	// Clean up
	if readbeatAckedByAllPeers || readbeatSuccessFromMajority {
		delete(l.pendingReadProposalsSet, rp.req.id)
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
		pendingWriteProposals:   make(map[int64]*Req),
		pendingReadProposalsSet: make(map[uint64]*readProposal),
		readbeatToReadProposal:  make(map[uint64]*readProposal),
		peerReadProposals:       make(map[Peer][]*readProposal),
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
