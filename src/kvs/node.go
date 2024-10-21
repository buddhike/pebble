package kvs

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/buddhike/pebble/kvs/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type nodeState int

type Req struct {
	Msg      proto.Message
	Response chan Res
}

type Res struct {
	PeerID string
	Msg    proto.Message
	Req    *Req
}

type Log interface {
	Append(*pb.Entry)
	Get(int64) *pb.Entry
	Truncate(int64)
	Len() int64
	Last() *pb.Entry
}

type State interface {
	Apply(*pb.Entry)
}

type Peer interface {
	ID() string
	Input() chan<- Req
}

const (
	stateFollower nodeState = iota
	stateCandidate
	stateLeader
	stateExit
)

type Node struct {
	id               string
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	logger           *zap.SugaredLogger
	currentLeader    string
	commitIndex      int64
	lastApplied      int64
	// Requests to this node
	request  chan Req
	peers    []Peer
	term     int64
	log      Log
	state    State
	votedFor string
	// Closed by user to notify that node must stop current activity and return
	stop chan struct{}
	// Closed by node to indicate the successful stop
	done chan struct{}
}

func (n *Node) Start() {
	s := stateFollower
	for s != stateExit {
		switch s {
		case stateFollower:
			s = n.becomeFollower()
		case stateCandidate:
			s = n.becomeCandidate()
		case stateLeader:
			s = n.becomeLeader()
		}
	}
	close(n.done)
}

func (n *Node) becomeFollower() nodeState {
	n.logger.Infof("pebl became follower id:%s term:%d", n.id, n.term)
	timer := time.NewTimer(n.electionTimeout)
	for {
		select {
		case req := <-n.request:
			switch msg := req.Msg.(type) {
			case *pb.AppendEntriesRequest:
				if msg.Term < n.term {
					req.Response <- Res{
						PeerID: n.id,
						Msg: &pb.AppendEntriesResponse{
							Term:    n.term,
							Success: false,
						},
						Req: &req,
					}
				} else {
					timer.Reset(n.electionTimeout)
					n.appendEntries(&req)
				}
			case *pb.VoteRequest:
				if msg.Term < n.term {
					req.Response <- Res{
						PeerID: n.id,
						Msg: &pb.VoteResponse{
							Term:    n.term,
							Granted: false,
						},
						Req: &req,
					}
				} else {
					timer.Reset(n.electionTimeout)
					n.vote(req)
				}
			case *pb.ProposeRequest:
				// Followers only respond to peers. Proposals
				// from clients are rejected. Rejection response
				// indicate the leader id so that client and re-attempt
				// that request with the leader.
				req.Response <- Res{
					PeerID: n.id,
					Msg: &pb.PropseResponse{
						Accepted:      false,
						CurrentLeader: n.currentLeader,
					},
					Req: &req,
				}
			}
		case <-timer.C:
			return stateCandidate
		case <-n.stop:
			return stateExit
		}
	}
}

func (n *Node) becomeCandidate() nodeState {
	result := stateCandidate
	for result == stateCandidate {
		result = n.runElection()
	}
	return result
}

func (n *Node) runElection() nodeState {
	n.updateNodeState(n.term+1, "")
	n.logger.Infof("pebl became candidate:%s term:%d", n.id, n.term)
	peerResponses := make(chan Res)
	numOutstandingResponses := 0
	// Use a separate go routine to drain in order to prevent this method
	// from blocking until it receives responses to every outstanding request
	// initiated during this election.
	defer func() {
		go n.drain(peerResponses, numOutstandingResponses)
	}()
	vr := pb.VoteRequest{
		CandidateID: n.id,
		Term:        n.term,
	}
	l := n.log.Last()
	if l != nil {
		vr.LastLogTerm = l.Term
		vr.LastLogIndex = l.Index
	}
	peers := slices.Clone(n.peers)
	votes := 1
	req := Req{
		Msg:      &vr,
		Response: peerResponses,
	}
	nextPeerInput := peers[0].Input()
	quorumSize := len(n.peers) + 1
	timer := time.NewTimer(n.electionTimeout)
	for {
		select {
		case nextPeerInput <- req:
			timer.Reset(n.electionTimeout)
			numOutstandingResponses++
			peers = peers[1:]
			if len(peers) > 0 {
				nextPeerInput = peers[0].Input()
			} else {
				nextPeerInput = nil
			}
		case v := <-peerResponses:
			numOutstandingResponses--
			timer.Reset(n.electionTimeout)
			switch msg := v.Msg.(type) {
			case *pb.VoteResponse:
				if msg.Term == n.term && msg.Granted {
					votes++
				}
				if votes >= (quorumSize/2)+1 {
					return stateLeader
				}
			}
		case v := <-n.request:
			switch msg := v.Msg.(type) {
			case *pb.AppendEntriesRequest:
				if msg.Term < n.term {
					v.Response <- Res{
						PeerID: n.id,
						Msg: &pb.AppendEntriesResponse{
							Term:    n.term,
							Success: false,
						},
					}
				} else {
					n.appendEntries(&v)
					return stateFollower
				}
			case *pb.VoteRequest:
				if n.term > msg.Term {
					v.Response <- Res{
						PeerID: n.id,
						Msg: &pb.VoteResponse{
							Term:    n.term,
							Granted: false,
						},
					}
				} else if n.term == msg.Term {
					timer.Reset(n.electionTimeout)
					n.vote(req)
				} else {
					n.vote(req)
					return stateFollower
				}
			case *pb.ProposeRequest:
				// Candidates only respond to peers. Proposals
				// from clients are rejected.
				v.Response <- Res{
					PeerID: n.id,
					Msg: &pb.PropseResponse{
						Accepted:      false,
						CurrentLeader: "",
					},
				}
			}
		case <-timer.C:
			return stateCandidate
		case <-n.stop:
			return stateExit
		default:
			if len(peers) > 1 {
				head := peers[0]
				peers = peers[1:]
				peers = append(peers, head)
				nextPeerInput = peers[0].Input()
			}
		}
	}
}

func (n *Node) becomeLeader() nodeState {
	n.logger.Infof("pebl became leader id:%s term:%d", n.id, n.term)
	nextIdx := make(map[string]int64)
	sendHeartbeat := make(map[string]bool)
	matchIdx := make(map[string]int64)
	idleTimer := time.NewTimer(n.heartbeatTimeout)
	idleTimer.Stop()
	peerResponses := make(chan Res)
	numOutstandingResponses := 0
	pendingProposals := make(map[int64]Req)

	defer func() {
		n.drain(peerResponses, numOutstandingResponses)
		for k := range maps.Keys(pendingProposals) {
			req := pendingProposals[k]
			req.Response <- Res{
				PeerID: n.id,
				Msg: &pb.PropseResponse{
					Accepted:      false,
					CurrentLeader: "",
				},
				Req: &req,
			}
		}
	}()

	for _, p := range n.peers {
		nextIdx[p.ID()] = n.log.Len() + 1
		sendHeartbeat[p.ID()] = true
		matchIdx[p.ID()] = 0
	}

	nextPeerIdx := 0
	isIdle := false
	for {
		nextPeer := n.peers[nextPeerIdx]
		nextPeerInput := nextPeer.Input()
		var appendEntriesReq Req
		if sendHeartbeat[nextPeer.ID()] {
			sendHeartbeat[nextPeer.ID()] = false
			m := &pb.AppendEntriesRequest{
				Term:         n.term,
				LeaderID:     n.id,
				LeaderCommit: n.commitIndex,
			}
			appendEntriesReq = Req{
				Msg:      m,
				Response: peerResponses,
			}
			// n.logger.Debugf("pebl heartbeat leader: %s term: %d peer: %s", n.id, n.term, nextPeer.ID())
		} else if n.log.Len() >= nextIdx[nextPeer.ID()] {
			idx := nextIdx[nextPeer.ID()]
			nextIdx[nextPeer.ID()] = int64(idx + 1)
			entries := make([]*pb.Entry, (n.log.Len()-idx)+1)
			for i := range len(entries) {
				entries[i] = n.log.Get((idx - 1) + int64(i))
			}

			m := &pb.AppendEntriesRequest{
				Term:         n.term,
				LeaderID:     n.id,
				LeaderCommit: n.commitIndex,
				Entries:      entries,
			}
			appendEntriesReq = Req{
				Msg:      m,
				Response: peerResponses,
			}
		} else {
			nextPeerInput = nil
		}

		if nextPeerInput == nil {
			// There's nothing to be sent to a peer.
			// Start idle timer if it's not already running.
			if !isIdle {
				isIdle = true
				idleTimer.Reset(n.heartbeatTimeout)
			}
		} else {
			isIdle = false
			idleTimer.Stop()
		}

		select {
		case nextPeerInput <- appendEntriesReq:
			numOutstandingResponses++
			nextPeerIdx++
			if nextPeerIdx >= len(n.peers) {
				nextPeerIdx = 0
			}
		case <-idleTimer.C:
			for k := range maps.Keys(sendHeartbeat) {
				sendHeartbeat[k] = true
			}
		case res := <-peerResponses:
			numOutstandingResponses--
			msg := res.Msg.(*pb.AppendEntriesResponse)
			if msg.Term > n.term {
				n.term = msg.Term
				return stateFollower
			}
			reqMsg := res.Req.Msg.(*pb.AppendEntriesRequest)
			if len(reqMsg.Entries) > 0 {
				if msg.Success {
					matchIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index
					nextIdx[res.PeerID] = reqMsg.Entries[len(reqMsg.Entries)-1].Index + 1
				} else {
					nextIdx[res.PeerID] = nextIdx[res.PeerID] - 1
				}
			}
			smallestMatchIndex := int64(0)
			matchedPeerCount := 0
			for k := range maps.Keys(matchIdx) {
				if matchIdx[k] > 0 && (smallestMatchIndex == 0 || matchIdx[k] <= smallestMatchIndex) {
					smallestMatchIndex = matchIdx[k]
					matchedPeerCount++
				}
			}
			quorumSize := len(n.peers) + 1
			majority := (quorumSize / 2) + 1
			if n.commitIndex < smallestMatchIndex && matchedPeerCount >= majority {
				e := n.log.Get(smallestMatchIndex - 1)
				if e.Term == n.term {
					n.commitIndex = smallestMatchIndex
					for i := n.commitIndex; n.lastApplied < n.commitIndex; n.lastApplied++ {
						entry := n.log.Get(i - 1)
						n.state.Apply(entry)
					}

					for k := range maps.Keys(pendingProposals) {
						if k <= n.commitIndex {
							r := pendingProposals[k]
							pendingProposals[k].Response <- Res{
								PeerID: n.id,
								Req:    &r,
								Msg: &pb.PropseResponse{
									Accepted: true,
								},
							}
						}
					}
				}
			}
		case req := <-n.request:
			switch r := req.Msg.(type) {
			case *pb.ProposeRequest:
				e := &pb.Entry{
					Index:     n.log.Len() + 1,
					Term:      n.term,
					Operation: r.Operation,
					Key:       r.Key,
					Value:     r.Value,
				}
				n.log.Append(e)
				pendingProposals[e.Index] = req
			case *pb.AppendEntriesRequest:
				if r.Term > n.term {
					n.appendEntries(&req)
					return stateFollower
				}
				res := Res{
					PeerID: n.id,
					Msg: &pb.AppendEntriesResponse{
						Term:    n.term,
						Success: false,
					},
					Req: &req,
				}
				req.Response <- res
			case *pb.VoteRequest:
				if r.Term > n.term {
					n.vote(req)
					return stateFollower
				}
				res := Res{
					PeerID: n.id,
					Msg: &pb.VoteResponse{
						Term:    n.term,
						Granted: false,
					},
					Req: &req,
				}
				req.Response <- res
			}
		case <-n.stop:
			return stateExit
		default:
			nextPeerIdx++
			if nextPeerIdx >= len(n.peers) {
				nextPeerIdx = 0
			}
		}
	}
}

func (n *Node) vote(req Req) {
	msg := req.Msg.(*pb.VoteRequest)
	if msg.Term < n.term {
		panic(fmt.Errorf("vote cannot vote for older term id:%s candidate:%s current-term:%d election-term:%d", n.id, msg.CandidateID, n.term, msg.Term))
	}
	if msg.Term > n.term {
		n.updateNodeState(msg.Term, "")
	}
	// Get the last entry from this node's log
	le := n.log.Last()
	isPeerLogAsUpToDate := (le == nil) || (msg.LastLogTerm > le.Term) || (le.Term == msg.LastLogTerm && msg.LastLogIndex >= le.Index)
	alreadyVotedThisCandidateInSameTerm := msg.CandidateID == n.votedFor && n.term == msg.Term
	termIsCurrentOrNew := msg.Term >= n.term
	haventVotedYet := n.votedFor == ""
	granted := (haventVotedYet || alreadyVotedThisCandidateInSameTerm) && termIsCurrentOrNew && isPeerLogAsUpToDate
	if granted {
		n.updateNodeState(n.term, msg.CandidateID)
	}
	n.logger.Infof("pebl voted id:%s candidate:%s (%v,%v,%v,%v,%v)", n.id, msg.CandidateID, isPeerLogAsUpToDate, alreadyVotedThisCandidateInSameTerm, termIsCurrentOrNew, haventVotedYet, granted)
	rmsg := pb.VoteResponse{
		Term:    n.term,
		Granted: granted,
	}
	res := Res{
		PeerID: n.id,
		Msg:    &rmsg,
		Req:    &req,
	}
	req.Response <- res
}

func (n *Node) appendEntries(req *Req) {
	msg := req.Msg.(*pb.AppendEntriesRequest)
	var rmsg *pb.AppendEntriesResponse
	n.currentLeader = msg.LeaderID
	if msg.Term < n.term {
		panic(fmt.Errorf("appendEntries cannot append entries from an older term id:%s leader:%s current:%d req:%d", n.id, msg.LeaderID, n.term, msg.Term))
	}
	if msg.Term != n.term {
		n.logger.Infof("pebl detected a new term via append entries id:%s current-term:%d new-term:%d", n.id, n.term, msg.Term)
		n.updateNodeState(msg.Term, "")
	}
	n.currentLeader = msg.LeaderID
	success := false
	if len(msg.Entries) > 0 {
		// We have entries to append
		// We must:
		// - ensure log matching property
		// - truncate the log if required
		// If we cannot satisfy these properties, respond as unsuccessful
		if msg.PrevLogIndex == 0 {
			// Leader is saying that entries should be appended to the begining
			// of the log. Truncate the log to the begining and append entries.
			n.log.Truncate(0)
			for _, e := range msg.Entries {
				n.log.Append(e)
			}
			success = true
		} else if msg.PrevLogIndex <= n.log.Len() {
			// Leader is saying that entries should be appended after PrevLogIndex.
			// We also know that PrevLogIndex is valid in follower's log.
			// To ensure log matching property, we read the entry in that location
			// in follower and ensure the terms match.
			p := n.log.Get(msg.PrevLogIndex)
			if p.Term == msg.PrevLogTerm {
				// Now that the terms are matching, truncate any entries past
				// PrevLogIndex and append entries.
				if msg.PrevLogIndex != n.log.Len() {
					n.log.Truncate(msg.PrevLogIndex)
				}
				for _, e := range msg.Entries {
					n.log.Append(e)
				}
				success = true
			}
		}
	} else {
		// This is a heartbeat request without any entries
		success = true
	}
	// After any available entries are appended, ensure that
	// commit index is updated to match leader's commit.
	// If commit index is past the entry last applied, apply
	// those entries to state.
	if msg.LeaderCommit > 0 && n.log.Len() > 0 && n.log.Len() >= msg.LeaderCommit && n.log.Get(msg.LeaderCommit).Term == n.term {
		n.commitIndex = msg.LeaderCommit
		for i := n.commitIndex; n.lastApplied < n.commitIndex; n.lastApplied++ {
			entry := n.log.Get(i)
			n.state.Apply(entry)
		}
	}
	rmsg = &pb.AppendEntriesResponse{
		Term:    n.term,
		Success: success,
	}
	res := Res{
		PeerID: n.id,
		Msg:    rmsg,
		Req:    req,
	}
	req.Response <- res
}

func (n *Node) updateNodeState(term int64, votedFor string) {
	n.term = term
	n.votedFor = votedFor
}

// drain is used to read all outstanding responses from a peer response channel.
// This is an essential because each state handler (i.e. becomeXxx) initialises
// a new channel to demux responses from peers for the requests issued while
// in that state. This approach gives the ability to reason about each state
// handler without considering any responses to requests issued prior to a
// state transition.
// drain is used to read any pending requests out of those response channels
// and ensure that peers can progress.
func (n *Node) drain(c chan Res, numberOfOutstandingResponses int) {
	for range c {
		numberOfOutstandingResponses--
		if numberOfOutstandingResponses == 0 {
			break
		}
	}
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Input() chan<- Req {
	return n.request
}

func (n *Node) Done() <-chan struct{} {
	return n.done
}

func (n *Node) SetPeers(peers []Peer) {
	n.peers = peers
}

func NewNode(id string, heartbeatTimeout, electionTimeout time.Duration, log Log, state State, stop chan struct{}, logger *zap.SugaredLogger) *Node {
	return &Node{
		id:               id,
		heartbeatTimeout: heartbeatTimeout,
		electionTimeout:  electionTimeout,
		log:              log,
		state:            state,
		stop:             stop,
		done:             make(chan struct{}),
		request:          make(chan Req),
		logger:           logger,
	}
}
