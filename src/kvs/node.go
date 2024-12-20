package kvs

import (
	"fmt"
	"slices"
	"time"

	"github.com/buddhike/pebble/kvs/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type nodeState int

type Req struct {
	id       uint64
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
	Apply(*pb.Entry) *pb.ProposeResponse
	Read([]byte) []byte
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
	input     chan Req
	peers     []Peer
	term      int64
	log       Log
	state     State
	votedFor  string
	requestID uint64
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
		case req := <-n.input:
			req.id = n.nextRequestID()
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
					Msg: &pb.ProposeResponse{
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
		go n.drain("runElection", peerResponses, numOutstandingResponses)
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

	nextPeerInput := peers[0].Input()
	quorumSize := len(n.peers) + 1
	timer := time.NewTimer(n.electionTimeout)
	for {
		req := Req{
			id:       n.nextRequestID(),
			Msg:      &vr,
			Response: peerResponses,
		}
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
		case v := <-n.input:
			v.id = n.nextRequestID()
			switch msg := v.Msg.(type) {
			case *pb.AppendEntriesRequest:
				if msg.Term < n.term {
					v.Response <- Res{
						PeerID: n.id,
						Msg: &pb.AppendEntriesResponse{
							Term:    n.term,
							Success: false,
						},
						Req: &v,
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
						Req: &v,
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
					Msg: &pb.ProposeResponse{
						Accepted:      false,
						CurrentLeader: "",
					},
					Req: &v,
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
	peerResponses := make(chan Res)
	idleTicker := time.NewTicker(n.heartbeatTimeout)
	leader := newLeader(n)

	defer func() {
		n.drain("leader", peerResponses, leader.numOutstandingResponses)
		leader.cancelPendingProposals()
	}()

	// Append noop entry so that this leader can know when a majority
	// of peers are caught-up with this term.
	noop := &pb.Entry{
		// Timestamp is added for diagnostics
		Timestamp: time.Now().UnixMilli(),
		Index:     n.log.Len() + 1,
		Term:      n.term,
		Operation: pb.Op_Noop,
	}
	n.log.Append(noop)

	for {
		canServiceReads := n.commitIndex >= noop.Index
		for peer := range leader.readyPeers() {
			if leader.shouldSendHeartbeat(peer) || leader.hasEntriesToSend(peer) {
				msg := &pb.AppendEntriesRequest{
					Term:         n.term,
					LeaderID:     n.id,
					LeaderCommit: n.commitIndex,
					Entries:      leader.pendingEntriesFor(peer),
				}
				if len(msg.Entries) > 0 {
					if msg.Entries[0].Index > 1 {
						pentry := n.log.Get(msg.Entries[0].Index - 1)
						msg.PrevLogIndex = pentry.Index
						msg.PrevLogTerm = pentry.Term
					}
				}
				req := Req{
					id:       n.nextRequestID(),
					Msg:      msg,
					Response: peerResponses,
				}
				leader.send(peer, req)
			} else if canServiceReads && leader.canSendReadbeat(peer) {
				msg := &pb.AppendEntriesRequest{
					Term:         n.term,
					LeaderID:     n.id,
					LeaderCommit: n.commitIndex,
				}
				req := Req{
					id:       n.nextRequestID(),
					Msg:      msg,
					Response: peerResponses,
				}
				leader.sendReadbeat(peer, &req)
			}
		}

		select {
		case <-idleTicker.C:
			leader.scheduleHeartbeats()
		case res := <-peerResponses:
			leader.receive(res)
			msg := res.Msg.(*pb.AppendEntriesResponse)
			if msg.Term > n.term {
				n.updateNodeState(msg.Term, "")
				return stateFollower
			}
			leader.updatePeerState(&res)
			p := leader.getLargestIndexApplicable()
			if n.commitIndex < p {
				n.logger.Infof("pebl advanced id:%s idx:%d", n.id, p)
				n.commitIndex = p
				for n.lastApplied != n.commitIndex {
					n.lastApplied++
					entry := n.log.Get(n.lastApplied)
					res := n.state.Apply(entry)
					res.CurrentLeader = n.id
					req := leader.getPendingWriteRequest(entry.Index)
					if req != nil {
						req.Response <- Res{
							PeerID: n.id,
							Msg:    res,
						}
					}
				}
			}
			leader.serviceReadProposals(&res)
		case req := <-n.input:
			req.id = n.nextRequestID()
			switch r := req.Msg.(type) {
			case *pb.ProposeRequest:
				if r.Operation == pb.Op_Get {
					leader.queueReadProposal(&req)
				} else {
					e := &pb.Entry{
						Index:     n.log.Len() + 1,
						Term:      n.term,
						Operation: r.Operation,
						Timestamp: time.Now().UnixMilli(),
						SessionID: r.SessionID,
						Sequence:  r.Sequence,
						Key:       r.Key,
						Value:     r.Value,
					}
					n.log.Append(e)
					leader.queueProposal(e, &req)
				}
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
		}
	}
}

func (n *Node) vote(req Req) {
	msg := req.Msg.(*pb.VoteRequest)
	if msg.Term < n.term {
		panic(fmt.Errorf("vote cannot vote for older term id:%s candidate:%s current-term:%d election-term:%d", n.id, msg.CandidateID, n.term, msg.Term))
	}
	if msg.Term > n.term {
		n.logger.Infof("pebl vote new term id:%s current-term:%d new-term:%d", n.id, n.term, msg.Term)
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
		n.logger.Infof("pebl appendEntries new term id:%s current-term:%d new-term:%d", n.id, n.term, msg.Term)
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
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.log.Get(n.lastApplied)
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
func (n *Node) drain(name string, c chan Res, numberOfOutstandingResponses int) {
	n.logger.Debugf("drain start id:%s %s(%d)", n.id, name, numberOfOutstandingResponses)
	for numberOfOutstandingResponses > 0 {
		<-c
		numberOfOutstandingResponses--
	}
	n.logger.Debugf("drain end id:%s %s(%d)", n.id, name, numberOfOutstandingResponses)
}

func (n *Node) nextRequestID() uint64 {
	n.requestID++
	return n.requestID
}

func (n *Node) ID() string {
	return n.id
}

func (n *Node) Input() chan<- Req {
	return n.input
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
		input:            make(chan Req),
		logger:           logger,
	}
}
