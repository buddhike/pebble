package kvs

import (
	"math/rand"
	"testing"
	"time"

	"github.com/buddhike/pebble/kvs/pb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNode(t *testing.T) {
	z, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := z.Sugar()

	n1 := newTestNode("n1", logger)
	n2 := newTestNode("n2", logger)
	n3 := newTestNode("n3", logger)
	n4 := newTestNode("n4", logger)
	n5 := newTestNode("n5", logger)
	n1.SetPeers([]Peer{n2, n3, n4, n5})
	n2.SetPeers([]Peer{n1, n3, n4, n5})
	n3.SetPeers([]Peer{n1, n2, n4, n5})
	n4.SetPeers([]Peer{n1, n2, n3, n5})
	n5.SetPeers([]Peer{n1, n2, n3, n4})

	go n1.Start()
	go n2.Start()
	go n3.Start()
	go n4.Start()
	go n5.Start()
	time.Sleep(time.Second * 15)
	nodes := map[string]*Node{
		"n1": n1,
		"n2": n2,
		"n3": n3,
		"n4": n4,
		"n5": n5,
	}

	resc := make(chan Res)
	nid := "n1"
	accepted := false
	for !accepted {
		t.Logf("attempting proposal: %s", nid)
		n := nodes[nid]
		n.Input() <- Req{
			Msg: &pb.ProposeRequest{
				Operation: "SET",
				Key:       []byte("k1"),
				Value:     []byte("v1"),
			},
			Response: resc,
		}
		r := <-resc
		pr := r.Msg.(*pb.PropseResponse)
		accepted = pr.Accepted
		if !accepted {
			nid = pr.CurrentLeader
		} else {
			close(n.stop)
		}
	}

	time.Sleep(time.Second * 10)
}

func newTestNode(id string, logger *zap.SugaredLogger) *Node {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	electionTimeout := rnd.Intn(100) + 100
	logger.Infof("node %s %d", id, electionTimeout)
	log := &inMemoryLog{
		entries: make([]*pb.Entry, 0),
	}
	state := &inMemoryMap{
		i: make(map[string]string),
	}
	stop := make(chan struct{})
	return NewNode(id, time.Millisecond*50, time.Millisecond*time.Duration(electionTimeout), log, state, stop, logger)
}
