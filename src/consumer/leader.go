package consumer

import (
	"context"
	"log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Leader struct {
	cfg        *ConsumerConfig
	etcdClient *clientv3.Client
	done       chan struct{}
	stop       chan struct{}
	mgr        *ManagerService
}

func NewLeader(cfg *ConsumerConfig, mgr *ManagerService, etcdClient *clientv3.Client, stop chan struct{}) *Leader {
	return &Leader{
		cfg:        cfg,
		etcdClient: etcdClient,
		done:       make(chan struct{}),
		stop:       stop,
		mgr:        mgr,
	}
}

func (l *Leader) Start() {
	go func() {
		l.elect()
		close(l.done)
	}()
}

func (l *Leader) elect() {
	for {
		// Create a session for the election
		session, err := concurrency.NewSession(l.etcdClient, concurrency.WithTTL(l.cfg.LeadershipTtlSeconds))
		if err != nil {
			log.Fatalf("failed to create session: %v", err)
		}

		// Generate a unique node ID (using hostname and PID)
		nodeID := l.cfg.GetEtcdPeerName()

		// Create an election
		election := concurrency.NewElection(session, "/leader-election")

		log.Printf("Node %s attempting to become leader...", nodeID)

		// Campaign to become leader
		ctx := context.Background()
		if err := election.Campaign(ctx, nodeID); err != nil {
			log.Fatalf("failed to campaign for leadership: %v", err)
		}

		log.Printf("Node %s successfully became leader!", nodeID)

		// Perform leader work
		log.Printf("Node %s is performing leader work...", nodeID)
		l.mgr.SetInService(true)

		// Observe leadership changes (optional - for monitoring)
		observeCh := election.Observe(ctx)
		go func() {
			for {
				select {
				case resp := <-observeCh:
					if len(resp.Kvs) > 0 {
						currentLeader := string(resp.Kvs[0].Value)
						log.Printf("Current leader is: %s", currentLeader)
					} else {
						log.Printf("No current leader")
					}
				case <-session.Done():
					log.Printf("Session ended, node %s lost leadership", nodeID)
					return
				}
			}
		}()

		// Keep the leader alive until session ends
		select {
		case <-session.Done():
			l.mgr.SetInService(false)
		case <-l.stop:
			session.Orphan()
			err := session.Close()
			if err != nil {
				log.Print(err)
			}
			return
		}
		log.Printf("Node %s is no longer the leader", nodeID)
		session.Close()
	}
}
