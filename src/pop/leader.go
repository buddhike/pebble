package main

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type Leader struct {
	cfg        *PopConfig
	etcdClient *clientv3.Client
	done       chan struct{}
	stop       chan struct{}
	mgr        *ManagerService
	logger     *zap.Logger
}

func NewLeader(cfg *PopConfig, mgr *ManagerService, etcdClient *clientv3.Client, stop chan struct{}, logger *zap.Logger) *Leader {
	return &Leader{
		cfg:        cfg,
		etcdClient: etcdClient,
		done:       make(chan struct{}),
		stop:       stop,
		mgr:        mgr,
		logger:     logger.Named("leader"),
	}
}

func (l *Leader) Start() {
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		go l.elect(ctx)
		<-l.stop
		cancel()
	}()
}

func (l *Leader) elect(ctx context.Context) {
	defer close(l.done)
	for {
		// Create a session for the election
		session, err := concurrency.NewSession(l.etcdClient, concurrency.WithTTL(l.cfg.LeadershipTtlSeconds))
		if err != nil {
			l.logger.Fatal("failed to create session", zap.Error(err))
		}

		// Generate a unique node ID (using hostname and PID)
		nodeID := l.cfg.GetEtcdPeerName()

		// Create an election
		election := concurrency.NewElection(session, "/leader-election")

		l.logger.Info("node attempting to become leader", zap.String("node_id", nodeID))

		if err := election.Campaign(ctx, nodeID); err != nil {
			l.logger.Error("failed to campaign for leadership", zap.Error(err))
			return
		}

		l.logger.Info("node successfully became leader", zap.String("node_id", nodeID), zap.Int64("rev", election.Rev()))

		// Perform leader work
		l.logger.Info("node is performing leader work", zap.String("node_id", nodeID))
		l.mgr.SetToInService(election.Rev())

		// Observe leadership changes (optional - for monitoring)
		observeCh := election.Observe(ctx)
		go func() {
			for {
				select {
				case resp := <-observeCh:
					if len(resp.Kvs) > 0 {
						currentLeader := string(resp.Kvs[0].Value)
						l.logger.Info("current leader", zap.String("leader", currentLeader))
					} else {
						l.logger.Info("no current leader")
					}
				case <-session.Done():
					l.logger.Info("session ended, node lost leadership", zap.String("node_id", nodeID))
					return
				}
			}
		}()

		// Keep the leader alive until session ends
		select {
		case <-session.Done():
			l.logger.Info("node is no longer the leader", zap.String("node_id", nodeID))
			l.mgr.SetToOutOfService()
			session.Close()
		case <-ctx.Done():
			session.Orphan()
			err := session.Close()
			if err != nil {
				l.logger.Error("error closing session", zap.Error(err))
			}
			return
		}
	}
}
