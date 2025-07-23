package consumer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/buddhike/pebble/aws"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type PopService struct {
	cfg                  *PopConfig
	kvs                  KVS
	kds                  aws.Kinesis
	stop                 chan struct{}
	done                 chan struct{}
	componentsDoneNotify map[string]chan struct{}
	logger               *zap.Logger
}

func NewPopService(name, streamName string, opts ...func(*PopConfig)) *PopService {
	cfg := PopConfig{
		PopID:                               1,
		Name:                                name,
		StreamName:                          streamName,
		PopUrls:                             "http://localhost:13001",
		PopListenAddress:                    "localhost",
		EtcdPeerUrls:                        "http://localhost:12001",
		EtcdListenPeerAddress:               "localhost",
		EtcdClientUrls:                      "http://localhost:11001",
		EtcdListenClientAddress:             "localhost",
		LeadershipTtlSeconds:                60,
		HealthcheckTimeoutMilliseconds:      1000,
		WorkerInactivityTimeoutMilliseconds: 5000,
		ShardReleaseTimeoutMilliseconds:     3000,
		EtcdStartTimeoutSeconds:             30,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	logger := cfg.logger
	if logger == nil {
		l, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}
		logger = l
	}

	return &PopService{
		cfg:                  &cfg,
		componentsDoneNotify: make(map[string]chan struct{}),
		stop:                 make(chan struct{}),
		done:                 make(chan struct{}),
		logger:               logger.Named("pop").With(zap.String("name", cfg.GetEtcdPeerName())),
	}
}

func (p *PopService) Start() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   p.cfg.GetClientConnectionUrls(),
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return err
	}

	p.kvs = cli

	if p.cfg.KinesisClient == nil {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return err
		}
		p.kds = kinesis.NewFromConfig(cfg)
	} else {
		p.kds = p.cfg.KinesisClient
	}

	mgr := NewManagerService(p.cfg, p.kds, p.kvs, p.stop, p.logger)
	err = mgr.Start()
	if err != nil {
		return err
	}
	p.componentsDoneNotify["ManagerService"] = mgr.done

	etcdServer := NewEtcdServer(p.cfg, p.stop, p.logger)
	err = etcdServer.Start()
	if err != nil {
		return err
	}
	p.componentsDoneNotify["EtcdServer"] = etcdServer.done

	leader := NewLeader(p.cfg, mgr, cli, p.stop, p.logger)
	p.componentsDoneNotify["Leader"] = leader.done
	leader.Start()
	return nil
}

func (p *PopService) Stop() {
	close(p.stop)
	for k, v := range p.componentsDoneNotify {
		p.logger.Info("shutdown initiated", zap.String("component", k))
		<-v
		p.logger.Info("shutdown complete", zap.String("component", k))
	}
	close(p.done)
}

func (p *PopService) Done() <-chan struct{} {
	return p.done
}
