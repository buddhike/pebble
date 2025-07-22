package consumer

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Consumer struct {
	cfg                  *ConsumerConfig
	done                 chan struct{}
	stop                 chan struct{}
	processFn            func(types.Record)
	kvs                  KVS
	kds                  aws.Kinesis
	componentsDoneNotify map[string]chan struct{}
	logger               *zap.Logger
}

func MustNewConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	config := &ConsumerConfig{
		ID:                                  uuid.NewString(),
		Name:                                name,
		StreamName:                          streamName,
		EfoConsumerArn:                      efoConsumerArn,
		ProcessFn:                           processFn,
		ManagerID:                           1,
		EtcdListenPeerAddress:               "0.0.0.0",
		EtcdListenClientAddress:             "0.0.0.0",
		ManagerListenAddress:                "0.0.0.0",
		EtcdPeerUrls:                        "http://0.0.0.0:11001",
		EtcdClientUrls:                      "http://0.0.0.0:12001",
		ManagerUrls:                         "http://0.0.0.0:13001",
		LeadershipTtlSeconds:                60,
		HealthcheckTimeoutMilliseconds:      1000,
		WorkerInactivityTimeoutMilliseconds: 5000,
		ShardReleaseTimeoutMilliseconds:     3000,
		EtcdStartTimeoutSeconds:             30,
	}

	for _, opt := range opts {
		opt(config)
	}

	logger := config.logger
	if logger == nil {
		l, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}
		logger = l
	}

	return &Consumer{
		cfg:                  config,
		done:                 make(chan struct{}),
		stop:                 make(chan struct{}),
		componentsDoneNotify: make(map[string]chan struct{}),
		processFn:            processFn,
		logger:               logger.Named("consumer").With(zap.String("name", config.Name), zap.String("workerid", config.ID)),
	}
}

func MustNewDevelopmentConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	allOpts := append(opts, WithLeadershipTtlSeconds(5), WithHealthCheckTimeoutMilliseconds(1), WithEtcdStartTimeoutSeconds(10), WithLogger(logger))
	return MustNewConsumer(name, streamName, efoConsumerArn, processFn, allOpts...)
}

func (c *Consumer) Start() error {
	go func() error {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   c.cfg.GetClientConnectionUrls(),
			DialTimeout: 5 * time.Second,
		})

		if err != nil {
			return err
		}

		c.kvs = cli

		if c.cfg.KinesisClient == nil {
			cfg, err := config.LoadDefaultConfig(context.Background())
			if err != nil {
				return err
			}
			c.kds = kinesis.NewFromConfig(cfg)
		} else {
			c.kds = c.cfg.KinesisClient
		}

		if c.cfg.ManagerID != 0 {
			mgr := NewManagerService(c.cfg, c.kds, c.kvs, c.stop, c.logger)
			err = mgr.Start()
			if err != nil {
				return err
			}
			c.componentsDoneNotify["ManagerService"] = mgr.done

			etcdServer := NewEtcdServer(c.cfg, c.stop, c.logger)
			err = etcdServer.Start()
			if err != nil {
				return err
			}
			c.componentsDoneNotify["EtcdServer"] = etcdServer.done

			leader := NewLeader(c.cfg, mgr, cli, c.stop, c.logger)
			c.componentsDoneNotify["Leader"] = leader.done
			leader.Start()
		}

		worker := NewWorker(c.cfg, c.kds, c.stop, c.logger)
		c.componentsDoneNotify["Worker"] = worker.done
		worker.Start()
		return nil
	}()

	return nil
}

func (c *Consumer) Stop() {
	close(c.stop)
	for k, v := range c.componentsDoneNotify {
		c.logger.Info("shutdown initiated", zap.String("component", k))
		<-v
		c.logger.Info("shutdown complete", zap.String("component", k))
	}
	close(c.done)
}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}
