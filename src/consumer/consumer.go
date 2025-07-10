package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Consumer struct {
	cfg                  *ConsumerConfig
	done                 chan struct{}
	stop                 chan struct{}
	processFn            func(types.Record)
	kvs                  KVS
	kds                  aws.Kinesis
	componentsDoneNotify map[string]chan struct{}
}

func NewConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	config := &ConsumerConfig{
		Name:                      name,
		StreamName:                streamName,
		EfoConsumerArn:            efoConsumerArn,
		ProcessFn:                 processFn,
		ManagerID:                 1,
		EtcdListenPeerAddress:     "0.0.0.0",
		EtcdListenClientAddress:   "0.0.0.0",
		ManagerListenAddress:      "0.0.0.0",
		EtcdPeerUrls:              "http://0.0.0.0:11001",
		EtcdClientUrls:            "http://0.0.0.0:12001",
		ManagerUrls:               "http://0.0.0.0:13001",
		LeadershipTtlSeconds:      60,
		HealthcheckTimeoutSeconds: 5,
		EtcdStartTimeoutSeconds:   30,
	}

	for _, opt := range opts {
		opt(config)
	}

	return &Consumer{
		cfg:                  config,
		done:                 make(chan struct{}),
		stop:                 make(chan struct{}),
		componentsDoneNotify: make(map[string]chan struct{}),
		processFn:            processFn,
	}
}

func NewDevelopmentConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	allOpts := append(opts, WithLeadershipTtlSeconds(5), WithHealthCheckTimeoutSeconds(1), WithEtcdStartTimeoutSeconds(10))
	return NewConsumer(name, streamName, efoConsumerArn, processFn, allOpts...)
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

		etcdServer := NewEtcdServer(c.cfg, c.stop)
		err = etcdServer.Start()
		if err != nil {
			return err
		}
		c.componentsDoneNotify["EtcdServer"] = etcdServer.done

		mgr := NewManagerService(c.cfg, c.kds, c.kvs, c.stop)
		err = mgr.Start()
		if err != nil {
			return err
		}
		c.componentsDoneNotify["ManagerService"] = mgr.done

		leader := NewLeader(c.cfg, mgr, cli, c.stop)
		c.componentsDoneNotify["Leader"] = leader.done
		leader.Start()

		worker := NewWorker(c.cfg, c.kds, c.stop)
		c.componentsDoneNotify["Worker"] = worker.done
		worker.Start()
		return nil
	}()

	return nil
}

func (c *Consumer) Stop() {
	close(c.stop)
	for k, v := range c.componentsDoneNotify {
		fmt.Printf("Shudown %s: initiated \n", k)
		<-v
		fmt.Printf("Shudown %s: ok \n", k)
	}
	close(c.done)
}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}
