package consumer

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ConsumerConfig struct {
	StreamName              string
	EfoConsumerArn          string
	ProcessFn               func(types.Record)
	Name                    string
	ManagerID               int
	KinesisClient           *kinesis.Client
	ManagerUrls             string
	EtcdPeerUrls            string
	EtcdClientUrls          string
	ManagerListenAddress    string
	EtcdListenClientAddress string
	EtcdListenPeerAddress   string
	LeadershipTtlSeconds    int
}

func (cfg *ConsumerConfig) GetClientConnectionUrls() []string {
	return strings.Split(cfg.EtcdClientUrls, ",")
}

func (cfg *ConsumerConfig) GetListenPeerUrls() ([]url.URL, error) {
	peerUrls := strings.Split(cfg.EtcdPeerUrls, ",")
	peerUrl := peerUrls[cfg.ManagerID-1]
	e, err := url.Parse(peerUrl)
	if err != nil {
		return nil, err
	}
	listenPeerUrl := fmt.Sprintf("%s://%s:%s", e.Scheme, cfg.EtcdListenPeerAddress, e.Port())
	e, err = url.Parse(listenPeerUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *ConsumerConfig) GetAdvertisePeerUrls() ([]url.URL, error) {
	peerUrls := strings.Split(cfg.EtcdPeerUrls, ",")
	peerUrl := peerUrls[cfg.ManagerID-1]
	e, err := url.Parse(peerUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *ConsumerConfig) GetListenClientUrls() ([]url.URL, error) {
	clientUrls := strings.Split(cfg.EtcdClientUrls, ",")
	clientUrl := clientUrls[cfg.ManagerID-1]
	e, err := url.Parse(clientUrl)
	if err != nil {
		return nil, err
	}
	listenClientUrl := fmt.Sprintf("%s://%s:%s", e.Scheme, cfg.EtcdListenClientAddress, e.Port())
	e, err = url.Parse(listenClientUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *ConsumerConfig) GetAdvertiseClientUrls() ([]url.URL, error) {
	clientUrls := strings.Split(cfg.EtcdClientUrls, ",")
	clientUrl := clientUrls[cfg.ManagerID-1]
	e, err := url.Parse(clientUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *ConsumerConfig) GetManagerListenUrl() (*url.URL, error) {
	managerUrls := strings.Split(cfg.ManagerUrls, ",")
	managerUrl := managerUrls[cfg.ManagerID-1]
	e, err := url.Parse(managerUrl)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (cfg *ConsumerConfig) GetInitialCluster() string {
	urls := make([]string, 0)
	peers := strings.Split(cfg.EtcdPeerUrls, ",")
	for i, p := range peers {
		urls = append(urls, fmt.Sprintf("%s-%d=%s", cfg.Name, i+1, p))
	}
	return strings.Join(urls, ",")
}

func (cfg *ConsumerConfig) GetEtcdPeerName() string {
	return fmt.Sprintf("%s-%d", cfg.Name, cfg.ManagerID)
}

type Consumer struct {
	cfg       *ConsumerConfig
	done      chan struct{}
	processFn func(types.Record)
	kvs       KVS
	kds       aws.Kinesis
}

func NewConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	config := &ConsumerConfig{
		Name:                    name,
		StreamName:              streamName,
		EfoConsumerArn:          efoConsumerArn,
		ProcessFn:               processFn,
		ManagerID:               1,
		EtcdListenPeerAddress:   "0.0.0.0",
		EtcdListenClientAddress: "0.0.0.0",
		ManagerListenAddress:    "0.0.0.0",
		EtcdPeerUrls:            "http://0.0.0.0:11001",
		EtcdClientUrls:          "http://0.0.0.0:12001",
		ManagerUrls:             "http://0.0.0.0:13001",
		LeadershipTtlSeconds:    60,
	}

	for _, opt := range opts {
		opt(config)
	}

	return &Consumer{
		cfg:       config,
		done:      make(chan struct{}),
		processFn: processFn,
	}
}

func NewDevelopmentConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	allOpts := append(opts, WithLeadershipTtlSeconds(5))
	return NewConsumer(name, streamName, efoConsumerArn, processFn, allOpts...)
}

func WithManagerListenAddress(addr string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.ManagerListenAddress = addr
	}
}

func WithEtcdListenPeerAddress(addr string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.EtcdListenPeerAddress = addr
	}
}

func WithEtcdListenClientAddress(addr string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.EtcdListenClientAddress = addr
	}
}

func WithManagerUrls(urls string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.ManagerUrls = urls
	}
}

func WithEtcdPeerUrls(urls string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.EtcdPeerUrls = urls
	}
}

func WithEtcdClientUrls(urls string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.EtcdClientUrls = urls
	}
}

func WithKinesisClient(kds *kinesis.Client) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.KinesisClient = kds
	}
}

func WithLeadershipTtlSeconds(seconds int) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.LeadershipTtlSeconds = seconds
	}
}

func WithManagerID(id int) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.ManagerID = id
	}
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

		stop := make(chan struct{})

		etcdServer := NewEtcdServer(c.cfg, stop)
		err = etcdServer.Start()
		if err != nil {
			return err
		}

		mgr := NewManagerService(c.cfg, c.kds, c.kvs, stop)
		err = mgr.Start()
		if err != nil {
			return err
		}

		leader := NewLeader(c.cfg, mgr, cli, stop)
		leader.Start()

		worker := NewWorker(c.cfg, c.kds, stop)
		worker.Start()
		return nil
	}()

	return nil
}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}
