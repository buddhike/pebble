package consumer

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"
)

type ConsumerConfig struct {
	ID                         string
	StreamName                 string
	EfoConsumerArn             string
	ProcessFn                  func(types.Record)
	Name                       string
	ManagerID                  int
	KinesisClient              *kinesis.Client
	ManagerUrls                string
	EtcdPeerUrls               string
	EtcdClientUrls             string
	ManagerListenAddress       string
	EtcdListenClientAddress    string
	EtcdListenPeerAddress      string
	LeadershipTtlSeconds       int
	HealthcheckTimeoutSeconds  int
	ShardReleaseTimeoutSeconds int
	EtcdStartTimeoutSeconds    int
	logger                     *zap.Logger
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

func (cfg *ConsumerConfig) GetCurrentNodeClientConnectionUrl() string {
	return cfg.GetClientConnectionUrls()[cfg.ManagerID-1]
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

func WithHealthCheckTimeoutSeconds(timeout int) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.HealthcheckTimeoutSeconds = timeout
	}
}

func WithEtcdStartTimeoutSeconds(timeout int) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.EtcdStartTimeoutSeconds = timeout
	}
}

func WithLogger(logger *zap.Logger) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.logger = logger
	}
}
