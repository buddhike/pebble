package consumer

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"go.uber.org/zap"
)

type PopConfig struct {
	Name                                string
	PopID                               int
	StreamName                          string
	KinesisClient                       *kinesis.Client
	PopUrls                             string
	PopListenAddress                    string
	EtcdPeerUrls                        string
	EtcdListenPeerAddress               string
	EtcdClientUrls                      string
	EtcdListenClientAddress             string
	LeadershipTtlSeconds                int
	HealthcheckTimeoutMilliseconds      int
	WorkerInactivityTimeoutMilliseconds int
	ShardReleaseTimeoutMilliseconds     int
	EtcdStartTimeoutSeconds             int
	logger                              *zap.Logger
}

func (cfg *PopConfig) GetClientConnectionUrls() []string {
	return strings.Split(cfg.EtcdClientUrls, ",")
}

func (cfg *PopConfig) GetListenPeerUrls() ([]url.URL, error) {
	peerUrls := strings.Split(cfg.EtcdPeerUrls, ",")
	peerUrl := peerUrls[cfg.PopID-1]
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

func (cfg *PopConfig) GetAdvertisePeerUrls() ([]url.URL, error) {
	peerUrls := strings.Split(cfg.EtcdPeerUrls, ",")
	peerUrl := peerUrls[cfg.PopID-1]
	e, err := url.Parse(peerUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *PopConfig) GetListenClientUrls() ([]url.URL, error) {
	clientUrls := strings.Split(cfg.EtcdClientUrls, ",")
	clientUrl := clientUrls[cfg.PopID-1]
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

func (cfg *PopConfig) GetAdvertiseClientUrls() ([]url.URL, error) {
	clientUrls := strings.Split(cfg.EtcdClientUrls, ",")
	clientUrl := clientUrls[cfg.PopID-1]
	e, err := url.Parse(clientUrl)
	if err != nil {
		return nil, err
	}
	return []url.URL{*e}, nil
}

func (cfg *PopConfig) GetManagerListenUrl() (*url.URL, error) {
	managerUrls := strings.Split(cfg.PopUrls, ",")
	managerUrl := managerUrls[cfg.PopID-1]
	e, err := url.Parse(managerUrl)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (cfg *PopConfig) GetInitialCluster() string {
	urls := make([]string, 0)
	peers := strings.Split(cfg.EtcdPeerUrls, ",")
	for i, p := range peers {
		urls = append(urls, fmt.Sprintf("%s-%d=%s", cfg.Name, i+1, p))
	}
	return strings.Join(urls, ",")
}

func (cfg *PopConfig) WorkerInactivityTimeout() time.Duration {
	return time.Duration(cfg.WorkerInactivityTimeoutMilliseconds) * time.Millisecond
}

func (cfg *PopConfig) GetEtcdPeerName() string {
	return fmt.Sprintf("%s-%d", cfg.Name, cfg.PopID)
}

func (cfg *PopConfig) GetCurrentNodeClientConnectionUrl() string {
	return cfg.GetClientConnectionUrls()[cfg.PopID-1]
}

func (cfg *PopConfig) ShardReleaseTimeout() time.Duration {
	return time.Duration(cfg.ShardReleaseTimeoutMilliseconds) * time.Millisecond
}

func (cfg *PopConfig) HealthcheckTimeout() time.Duration {
	return time.Duration(cfg.HealthcheckTimeoutMilliseconds) * time.Millisecond
}

func WithPopListenAddress(addr string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.PopListenAddress = addr
	}
}

func WithEtcdListenPeerAddress(addr string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.EtcdListenPeerAddress = addr
	}
}

func WithEtcdListenClientAddress(addr string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.EtcdListenClientAddress = addr
	}
}

func WithManagerPopUrls(urls string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.PopUrls = urls
	}
}

func WithEtcdPeerUrls(urls string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.EtcdPeerUrls = urls
	}
}

func WithEtcdClientUrls(urls string) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.EtcdClientUrls = urls
	}
}

func WithLeadershipTtlSeconds(seconds int) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.LeadershipTtlSeconds = seconds
	}
}

func WithID(id int) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.PopID = id
	}
}

func WithHealthCheckTimeoutMilliseconds(timeout int) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.HealthcheckTimeoutMilliseconds = timeout
	}
}

func WithWorkerInactivityTimeoutMilliseconds(timeout int) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.WorkerInactivityTimeoutMilliseconds = timeout
	}
}

func WithEtcdStartTimeoutSeconds(timeout int) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.EtcdStartTimeoutSeconds = timeout
	}
}

func WithManagerLogger(logger *zap.Logger) func(*PopConfig) {
	return func(cfg *PopConfig) {
		cfg.logger = logger
	}
}
