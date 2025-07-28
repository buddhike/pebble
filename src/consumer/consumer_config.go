package consumer

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.uber.org/zap"
)

type ConsumerConfig struct {
	ID                                  string
	StreamName                          string
	EfoConsumerArn                      string
	ProcessFn                           func(types.Record)
	Name                                string
	KinesisClient                       *kinesis.Client
	PopUrls                             string
	HealthcheckTimeoutMilliseconds      int
	WorkerInactivityTimeoutMilliseconds int
	CheckpointIntervalMilliseconds      int
	StandaloneConsumer                  bool
	logger                              *zap.Logger
}

func (cfg *ConsumerConfig) HealthcheckTimeout() time.Duration {
	return time.Duration(cfg.HealthcheckTimeoutMilliseconds) * time.Millisecond
}

func (cfg *ConsumerConfig) WorkerInactivityTimeout() time.Duration {
	return time.Duration(cfg.WorkerInactivityTimeoutMilliseconds) * time.Millisecond
}

func (cfg *ConsumerConfig) CheckpointInterval() time.Duration {
	return time.Duration(cfg.CheckpointIntervalMilliseconds) * time.Millisecond
}

func WithKinesisClient(kds *kinesis.Client) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.KinesisClient = kds
	}
}

func WithLogger(logger *zap.Logger) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.logger = logger
	}
}

func WithPopUrls(urls string) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.PopUrls = urls
	}
}

func WithCheckpointInterval(t int) func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.CheckpointIntervalMilliseconds = t
	}
}

func AsStandalone() func(*ConsumerConfig) {
	return func(cfg *ConsumerConfig) {
		cfg.StandaloneConsumer = true
	}
}
