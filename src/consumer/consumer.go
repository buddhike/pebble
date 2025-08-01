package consumer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Consumer struct {
	cfg                  *ConsumerConfig
	done                 chan struct{}
	stop                 chan struct{}
	processFn            func(types.Record)
	kds                  aws.Kinesis
	componentsDoneNotify map[string]chan struct{}
	logger               *zap.Logger
	pop                  *PopService
}

func MustNewConsumer(name, streamName, efoConsumerArn, popUrls string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	cfg := &ConsumerConfig{
		ID:                                  uuid.NewString(),
		Name:                                name,
		StreamName:                          streamName,
		EfoConsumerArn:                      efoConsumerArn,
		ProcessFn:                           processFn,
		PopUrls:                             popUrls,
		HealthcheckTimeoutMilliseconds:      1000,
		WorkerInactivityTimeoutMilliseconds: 5000,
		CheckpointIntervalMilliseconds:      60000,
		CheckpointRetryIntervalMilliseconds: 1000,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	logger := cfg.logger
	if logger == nil {
		l, err := zap.NewProduction()
		if err != nil {
			panic(err)
		}
		logger = l
	}

	if cfg.KinesisClient == nil {
		awsCfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(err)
		}
		cfg.KinesisClient = kinesis.NewFromConfig(awsCfg)
	}

	return &Consumer{
		cfg:                  cfg,
		done:                 make(chan struct{}),
		stop:                 make(chan struct{}),
		componentsDoneNotify: make(map[string]chan struct{}),
		kds:                  cfg.KinesisClient,
		processFn:            processFn,
		logger:               logger.Named("consumer").With(zap.String("name", cfg.Name), zap.String("workerid", cfg.ID)),
	}
}

func MustNewStandaloneConsumer(name, streamName, efoConsumerArn string, processFn func(types.Record), opts ...func(*ConsumerConfig)) *Consumer {
	opts = append(opts, AsStandalone())
	return MustNewConsumer(name, streamName, efoConsumerArn, "", processFn, opts...)
}

func (c *Consumer) Start() error {
	go func() error {
		if c.cfg.StandaloneConsumer {
			c.pop = NewPopService(c.cfg.Name, c.cfg.StreamName)
			err := c.pop.Start()
			if err != nil {
				return err
			}
			c.cfg.PopUrls = c.pop.cfg.PopUrls
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
	if c.pop != nil {
		c.pop.Stop()
		<-c.pop.Done()
	}
	close(c.done)

}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}
