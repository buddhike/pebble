package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"go.uber.org/zap"
)

type shard struct {
	Shard    *types.Shard
	Parents  map[string]*shard
	Children map[string]*shard
}

type ShardDiscoveryService struct {
	cfg         *PopConfig
	mut         *sync.Mutex
	shards      map[string]*shard
	kds         aws.Kinesis
	done        chan struct{}
	stop        chan struct{}
	initialised chan struct{}
	logger      *zap.Logger
	timer       func(time.Duration) <-chan time.Time
}

func NewShardDiscoveryService(cfg *PopConfig, kds aws.Kinesis, stop chan struct{}, logger *zap.Logger) *ShardDiscoveryService {
	return &ShardDiscoveryService{
		cfg:         cfg,
		mut:         &sync.Mutex{},
		shards:      make(map[string]*shard),
		kds:         kds,
		done:        make(chan struct{}),
		initialised: make(chan struct{}),
		stop:        stop,
		logger:      logger.Named("shard-discovery-service"),
		timer:       time.After,
	}
}

func (svc *ShardDiscoveryService) Start() {
	go func() {
		defer close(svc.done)
		initialized := false
		defer func() {
			// Unblock anyone calling Roots or GetChildren before exiting
			if !initialized {
				close(svc.initialised)
			}
		}()

		for {
			select {
			case <-svc.stop:
				return
			default:
				err := svc.discoverOnce()
				if err != nil {
					break
				}
				if !initialized {
					close(svc.initialised)
					initialized = true
				}
			}

			select {
			case <-svc.stop:
				return
			case <-svc.timer(svc.cfg.ShardDiscoveryInterval()):
			}
		}
	}()
}

func (svc *ShardDiscoveryService) discoverOnce() error {
	shards, err := svc.enumerateAllShards()
	if err != nil {
		svc.logger.Error("failed to discover shards", zap.Error(err))
		return err
	}

	svc.mut.Lock()
	defer svc.mut.Unlock()

	for _, ks := range shards {
		s := svc.shards[*ks.ShardId]
		if s == nil {
			s = &shard{Shard: ks, Parents: make(map[string]*shard), Children: make(map[string]*shard)}
			svc.shards[*ks.ShardId] = s
		}
		s.Shard = ks
		if ks.ParentShardId != nil {
			p := svc.shards[*ks.ParentShardId]
			if p == nil {
				p = &shard{Parents: make(map[string]*shard), Children: make(map[string]*shard)}
			}
			s.Parents[*ks.ParentShardId] = p
			p.Children[*ks.ShardId] = s
		}
		if ks.AdjacentParentShardId != nil {
			p := svc.shards[*ks.AdjacentParentShardId]
			if p == nil {
				p = &shard{Parents: make(map[string]*shard), Children: make(map[string]*shard)}
			}
			s.Parents[*ks.AdjacentParentShardId] = p
			p.Children[*ks.ShardId] = s
		}
	}

	return nil
}

func (svc *ShardDiscoveryService) enumerateAllShards() ([]*types.Shard, error) {
	var shards []*types.Shard
	var nextToken *string
	for {
		input := &kinesis.ListShardsInput{
			StreamName: &svc.cfg.StreamName,
			NextToken:  nextToken,
		}
		out, err := svc.kds.ListShards(context.Background(), input)
		if err != nil {
			svc.logger.Error("failed to list shards", zap.Error(err))
			return nil, err
		}
		for _, s := range out.Shards {
			shards = append(shards, &s)
		}
		nextToken = out.NextToken
		if nextToken == nil {
			break
		}
	}

	return shards, nil
}

func (svc *ShardDiscoveryService) GetAll() []*types.Shard {
	<-svc.initialised
	svc.mut.Lock()
	defer svc.mut.Unlock()
	var r []*types.Shard
	for _, v := range svc.shards {
		if v.Shard != nil {
			r = append(r, v.Shard)
		}
	}
	return r
}

func (svc *ShardDiscoveryService) GetRoots() []*types.Shard {
	<-svc.initialised
	svc.mut.Lock()
	defer svc.mut.Unlock()
	var r []*types.Shard
	for _, v := range svc.shards {
		if v.Shard.ParentShardId == nil || svc.shards[*v.Shard.ParentShardId] == nil || svc.shards[*v.Shard.ParentShardId].Shard == nil {
			r = append(r, v.Shard)
		}
	}
	return r
}

func (svc *ShardDiscoveryService) GetChildren(shardID string) []*types.Shard {
	<-svc.initialised
	svc.mut.Lock()
	defer svc.mut.Unlock()

	s := svc.shards[shardID]
	var r []*types.Shard
	for _, shard := range s.Children {
		r = append(r, shard.Shard)
	}
	return r
}
