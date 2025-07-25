package consumer

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"go.uber.org/zap"
)

type shard struct {
	Shard    *types.Shard
	parents  map[string]*shard
	children map[string]*shard
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
			case <-time.After(svc.cfg.ShardDiscoveryInterval()):
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

	unresolvedParents := make(map[string]struct{})
	for _, ks := range shards {
		s := svc.shards[*ks.ShardId]
		if s == nil {
			s = &shard{Shard: ks, parents: make(map[string]*shard), children: make(map[string]*shard)}
			svc.shards[*ks.ShardId] = s
		}
		s.Shard = ks
		if ks.ParentShardId != nil {
			p := svc.shards[*ks.ParentShardId]
			if p == nil {
				p = &shard{}
				unresolvedParents[*ks.ParentShardId] = struct{}{}
			}
			s.parents[*ks.ParentShardId] = p
			p.children[*ks.ShardId] = s
		}
		if ks.AdjacentParentShardId != nil {
			p := svc.shards[*ks.AdjacentParentShardId]
			if p == nil {
				p = &shard{}
				unresolvedParents[*ks.AdjacentParentShardId] = struct{}{}
			}
			s.parents[*ks.AdjacentParentShardId] = p
			p.children[*ks.ShardId] = s
		}
		delete(unresolvedParents, *ks.ShardId)
	}
	if len(unresolvedParents) > 0 {
		u := slices.Collect(maps.Values(unresolvedParents))
		panic(fmt.Sprintf("parents shards for following shards were not found: %v", u))
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
		r = append(r, v.Shard)
	}
	return r
}

func (svc *ShardDiscoveryService) GetRoots() []*types.Shard {
	<-svc.initialised
	svc.mut.Lock()
	defer svc.mut.Unlock()
	var r []*types.Shard
	for _, v := range svc.shards {
		if v.Shard.ParentShardId == nil || svc.shards[*v.Shard.ParentShardId] == nil {
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
	for _, shard := range s.children {
		r = append(r, shard.Shard)
	}
	return r
}
