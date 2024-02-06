package vegas

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/vegas/pb"
	"google.golang.org/protobuf/proto"
)

type RecordProcessor func(*pb.UserRecord) error

type Consumer struct {
	sm   *subscriptionManager
	done chan struct{}
}

func (c *Consumer) Done() <-chan struct{} {
	return c.done
}

func NewConsumer(streamName, consumerARN string, p RecordProcessor) (*Consumer, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	kc := kinesis.NewFromConfig(cfg)
	d, err := kc.DescribeStream(context.TODO(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		return nil, err
	}
	sm := &subscriptionManager{
		streamARN:   d.StreamDescription.StreamARN,
		consumerARN: &consumerARN,
		kc:          kc,
		p:           p,
		rfs:         []recordFilter{newInvalidMappingFilter()},
		urfs:        []userRecordFilter{newDedupFilter()},
	}
	go sm.Start()
	return &Consumer{
		sm:   sm,
		done: make(chan struct{}),
	}, nil
}

type subscriptionManager struct {
	streamARN   *string
	consumerARN *string
	kc          consumerClient
	p           RecordProcessor
	rfs         []recordFilter
	urfs        []userRecordFilter
	done        chan struct{}
}

func (m *subscriptionManager) Start() {
	l, err := m.kc.ListShards(context.TODO(), &kinesis.ListShardsInput{
		StreamARN: m.streamARN,
	})
	if err != nil {
		panic(err)
	}
	p := make(map[string]bool)
	for _, s := range l.Shards {
		p[*s.ShardId] = true
	}
	children := make(chan []types.ChildShard)
	for _, s := range l.Shards {
		if (s.ParentShardId == nil && s.AdjacentParentShardId == nil) ||
			(s.ParentShardId != nil && !p[*s.ParentShardId]) &&
				s.AdjacentParentShardId == nil ||
			(s.AdjacentParentShardId != nil && !p[*s.AdjacentParentShardId]) {
			r := &shardReader{
				consumerARN: m.consumerARN,
				c:           &shardReaderContext{shardID: *s.ShardId},
				kc:          m.kc,
				p:           m.p,
				rfs:         m.rfs,
				urfs:        m.urfs,
				done:        m.done,
				children:    children,
			}
			go r.Start()
		}
	}
	for {
		select {
		case ch := <-children:
			for _, c := range ch {
				r := &shardReader{
					consumerARN: m.consumerARN,
					c:           &shardReaderContext{shardID: *c.ShardId},
					kc:          m.kc,
					p:           m.p,
					rfs:         m.rfs,
					urfs:        m.urfs,
					done:        m.done,
					children:    children,
				}
				go r.Start()
			}
		case <-m.done:
			return
		}
	}
}

type shardReader struct {
	consumerARN *string
	kc          consumerClient
	p           RecordProcessor
	c           *shardReaderContext
	rfs         []recordFilter
	urfs        []userRecordFilter
	done        chan struct{}
	children    chan<- []types.ChildShard
}

func (r *shardReader) Start() {
	s, err := r.kc.SubscribeToShard(context.TODO(), &kinesis.SubscribeToShardInput{
		ShardId:     &r.c.shardID,
		ConsumerARN: r.consumerARN,
		StartingPosition: &types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		},
	})
	if err != nil {
		panic(err)
	}
	for {
		c, seq := r.consume(s.GetStream().Events())
		if c {
			s, err = r.kc.SubscribeToShard(context.TODO(), &kinesis.SubscribeToShardInput{
				ShardId:     &r.c.shardID,
				ConsumerARN: r.consumerARN,
				StartingPosition: &types.StartingPosition{
					Type:           types.ShardIteratorTypeAtSequenceNumber,
					SequenceNumber: seq,
				},
			})
			if err != nil {
				panic(err)
			}
		}
	}
}

func (r *shardReader) consume(s <-chan types.SubscribeToShardEventStream) (bool, *string) {
	var continuationSeq *string
	for {
		select {
		case i, ok := <-s:
			if !ok {
				return true, continuationSeq
			}
			if te, ok := i.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
				value := te.Value
				if len(value.ChildShards) > 0 {
					r.children <- value.ChildShards
					return false, nil
				}
				for _, kr := range value.Records {
					rcd := pb.Record{}
					err := proto.Unmarshal(kr.Data, &rcd)
					if err != nil {
						panic(err)
					}

					if r.shouldFilterRecord(&rcd) {
						continue
					}

					for _, ur := range rcd.UserRecords {
						if r.shouldFilterUserRecord(ur) {
							continue
						}
						err := r.p(ur)
						if err != nil {
							panic(err)
						}
					}
				}
				continuationSeq = value.ContinuationSequenceNumber
			}
		case <-r.done:
			return false, nil
		}
	}
}

func (r *shardReader) shouldFilterRecord(rcd *pb.Record) bool {
	for _, rf := range r.rfs {
		if rf.Apply(r.c, rcd) {
			return true
		}
	}
	return false
}

func (r *shardReader) shouldFilterUserRecord(rcd *pb.UserRecord) bool {
	for _, rf := range r.urfs {
		if rf.Apply(r.c, rcd) {
			return true
		}
	}
	return false
}

type recordFilter interface {
	Apply(*shardReaderContext, *pb.Record) bool
}

type userRecordFilter interface {
	Apply(*shardReaderContext, *pb.UserRecord) bool
}

type shardReaderContext struct {
	shardID string
}

type invalidMappingFilter struct {
}

func (f *invalidMappingFilter) Apply(c *shardReaderContext, r *pb.Record) bool {
	return r.ShardID != c.shardID
}

func newInvalidMappingFilter() *invalidMappingFilter {
	return &invalidMappingFilter{}
}

type dedupFilter struct {
	l    *sync.Mutex
	seen map[string]bool
}

func (s *dedupFilter) Apply(c *shardReaderContext, r *pb.UserRecord) bool {
	s.l.Lock()
	defer s.l.Unlock()

	h := md5.Sum(r.RecordID)
	k := hex.EncodeToString(h[:])
	if _, ok := s.seen[k]; ok {
		return ok
	}
	s.seen[k] = true
	return false
}

func newDedupFilter() *dedupFilter {
	return &dedupFilter{
		l:    &sync.Mutex{},
		seen: make(map[string]bool),
	}
}

type consumerClient interface {
	SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}
