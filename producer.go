package vegas

import (
	"context"
	"crypto/md5"
	"errors"
	"math/big"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/buddhike/vegas/pb"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	stream   string
	shardMap *shardMap
	done     chan struct{}
}

func (p *Producer) Send(ctx context.Context, partitionKey string, data []byte) error {
	rid := md5.Sum(data)
	return p.SendWithRecordID(ctx, partitionKey, rid[:], data)
}

func (p *Producer) SendWithRecordID(ctx context.Context, partitionKey string, recordID, data []byte) error {
	for {
		rr := &resolveRequest{
			partitionKey: partitionKey,
			response:     make(chan *shardWriter),
		}
		p.shardMap.request <- rr
		m := <-rr.response
		wr := &writeRequest{
			UserRecord: &pb.UserRecord{
				PartitionKey: partitionKey,
				RecordID:     recordID,
				Data:         data,
			},
			Response: make(chan *writeResponse, 1),
		}
		select {
		case m.input <- wr:
			return nil
		case <-m.close:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case wrr := <-wr.Response:
			return wrr.Err
		}
	}
}

func (p *Producer) Done() <-chan struct{} {
	return p.done
}

func NewProducer(streamName string, bufferSize, batchSize, batchTimeoutMS int) (*Producer, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}
	kc := kinesis.NewFromConfig(cfg)
	s, err := kc.DescribeStream(context.TODO(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		return nil, err
	}
	sm := newShardMap(*s.StreamDescription.StreamARN, bufferSize, batchSize, batchTimeoutMS, kc)
	go sm.Start()
	return &Producer{
		stream:   streamName,
		shardMap: sm,
	}, nil
}

type resolveRequest struct {
	partitionKey string
	response     chan *shardWriter
}

type shardMapEntry struct {
	start  big.Int
	end    big.Int
	writer *shardWriter
}

type shardMap struct {
	streamARN      *string
	version        int
	request        chan *resolveRequest
	done           chan struct{}
	invalidations  chan int
	kc             kinesisClient
	bufferSize     int
	batchSize      int
	batchTimeoutMS int
	entries        []shardMapEntry
}

func (m *shardMap) Start() {
	m.build()
forever:
	for {
		select {
		case rr := <-m.request:
			w := m.resolve(rr.partitionKey)
			rr.response <- w
		case v := <-m.invalidations:
			if m.version == v {
				m.rebuild()
			}
		case <-m.done:
			break forever
		}
	}
}

func (m *shardMap) build() {
	m.version++
	m.entries = m.entries[:0]
	o, err := m.kc.ListShards(context.TODO(), &kinesis.ListShardsInput{
		StreamARN: m.streamARN,
	})
	if err != nil {
		panic(err)
	}

	p := make(map[string]bool)
	for _, s := range o.Shards {
		if s.ParentShardId != nil {
			p[*s.ParentShardId] = true
		}
		if s.AdjacentParentShardId != nil {
			p[*s.AdjacentParentShardId] = true
		}
	}

	for _, s := range o.Shards {
		if _, ok := p[*s.ShardId]; !ok {
			e := shardMapEntry{}
			e.start.SetString(*s.HashKeyRange.StartingHashKey, 10)
			e.end.SetString(*s.HashKeyRange.EndingHashKey, 10)
			e.writer = newShardWriter(m.streamARN, s.ShardId, s.HashKeyRange.StartingHashKey, m.bufferSize, m.batchSize, m.batchTimeoutMS, m.kc, m.done, m.invalidations)
			go e.writer.Start()
			m.entries = append(m.entries, e)
		}
	}
}

func (m *shardMap) resolve(partitionKey string) *shardWriter {
	h := md5.Sum([]byte(partitionKey))
	i := big.NewInt(0).SetBytes(h[0:])
	for _, en := range m.entries {
		s := i.Cmp(&en.start)
		e := i.Cmp(&en.end)
		if s >= 0 && e <= 0 {
			return en.writer
		}
	}
	panic(errors.New("not found"))
}

func (m *shardMap) rebuild() {
	requests := make([]*writeRequest, 0)
	for _, e := range m.entries {
		r := make(chan []*writeRequest)
		e.writer.drain <- r
		rs := <-r
		requests = append(requests, rs...)
	}
	m.build()
	for _, r := range requests {
		w := m.resolve(r.UserRecord.PartitionKey)
		w.input <- r
	}
}

func newShardMap(streamARN string, bufferSize, batchSize, batchTimeoutMS int, kc kinesisClient) *shardMap {
	return &shardMap{
		streamARN:      &streamARN,
		request:        make(chan *resolveRequest),
		done:           make(chan struct{}),
		invalidations:  make(chan int),
		kc:             kc,
		bufferSize:     bufferSize,
		batchSize:      batchSize,
		batchTimeoutMS: batchTimeoutMS,
	}
}

type writeRequest struct {
	UserRecord *pb.UserRecord
	Response   chan *writeResponse
}

type writeResponse struct {
	Err error
}
type shardWriter struct {
	shardID        *string                   // Shard ID written by this writer
	input          chan *writeRequest        // Receive messages to be written out
	drain          chan chan []*writeRequest // Signaled by ShardMap to collect outstanding writeRequests in this shardWriter's buffer
	close          chan struct{}             // Closed when shardWriter is no longer accepting writeRequests
	done           chan struct{}             // Closed when it's time to abort (e.g. SIGTERM)
	invalidations  chan int                  // Used to notify shard map invalidations detected by this shardWriter
	kinesisClient  kinesisClient
	batchSize      int
	batchTimeoutMS int
	partitionKey   *string
	streamARN      *string
	version        int // Used to identify which iteration of shardMap prep created this shardWriter
}

func (w *shardWriter) Start() {
	batch := make([]*writeRequest, 0)
	send := true
	var sn *string
	batchTimeoutMS := time.Millisecond * time.Duration(w.batchTimeoutMS)
	t := time.NewTimer(batchTimeoutMS)
forever:
	for {
		select {
		case wr, ok := <-w.input:
			if ok {
				batch = append(batch, wr)
				if send && len(batch) >= w.batchSize {
					sn, send, batch = w.sendBatch(batch, sn)
				}
			}
		case <-t.C:
			if send && len(batch) > 0 {
				sn, send, batch = w.sendBatch(batch, sn)
			}
			if send {
				t.Reset(batchTimeoutMS)
			}
		case r := <-w.drain:
			close(w.close)
			for a := range w.input {
				batch = append(batch, a)
			}
			r <- batch
			break forever
		case <-w.done:
			close(w.close)
			break forever
		}
	}
}

func (w *shardWriter) sendBatch(batch []*writeRequest, sequenceNumber *string) (*string, bool, []*writeRequest) {
	ignoredPartitionKey := aws.String("a")
	ur := make([]*pb.UserRecord, 0)
	c := w.batchSize
	if len(batch) < c {
		c = len(batch)
	}
	batch = batch[0:c]
	for _, i := range batch {
		ur = append(ur, i.UserRecord)
	}
	r := &pb.Record{
		ShardID:     *w.shardID,
		UserRecords: ur,
	}
	m, err := proto.Marshal(r)
	if err != nil {
		panic(err)
	}
	p := &kinesis.PutRecordInput{
		PartitionKey:              ignoredPartitionKey, // Ignored because we use ExplicitHashKey
		ExplicitHashKey:           w.partitionKey,
		StreamARN:                 w.streamARN,
		Data:                      m,
		SequenceNumberForOrdering: sequenceNumber,
	}

	o, err := w.kinesisClient.PutRecord(context.TODO(), p)
	if err != nil {
		for _, i := range batch {
			i.Response <- &writeResponse{
				Err: err,
			}
			close(i.Response)
		}
		return sequenceNumber, true, batch[c:]
	} else {
		if *o.ShardId == *w.shardID {
			for _, i := range batch {
				close(i.Response)
			}
			return o.SequenceNumber, true, batch[c:]
		} else {
			// Although the record is written to KDS
			// it's written to the wrong shard. We don't
			// notify Response channel here.
			// New shardWriter assigned to handle the remapped
			// partition will eventually report success or
			// failure.
			// The key motivation here is to not surface
			// shard split/merge details to user API.
			close(w.close)
			w.invalidations <- w.version
			return nil, false, batch
		}
	}
}

func newShardWriter(streamARN, shardID, partitionKey *string, bufferSize int, batchSize, batchTimeoutMS int, kinesisClient kinesisClient, done chan struct{}, invalidations chan int) *shardWriter {
	return &shardWriter{
		streamARN:      streamARN,
		partitionKey:   partitionKey,
		shardID:        shardID,
		input:          make(chan *writeRequest, bufferSize),
		close:          make(chan struct{}),
		drain:          make(chan chan []*writeRequest),
		invalidations:  invalidations,
		done:           done,
		kinesisClient:  kinesisClient,
		batchSize:      batchSize,
		batchTimeoutMS: batchTimeoutMS,
	}
}

type kinesisClient interface {
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
}
