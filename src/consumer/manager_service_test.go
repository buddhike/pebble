package consumer

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestAssignAllShardsToWorker(t *testing.T) {
	svc, w := newTestSubject()
	defer w.CleanUp()

	r := svc.handleAssignRequest(&AssignRequest{
		WorkerID:  "a",
		MaxShards: 1,
	})

	assert.Equal(t, r.Assignments[0].ShardID, "s0")
}

func TestAssignToAWorkerWithoutCapacity(t *testing.T) {
	svc, w := newTestSubject()
	defer w.CleanUp()

	r := svc.handleAssignRequest(&AssignRequest{
		WorkerID:  "a",
		MaxShards: 0,
	})

	assert.Equal(t, len(r.Assignments), 0)
}

func TestAssignAfterInactivityPeriod(t *testing.T) {
	svc, w := newTestSubject()
	defer w.CleanUp()

	r1 := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 1})
	w.ExpireInactiveWorkers()
	r2 := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 2})

	assert.Equal(t, "s0", r1.Assignments[0].ShardID)
	assert.Equal(t, "s0", r2.Assignments[0].ShardID)
	assert.Greater(t, r2.Assignments[0].ID, r1.Assignments[0].ID)
}

func TestAssignWhenThereAreMoreWorkersThanShards(t *testing.T) {
	svc, w := newTestSubject()
	defer w.CleanUp()

	// Worker a is assigned a shard
	r := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 1})
	assert.Equal(t, "s0", r.Assignments[0].ShardID)

	// Worker b should not get any shards
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Empty(t, r.Assignments)

	w.ExpireShardReleaseTimeout()
	r3 := svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Empty(t, r3.Assignments)
}

func TestAssignWhenWorkersAreNotBalanced(t *testing.T) {
	svc, w := newTestSubject("s0", "s1")
	defer w.CleanUp()

	r := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 2})
	assert.Equal(t, "s0", r.Assignments[0].ShardID)
	assert.Equal(t, "s1", r.Assignments[1].ShardID)

	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Empty(t, r.Assignments)

	w.ExpireShardReleaseTimeout()

	// a gets initial assignment if it calls assign before b
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Equal(t, 2, len(r.Assignments))

	// b acquires any reassigned shards
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Equal(t, 1, len(r.Assignments))
	assert.Equal(t, "s0", r.Assignments[0].ShardID)

	// a should now receive the remaining assignment
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Equal(t, 1, len(r.Assignments))
	assert.Equal(t, "s1", r.Assignments[0].ShardID)
}

func TestAssignWhenWorkerBecomesInactiveBeforeItAcquiresReassignments(t *testing.T) {
	svc, w := newTestSubject("s0", "s1")
	defer w.CleanUp()

	r := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 2})
	assert.Equal(t, "s0", r.Assignments[0].ShardID)
	assert.Equal(t, "s1", r.Assignments[1].ShardID)

	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Empty(t, r.Assignments)

	w.ExpireShardReleaseTimeout()

	// ensure a keeps heartbeating
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Len(t, r.Assignments, 2)

	w.ExpireInactiveWorkers()

	// a re-acquires the shard that was offerred to b
	r2 := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Len(t, r2.Assignments, 2)
	assert.Greater(t, r2.Assignments[0].ID, r.Assignments[0].ID)

	// b should not acquire any reassignments
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Len(t, r.Assignments, 0)
}

func TestAssignWhenWorkerReactivatesBeforePreviousReassignmentOfferIsClearedByAnotherWorker(t *testing.T) {
	svc, w := newTestSubject("s0", "s1")
	defer w.CleanUp()

	r := svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 2})
	assert.Equal(t, "s0", r.Assignments[0].ShardID)
	assert.Equal(t, "s1", r.Assignments[1].ShardID)

	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Empty(t, r.Assignments)

	w.ExpireShardReleaseTimeout()

	// ensure a keeps heartbeating
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Len(t, r.Assignments, 2)

	w.ExpireInactiveWorkers()

	// b should not acquire any reassignments
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "b", MaxShards: 1})
	assert.Len(t, r.Assignments, 1)

	// a should see the rebalanced assignments
	r = svc.handleAssignRequest(&AssignRequest{WorkerID: "a", MaxShards: 4})
	assert.Len(t, r.Assignments, 1)
}

type testWorld struct {
	CleanUp              func()
	AdvanceTime          func(by time.Duration)
	AdvanceTimeBySeconds func(s int)
	GetEpoc              func() int
}

func (w *testWorld) ExpireShardReleaseTimeout() {
	w.AdvanceTimeBySeconds(3)
}

func (w *testWorld) ExpireInactiveWorkers() {
	epoc := w.GetEpoc()
	target := (epoc + 5) - ((epoc + 5) % 5)
	w.AdvanceTimeBySeconds(target - epoc)
}

func newTestSubject(shardIds ...string) (*ManagerService, *testWorld) {
	epoc := 0
	clock := time.Time{}
	popCfg := &PopConfig{
		WorkerInactivityTimeoutMilliseconds: 5000,
		ShardReleaseTimeoutMilliseconds:     2000,
	}
	stop := make(chan struct{})
	logger := zap.NewNop()
	kds := newMockKinesis(shardIds...)
	kvs := newMockKVS()
	sd := NewShardDiscoveryService(popCfg, kds, stop, logger)
	sd.Start()

	svc := NewManagerService(popCfg, kds, kvs, sd, stop, logger)
	svc.clock = func() time.Time {
		return clock
	}
	svc.SetToInService(1)

	return svc, &testWorld{
		CleanUp: func() {
			close(stop)
			<-sd.done
		},
		AdvanceTime: func(by time.Duration) {
			clock = clock.Add(by)
		},
		AdvanceTimeBySeconds: func(s int) {
			clock = clock.Add(time.Second * time.Duration(s))
			epoc += s
		},
		GetEpoc: func() int {
			return epoc
		},
	}
}

func newMockKinesis(shardIds ...string) *mockKinesis {
	svc := &mockKinesis{}

	var shards []types.Shard
	for _, shardIdStr := range shardIds {
		parts := strings.Split(shardIdStr, ",")
		shard := types.Shard{
			ShardId: aws.String(strings.TrimSpace(parts[0])),
		}

		// Set parent shard ID if provided (element 1)
		if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
			shard.ParentShardId = aws.String(strings.TrimSpace(parts[1]))
		}

		// Set adjacent parent shard ID if provided (element 2)
		if len(parts) > 2 && strings.TrimSpace(parts[2]) != "" {
			shard.AdjacentParentShardId = aws.String(strings.TrimSpace(parts[2]))
		}

		shards = append(shards, shard)
	}

	// If no shard IDs provided, use a default one
	if len(shards) == 0 {
		shards = []types.Shard{
			{
				ShardId: aws.String("s0"),
			},
		}
	}

	svc.On("ListShards", mock.Anything, mock.Anything, mock.Anything).Return(&kinesis.ListShardsOutput{
		Shards: shards,
	}, nil)
	return svc
}

func newMockKVS() *mockKVS {
	kvs := &mockKVS{}
	kvs.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&clientv3.GetResponse{}, nil)
	return kvs
}

type mockKinesis struct {
	mock.Mock
}

func (m *mockKinesis) AddTagsToStream(ctx context.Context, input *kinesis.AddTagsToStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.AddTagsToStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.AddTagsToStreamOutput), args.Error(1)
}

func (m *mockKinesis) CreateStream(ctx context.Context, input *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.CreateStreamOutput), args.Error(1)
}

func (m *mockKinesis) DecreaseStreamRetentionPeriod(ctx context.Context, input *kinesis.DecreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.DecreaseStreamRetentionPeriodOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DecreaseStreamRetentionPeriodOutput), args.Error(1)
}

func (m *mockKinesis) DeleteStream(ctx context.Context, input *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DeleteStreamOutput), args.Error(1)
}

func (m *mockKinesis) DeregisterStreamConsumer(ctx context.Context, input *kinesis.DeregisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DeregisterStreamConsumerOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DeregisterStreamConsumerOutput), args.Error(1)
}

func (m *mockKinesis) DescribeLimits(ctx context.Context, input *kinesis.DescribeLimitsInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeLimitsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DescribeLimitsOutput), args.Error(1)
}

func (m *mockKinesis) DescribeStreamConsumer(ctx context.Context, input *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DescribeStreamConsumerOutput), args.Error(1)
}

func (m *mockKinesis) DescribeStreamSummary(ctx context.Context, input *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DescribeStreamSummaryOutput), args.Error(1)
}

func (m *mockKinesis) DisableEnhancedMonitoring(ctx context.Context, input *kinesis.DisableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.DisableEnhancedMonitoringOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DisableEnhancedMonitoringOutput), args.Error(1)
}

func (m *mockKinesis) EnableEnhancedMonitoring(ctx context.Context, input *kinesis.EnableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.EnableEnhancedMonitoringOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.EnableEnhancedMonitoringOutput), args.Error(1)
}

func (m *mockKinesis) IncreaseStreamRetentionPeriod(ctx context.Context, input *kinesis.IncreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.IncreaseStreamRetentionPeriodOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.IncreaseStreamRetentionPeriodOutput), args.Error(1)
}

func (m *mockKinesis) ListStreamConsumers(ctx context.Context, input *kinesis.ListStreamConsumersInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamConsumersOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.ListStreamConsumersOutput), args.Error(1)
}

func (m *mockKinesis) ListStreams(ctx context.Context, input *kinesis.ListStreamsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.ListStreamsOutput), args.Error(1)
}

func (m *mockKinesis) ListTagsForStream(ctx context.Context, input *kinesis.ListTagsForStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.ListTagsForStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.ListTagsForStreamOutput), args.Error(1)
}

func (m *mockKinesis) MergeShards(ctx context.Context, input *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.MergeShardsOutput), args.Error(1)
}

func (m *mockKinesis) RegisterStreamConsumer(ctx context.Context, input *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.RegisterStreamConsumerOutput), args.Error(1)
}

func (m *mockKinesis) RemoveTagsFromStream(ctx context.Context, input *kinesis.RemoveTagsFromStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.RemoveTagsFromStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.RemoveTagsFromStreamOutput), args.Error(1)
}

func (m *mockKinesis) SplitShard(ctx context.Context, input *kinesis.SplitShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SplitShardOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.SplitShardOutput), args.Error(1)
}

func (m *mockKinesis) StartStreamEncryption(ctx context.Context, input *kinesis.StartStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StartStreamEncryptionOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.StartStreamEncryptionOutput), args.Error(1)
}

func (m *mockKinesis) StopStreamEncryption(ctx context.Context, input *kinesis.StopStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StopStreamEncryptionOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.StopStreamEncryptionOutput), args.Error(1)
}

func (m *mockKinesis) SubscribeToShard(ctx context.Context, input *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.SubscribeToShardOutput), args.Error(1)
}

func (m *mockKinesis) UpdateShardCount(ctx context.Context, input *kinesis.UpdateShardCountInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateShardCountOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.UpdateShardCountOutput), args.Error(1)
}

func (m *mockKinesis) UpdateStreamMode(ctx context.Context, input *kinesis.UpdateStreamModeInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateStreamModeOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.UpdateStreamModeOutput), args.Error(1)
}

func (m *mockKinesis) DescribeStream(ctx context.Context, input *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.DescribeStreamOutput), args.Error(1)
}

func (m *mockKinesis) GetRecords(ctx context.Context, input *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.GetRecordsOutput), args.Error(1)
}

func (m *mockKinesis) GetShardIterator(ctx context.Context, input *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.GetShardIteratorOutput), args.Error(1)
}

func (m *mockKinesis) ListShards(ctx context.Context, input *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.ListShardsOutput), args.Error(1)
}

func (m *mockKinesis) PutRecord(ctx context.Context, input *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.PutRecordOutput), args.Error(1)
}

func (m *mockKinesis) PutRecords(ctx context.Context, input *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	args := m.Called(ctx, input, optFns)
	return args.Get(0).(*kinesis.PutRecordsOutput), args.Error(1)
}

type mockKVS struct {
	mock.Mock
}

func (m *mockKVS) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*clientv3.GetResponse), args.Error(1)
}

func (m *mockKVS) Put(ctx context.Context, key, value string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	args := m.Called(ctx, key, value, opts)
	return args.Get(0).(*clientv3.PutResponse), args.Error(1)
}

func (m *mockKVS) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	args := m.Called(ctx, key, opts)
	return args.Get(0).(*clientv3.DeleteResponse), args.Error(1)
}

func (m *mockKVS) Txn(ctx context.Context) clientv3.Txn {
	args := m.Called(ctx)
	return args.Get(0).(clientv3.Txn)
}
