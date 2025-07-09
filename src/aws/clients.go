package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type Kinesis interface {
	CreateStream(ctx context.Context, params *kinesis.CreateStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error)
	DeleteStream(ctx context.Context, params *kinesis.DeleteStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error)
	DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
	DescribeStreamSummary(ctx context.Context, params *kinesis.DescribeStreamSummaryInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamSummaryOutput, error)
	ListStreams(ctx context.Context, params *kinesis.ListStreamsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamsOutput, error)
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	ListTagsForStream(ctx context.Context, params *kinesis.ListTagsForStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.ListTagsForStreamOutput, error)
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	SubscribeToShard(ctx context.Context, params *kinesis.SubscribeToShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SubscribeToShardOutput, error)
	AddTagsToStream(ctx context.Context, params *kinesis.AddTagsToStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.AddTagsToStreamOutput, error)
	RemoveTagsFromStream(ctx context.Context, params *kinesis.RemoveTagsFromStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.RemoveTagsFromStreamOutput, error)
	UpdateShardCount(ctx context.Context, params *kinesis.UpdateShardCountInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateShardCountOutput, error)
	UpdateStreamMode(ctx context.Context, params *kinesis.UpdateStreamModeInput, optFns ...func(*kinesis.Options)) (*kinesis.UpdateStreamModeOutput, error)
	EnableEnhancedMonitoring(ctx context.Context, params *kinesis.EnableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.EnableEnhancedMonitoringOutput, error)
	DisableEnhancedMonitoring(ctx context.Context, params *kinesis.DisableEnhancedMonitoringInput, optFns ...func(*kinesis.Options)) (*kinesis.DisableEnhancedMonitoringOutput, error)
	DescribeLimits(ctx context.Context, params *kinesis.DescribeLimitsInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeLimitsOutput, error)
	MergeShards(ctx context.Context, params *kinesis.MergeShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error)
	SplitShard(ctx context.Context, params *kinesis.SplitShardInput, optFns ...func(*kinesis.Options)) (*kinesis.SplitShardOutput, error)
	StartStreamEncryption(ctx context.Context, params *kinesis.StartStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StartStreamEncryptionOutput, error)
	StopStreamEncryption(ctx context.Context, params *kinesis.StopStreamEncryptionInput, optFns ...func(*kinesis.Options)) (*kinesis.StopStreamEncryptionOutput, error)
	DecreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.DecreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.DecreaseStreamRetentionPeriodOutput, error)
	IncreaseStreamRetentionPeriod(ctx context.Context, params *kinesis.IncreaseStreamRetentionPeriodInput, optFns ...func(*kinesis.Options)) (*kinesis.IncreaseStreamRetentionPeriodOutput, error)
	RegisterStreamConsumer(ctx context.Context, params *kinesis.RegisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.RegisterStreamConsumerOutput, error)
	DeregisterStreamConsumer(ctx context.Context, params *kinesis.DeregisterStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DeregisterStreamConsumerOutput, error)
	DescribeStreamConsumer(ctx context.Context, params *kinesis.DescribeStreamConsumerInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamConsumerOutput, error)
	ListStreamConsumers(ctx context.Context, params *kinesis.ListStreamConsumersInput, optFns ...func(*kinesis.Options)) (*kinesis.ListStreamConsumersOutput, error)
}
