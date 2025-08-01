package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"go.uber.org/zap"
)

type ShardProcessor struct {
	cfg        *ConsumerConfig
	assignment *Assignment
	done       chan struct{}
	stop       chan struct{}
	managerUrl string
	logger     *zap.Logger
	kds        aws.Kinesis
	clock      func() time.Time
	timer      func(time.Duration) <-chan time.Time
}

func NewShardProcessor(cfg *ConsumerConfig, managerUrl string, logger *zap.Logger, kds aws.Kinesis, assignment *Assignment) *ShardProcessor {
	return &ShardProcessor{
		cfg:        cfg,
		assignment: assignment,
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		managerUrl: managerUrl,
		logger:     logger.With(zap.String("shard-id", assignment.ShardID), zap.Int64("assignment-id", assignment.ID)),
		kds:        kds,
		clock:      time.Now,
		timer:      time.After,
	}
}

func (p *ShardProcessor) Start() {
	defer close(p.done)
	sequenceNumber := ""
	okToSubscribe := true
	assignment := p.assignment

	if assignment.SequenceNumber == "" {
		p.logger.Fatal("assignment with an empty sequence number")
	}

	if assignment.SequenceNumber == "CLOSED" {
		p.logger.Fatal("assigned a closed shard")
	}

	for okToSubscribe {
		if sequenceNumber == "CLOSED" {
			p.logger.Fatal("worker must not continue or assigned after shard is closed", zap.String("shard-id", assignment.ShardID))
		}
		// Determine starting position for subscription
		var startingPosition types.StartingPosition
		if sequenceNumber != "" {
			startingPosition = types.StartingPosition{
				Type:           types.ShardIteratorTypeAfterSequenceNumber,
				SequenceNumber: &sequenceNumber,
			}
		} else if assignment.SequenceNumber == "LATEST" {
			startingPosition = types.StartingPosition{
				Type: types.ShardIteratorTypeLatest,
			}
		} else {
			startingPosition = types.StartingPosition{
				Type:           types.ShardIteratorTypeAfterSequenceNumber,
				SequenceNumber: &assignment.SequenceNumber,
			}
		}

		// Create subscription input
		subscribeInput := &kinesis.SubscribeToShardInput{
			ConsumerARN:      &p.cfg.EfoConsumerArn,
			ShardId:          &assignment.ShardID,
			StartingPosition: &startingPosition,
		}

		// Subscribe to shard
		select {
		case <-p.stop:
			return
		default:
			output, err := p.kds.SubscribeToShard(context.Background(), subscribeInput)
			if err != nil {
				p.logger.Info("failed to subscribe to shard", zap.String("sequence-number", sequenceNumber), zap.Error(err))
				continue
			}
			// Process events from the subscription with stop signal handling
			okToSubscribe, sequenceNumber = p.handleSubscription(output)
		}
	}
}

func (p *ShardProcessor) handleSubscription(output *kinesis.SubscribeToShardOutput) (bool, string) {
	assignment := p.assignment
	eventStream := output.GetStream().Events()
	sn := ""
	lastCheckpointTime := p.clock()
	for {
		select {
		case event, ok := <-eventStream:
			if !ok {
				p.logger.Info("event stream closed")
				return true, sn // ok to resubscribe
			}

			if event == nil {
				p.logger.Info("skipping unexpected nil event")
				continue
			}

			evt := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
			if evt == nil {
				p.logger.Info("skipping unexpected event type")
				continue
			}

			// Process records
			for _, record := range evt.Value.Records {
				p.cfg.ProcessFn(record)
			}

			// Checkpoint after processing records
			now := p.clock()
			closed := false
			if evt.Value.ContinuationSequenceNumber != nil {
				sn = *evt.Value.ContinuationSequenceNumber
			} else {
				closed = true
				p.logger.Info("shard closed")
			}

			if !closed && now.Sub(lastCheckpointTime) < p.cfg.CheckpointInterval() {
				continue
			}

			checkpointResp := p.checkpoint(sn, closed)

			var stopReason string
			switch {
			case checkpointResp == nil:
				stopReason = "checkpoint failed"
			case checkpointResp.OwnershipChanged:
				stopReason = "ownership changed"
			case checkpointResp.NotInService:
				stopReason = "not in service"
			case closed:
				stopReason = "shard closed"
			}

			if stopReason != "" {
				p.logger.Info("stop reading event stream", zap.String("reason", stopReason))
				eventStream = nil
				break
			}

			lastCheckpointTime = now
			p.logger.Info("checkpoint completed", zap.String("shard-id", assignment.ShardID), zap.String("SequenceNumber", sn), zap.Int64("assignment-id", assignment.ID))
		case <-p.stop:
			p.logger.Info("stopped shard processor", zap.String("ShardID", assignment.ShardID), zap.Int64("assignment-id", assignment.ID))
			return false, ""
		}
	}
}

func (p *ShardProcessor) checkpoint(sn string, closed bool) *CheckpointResponse {
	response, err := p.checkpointOnce(sn, closed)
	if err == nil {
		return response
	}

	for {
		p.logger.Error("checkpoint failed", zap.Error(err))
		select {
		case <-p.timer(p.cfg.CheckpointRetryInterval()):
			response, err = p.checkpointOnce(sn, closed)
			if err == nil {
				return response
			}
		case <-p.stop:
			return nil
		}
	}
}

func (p *ShardProcessor) checkpointOnce(sn string, closed bool) (*CheckpointResponse, error) {
	if closed {
		sn = "CLOSED"
	}
	checkpointReq := CheckpointRequest{
		AssignmentID:   p.assignment.ID,
		WorkerID:       p.cfg.ID,
		ShardID:        p.assignment.ShardID,
		SequenceNumber: sn,
	}

	reqBody, err := json.Marshal(checkpointReq)
	if err != nil {
		panic(err)
	}

	resp, err := http.Post(fmt.Sprintf("%s/checkpoint/", p.managerUrl), "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send checkpoint request: %w", err)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		p.logger.Fatal("unexpected http response status for checkpoint request", zap.Int("status-code", resp.StatusCode))
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected http response status for checkpoint request: wanted 200, received %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint response: %w", err)
	}

	var checkpointResp CheckpointResponse
	err = json.Unmarshal(respBody, &checkpointResp)
	if err != nil {
		// If we have a 200 response that we cannot unmarshal, fail fast
		p.logger.Fatal("failed to unmarshal checkpoint response", zap.Error(err))
	}
	return &checkpointResp, nil
}

func (p *ShardProcessor) Stop() {
	close(p.stop)
}

func (p *ShardProcessor) Done() {
	<-p.done
}
