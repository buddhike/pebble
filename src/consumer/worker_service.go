package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type WorkerService struct {
	cfg          *ConsumerConfig
	kds          aws.Kinesis
	done         chan struct{}
	stop         chan struct{}
	managerUrls  []string
	managerIndex int
	logger       *zap.Logger
}

func NewWorker(cfg *ConsumerConfig, kds aws.Kinesis, stop chan struct{}, logger *zap.Logger) *WorkerService {
	id := fmt.Sprintf("%s-%s", cfg.Name, uuid.NewString())
	return &WorkerService{
		cfg:         cfg,
		kds:         kds,
		done:        make(chan struct{}),
		stop:        stop,
		managerUrls: strings.Split(cfg.ManagerUrls, ","),
		logger:      logger.Named(fmt.Sprintf("Worker-%s", id)),
	}
}

func (w *WorkerService) currentManager() string {
	return w.managerUrls[w.managerIndex]
}

func (w *WorkerService) rotateManager() {
	w.managerIndex++
	if w.managerIndex == len(w.managerUrls) {
		w.managerIndex = 0
	}
}

func (w *WorkerService) Start() {
	go func() {
		defer close(w.done)

		processors := make([]chan struct{}, 0)
		for {
			select {
			case <-w.stop:
				w.logger.Info("worker is stopped")
				return
			default:
				// Create assign request
				assignReq := AssignRequest{
					WorkerID: w.cfg.ID,
				}

				// Marshal request to JSON
				reqBody, err := json.Marshal(assignReq)
				if err != nil {
					w.logger.Error("failed to marshal assign request", zap.Error(err))
					select {
					case <-w.stop:
						w.logger.Info("worker is stopped while awiting retry assign request")
						return
					case <-time.After(5 * time.Second):
						continue
					}
				}

				// Make HTTP request to manager
				resp, err := http.Post(fmt.Sprintf("%s/assign/", w.currentManager()), "application/json", bytes.NewBuffer(reqBody))
				if err != nil {
					w.logger.Error("failed to make assign request", zap.Error(err))
					w.rotateManager()
					select {
					case <-w.stop:
						w.logger.Info("shutting down worker")
						return
					case <-time.After(5 * time.Second):
						continue
					}
				}

				// Read response body
				respBody, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					w.logger.Error("failed to read response body", zap.Error(err))
					select {
					case <-w.stop:
						w.logger.Info("shutting down worker")
						return
					case <-time.After(5 * time.Second):
						continue
					}
				}

				// Unmarshal response
				var assignResp AssignResponse
				err = json.Unmarshal(respBody, &assignResp)
				if err != nil {
					w.logger.Info("failed to unmarshal assign response", zap.Error(err), zap.String("assign_response", string(respBody)))
					select {
					case <-w.stop:
						w.logger.Info("shutting down worker")
						return
					case <-time.After(5 * time.Second):
						continue
					}
				}

				if assignResp.Status.NotInService {
					w.rotateManager()
				}

				// Check if we got any assignments
				if len(assignResp.Assignments) > 0 {
					w.logger.Info("shutting down worker")
					w.logger.Info("shards assigned", zap.Int("count", len(assignResp.Assignments)))
					for _, assignment := range assignResp.Assignments {
						done := make(chan struct{})
						go w.processShard(assignment, done)
					}
				} else {
					w.logger.Info("no shards assigned")
				}

				// Wait before next request with stop signal handling
				select {
				case <-w.stop:
					w.logger.Info("shutting down worker")
					for _, p := range processors {
						<-p
					}
					return
				case <-time.After(5 * time.Second):
					// Continue to next iteration
				}
			}
		}
	}()
}

func (w *WorkerService) processShard(assignment Assignment, done chan struct{}) {
	defer close(done)
	// Determine starting position for subscription
	var startingPosition types.StartingPosition
	if assignment.SequenceNumber == "LATEST" {
		startingPosition = types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		}
	} else {
		startingPosition = types.StartingPosition{
			Type:           types.ShardIteratorTypeAtSequenceNumber,
			SequenceNumber: &assignment.SequenceNumber,
		}
	}

	// Create subscription input
	subscribeInput := &kinesis.SubscribeToShardInput{
		ConsumerARN:      &w.cfg.EfoConsumerArn,
		ShardId:          &assignment.ShardID,
		StartingPosition: &startingPosition,
	}

	// Subscribe to shard
	output, err := w.kds.SubscribeToShard(context.Background(), subscribeInput)
	if err != nil {
		w.logger.Info("failed to subscribe to shard", zap.String("shard-id", assignment.ShardID), zap.Error(err))
		return
	}

	// Process events from the subscription with stop signal handling
	eventStream := output.GetStream().Events()
	for {
		select {
		case event, ok := <-eventStream:
			if !ok {
				log.Printf("Event stream closed for shard %s", assignment.ShardID)
				return
			}

			if event == nil {
				log.Printf("Error in shard subscription %s", assignment.ShardID)
				return
			}

			evt := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
			if evt == nil {
				log.Printf("Unexpected event type")
				return
			}

			// Process records
			for _, record := range evt.Value.Records {
				w.cfg.ProcessFn(record)
			}

			// Checkpoint after processing records
			if len(evt.Value.Records) > 0 {
				lastRecord := evt.Value.Records[len(evt.Value.Records)-1]
				checkpointReq := CheckpointRequest{
					WorkerID:       w.cfg.ID,
					ShardID:        assignment.ShardID,
					SequenceNumber: *lastRecord.SequenceNumber,
				}

				reqBody, err := json.Marshal(checkpointReq)
				if err != nil {
					w.logger.Error("failed to marshal checkpoint request", zap.Error(err))
					continue
				}

				resp, err := http.Post(fmt.Sprintf("%s/checkpoint/", w.currentManager()), "application/json", bytes.NewBuffer(reqBody))
				if err != nil {
					w.logger.Error("failed to send checkpoint request", zap.Error(err))
					continue
				}

				respBody, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					w.logger.Error("failed to read checkpoint response: %v", zap.Error(err))
					continue
				}

				var checkpointResp CheckpointResponse
				err = json.Unmarshal(respBody, &checkpointResp)
				if err != nil {
					w.logger.Error("failed to unmarshal checkpoint response: %v", zap.Error(err))
					continue
				}

				if checkpointResp.OwnershipChanged {
					w.logger.Info("ownership changed", zap.String("ShardID", assignment.ShardID))
					return
				}

				if checkpointResp.NotInService {
					w.logger.Info("manager not in service, stopping processing of shard", zap.String("ShardID", assignment.ShardID))
					return
				}

				w.logger.Info("successfully checkpointed shard", zap.String("shard-id", assignment.ShardID), zap.String("SequenceNumber", *lastRecord.SequenceNumber))
			}

		case <-w.stop:
			w.logger.Info("received stop signal, stopping processing of shard", zap.String("ShardID", assignment.ShardID))
			return
		}
	}

}
