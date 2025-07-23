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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/buddhike/pebble/messages"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type shardProcessor struct {
	Assignment *messages.Assignment
	Done       chan struct{}
	Stop       chan struct{}
}

type WorkerService struct {
	cfg               *ConsumerConfig
	kds               aws.Kinesis
	done              chan struct{}
	stop              chan struct{}
	managerUrls       []string
	managerIndex      int
	logger            *zap.Logger
	maxShards         int
	mut               *sync.Mutex
	lastHeartbeatTime time.Time
	shardProcessors   map[string]*shardProcessor
}

func NewWorker(cfg *ConsumerConfig, kds aws.Kinesis, stop chan struct{}, logger *zap.Logger) *WorkerService {
	id := fmt.Sprintf("%s-%s", cfg.Name, uuid.NewString())
	return &WorkerService{
		cfg:             cfg,
		kds:             kds,
		done:            make(chan struct{}),
		stop:            stop,
		managerUrls:     strings.Split(cfg.PopUrls, ","),
		logger:          logger.Named(fmt.Sprintf("Worker-%s", id)),
		maxShards:       1,
		mut:             &sync.Mutex{},
		shardProcessors: make(map[string]*shardProcessor),
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
	w.maxShards = 1
}

func (w *WorkerService) Start() {
	go func() {
		defer close(w.done)

		for {
			select {
			case <-w.stop:
				w.logger.Info("worker is stopped")
				return
			default:
				// Create assign request
				assignReq := messages.AssignRequest{
					WorkerID:  w.cfg.ID,
					MaxShards: w.maxShards,
				}

				// Marshal request to JSON
				reqBody, err := json.Marshal(assignReq)
				if err != nil {
					w.logger.Error("failed to marshal assign request", zap.Error(err))
					select {
					case <-w.stop:
						w.logger.Info("worker is stopped while awiting retry assign request")
						return
					case <-time.After(w.cfg.HealthcheckTimeout()):
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
					case <-time.After(w.cfg.HealthcheckTimeout()):
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
					case <-time.After(w.cfg.HealthcheckTimeout()):
						continue
					}
				}

				// Unmarshal response
				var assignResp messages.AssignResponse
				err = json.Unmarshal(respBody, &assignResp)
				if err != nil {
					w.logger.Info("failed to unmarshal assign response", zap.Error(err), zap.String("assign_response", string(respBody)))
					select {
					case <-w.stop:
						w.logger.Info("shutting down worker")
						return
					case <-time.After(w.cfg.HealthcheckTimeout()):
						continue
					}
				}

				if assignResp.Status.NotInService {
					w.rotateManager()
				}

				// Check if we got any assignments
				if len(assignResp.Assignments) > 0 {
					lt := make(map[string]*messages.Assignment)
					start := make([]*shardProcessor, 0)
					stop := make([]*shardProcessor, 0)
					// Evaluate what we need to start
					for _, a := range assignResp.Assignments {
						lt[a.ShardID] = &a
						p := w.shardProcessors[a.ShardID]
						if p == nil {
							start = append(start, &shardProcessor{Assignment: &a, Done: make(chan struct{}), Stop: make(chan struct{})})
						}
						// We check if a shard processor already exists for the assignment (p != nil)
						// and if the assignment id has changed (p.Assignment.ID != a.ID).
						// this means the assignment for the shard has changed (e.g., due to reassignment or failover),
						// so we need to stop the old processor and start a new one with the updated assignment.
						// this ensures that the worker is always processing the correct assignment for each shard,
						// and prevents processing with stale or incorrect assignment information.
						if p != nil && p.Assignment.ID != a.ID {
							stop = append(stop, p)
							start = append(start, &shardProcessor{Assignment: &a, Done: make(chan struct{}), Stop: make(chan struct{})})
						}
					}
					// Evaluate which shard processors to stop
					for k, v := range w.shardProcessors {
						if lt[k] == nil {
							stop = append(stop, v)
						}
					}
					// Keep track of the new processors
					for _, s := range start {
						w.shardProcessors[s.Assignment.ShardID] = s
					}
					// Make liveness visibile to go routines processing shards
					w.mut.Lock()
					w.lastHeartbeatTime = time.Now()
					w.mut.Unlock()
					// Start new processors
					for _, p := range start {
						w.logger.Info("processing shard", zap.String("shard id", p.Assignment.ShardID))
						go w.processShard(p)
					}
					// Stop processors no longer owned in a non-blocking manner
					for _, s := range stop {
						w.logger.Info("releasing shard", zap.String("shard id", s.Assignment.ShardID))
						delete(w.shardProcessors, s.Assignment.ShardID)
						go func() {
							close(s.Stop)
							<-s.Done
						}()
					}
					if len(start) > 0 {
						w.maxShards = w.maxShards * 2
						// reset if we wrap around int bounds
						if w.maxShards < 0 {
							w.maxShards = 1
						}
					}
				}

				// Wait before next request with stop signal handling
				select {
				case <-w.stop:
					w.logger.Info("shutting down worker")
					for _, p := range w.shardProcessors {
						close(p.Stop)
					}
					for _, p := range w.shardProcessors {
						<-p.Done
					}
					return
				case <-time.After(w.cfg.HealthcheckTimeout()):
					// Continue to next iteration
				}
			}
		}
	}()
}

func (w *WorkerService) processShard(p *shardProcessor) {
	defer close(p.Done)
	sequenceNumber := ""
	okToSubscribe := true
	assignment := p.Assignment

	for okToSubscribe {
		// Determine starting position for subscription
		var startingPosition types.StartingPosition
		if sequenceNumber != "" {
			startingPosition = types.StartingPosition{
				Type:           types.ShardIteratorTypeAtSequenceNumber,
				SequenceNumber: &sequenceNumber,
			}
		} else if assignment.SequenceNumber == "LATEST" {
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
		select {
		case <-p.Stop:
			return
		default:
			w.mut.Lock()
			timedout := time.Since(w.lastHeartbeatTime) >= w.cfg.WorkerInactivityTimeout()
			w.mut.Unlock()
			if timedout {
				return
			}
			output, err := w.kds.SubscribeToShard(context.Background(), subscribeInput)
			if err != nil {
				w.logger.Info("failed to subscribe to shard", zap.String("shard-id", assignment.ShardID), zap.Error(err))
				continue
			}
			// Process events from the subscription with stop signal handling
			okToSubscribe, sequenceNumber = w.handleSubscription(output, p)
		}
	}
}

func (w *WorkerService) handleSubscription(output *kinesis.SubscribeToShardOutput, p *shardProcessor) (bool, string) {
	assignment := p.Assignment
	eventStream := output.GetStream().Events()
	sn := ""
	for {
		select {
		case event, ok := <-eventStream:
			if !ok {
				log.Printf("Event stream closed for shard %s", assignment.ShardID)
				return true, sn // ok to resubscribe
			}

			if event == nil {
				log.Printf("Error in shard subscription %s", assignment.ShardID)
				return false, ""
			}

			evt := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
			if evt == nil {
				log.Printf("Unexpected event type")
				return false, ""
			}

			// Process records
			for _, record := range evt.Value.Records {
				w.cfg.ProcessFn(record)
			}

			// Checkpoint after processing records
			if len(evt.Value.Records) > 0 {
				lastRecord := evt.Value.Records[len(evt.Value.Records)-1]
				checkpointReq := messages.CheckpointRequest{
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

				var checkpointResp messages.CheckpointResponse
				err = json.Unmarshal(respBody, &checkpointResp)
				if err != nil {
					w.logger.Error("failed to unmarshal checkpoint response: %v", zap.Error(err))
					continue
				}

				if checkpointResp.OwnershipChanged {
					w.logger.Info("ownership changed", zap.String("ShardID", assignment.ShardID))
					eventStream = nil
					break
				}

				if checkpointResp.NotInService {
					w.logger.Info("manager not in service, stopping processing of shard", zap.String("ShardID", assignment.ShardID))
					return false, sn
				}

				sn = *lastRecord.SequenceNumber
				w.logger.Info("successfully checkpointed shard", zap.String("shard-id", assignment.ShardID), zap.String("SequenceNumber", *lastRecord.SequenceNumber))
			}
		case <-p.Stop:
			w.logger.Info("received stop signal, stopping processing of shard", zap.String("ShardID", assignment.ShardID))
			return false, ""
		case <-w.stop:
			w.logger.Info("received stop signal, stopping processing of shard", zap.String("ShardID", assignment.ShardID))
			return false, ""
		}
	}
}
