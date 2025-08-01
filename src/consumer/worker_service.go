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
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type shardProcessor struct {
	Assignment *Assignment
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
	clock             func() time.Time
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
		clock:           func() time.Time { return time.Now() },
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

		err := w.assignOnce()
		if err != nil {
			w.logger.Error("error invoking assign", zap.Error(err), zap.String("current-manager", w.currentManager()))
			w.rotateManager()
		}

		for {
			select {
			case <-w.stop:
				w.logger.Info("worker is stopped")
				for _, p := range w.shardProcessors {
					close(p.Stop)
				}
				for _, p := range w.shardProcessors {
					<-p.Done
				}
				return
			case <-time.After(w.cfg.HealthcheckTimeout()):
				err := w.assignOnce()
				if err != nil {
					w.logger.Error("error invoking assign", zap.Error(err), zap.String("current-manager", w.currentManager()))
					w.rotateManager()
				}
			}
		}
	}()
}

func (w *WorkerService) assignOnce() error {
	// Create assign request
	assignReq := AssignRequest{
		WorkerID:  w.cfg.ID,
		MaxShards: w.maxShards,
	}

	// Marshal request to JSON
	reqBody, err := json.Marshal(assignReq)
	if err != nil {
		panic(err)
	}

	// Make HTTP request to manager
	resp, err := http.Post(fmt.Sprintf("%s/assign/", w.currentManager()), "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed send assign request: %w", err)
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		panic(fmt.Errorf("unexpected http response status for assign request: %d", resp.StatusCode))
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected http response code for assign request: %d", resp.StatusCode)
	}

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read assign response body: %w", err)
	}

	// Unmarshal response
	var assignResp AssignResponse
	err = json.Unmarshal(respBody, &assignResp)
	if err != nil {
		w.logger.Info("failed to unmarshal assign response", zap.Error(err), zap.String("assign_response", string(respBody)))
		return fmt.Errorf("failed to unmarshal assign response: %w", err)
	}

	if assignResp.Status.NotInService {
		w.rotateManager()
		return nil
	}

	if len(assignResp.Assignments) > 0 {
		lt := make(map[string]*Assignment)
		start := make([]*shardProcessor, 0)
		stop := make([]*shardProcessor, 0)
		restart := make([]*shardProcessor, 0)
		// Evaluate what we need to start
		for _, a := range assignResp.Assignments {
			lt[a.ShardID] = &a
			p := w.shardProcessors[a.ShardID]
			if p == nil {
				start = append(start, &shardProcessor{Assignment: &a, Done: make(chan struct{}), Stop: make(chan struct{})})
			}
			// We check if a shard processor already exists for the assignment (p != nil)
			// and if the assignment id has changed (p.Assignment.ID < a.ID).
			// this means the assignment for the shard has changed (e.g., due to reassignment or failover),
			// so we need to stop the old processor and start a new one with the updated assignment.
			// this ensures that the worker is always processing the correct assignment for each shard,
			// and prevents processing with stale or incorrect assignment information.
			if p != nil && p.Assignment.ID < a.ID {
				restart = append(restart, &shardProcessor{Assignment: &a, Done: make(chan struct{}), Stop: make(chan struct{})})
			}
		}
		// Evaluate which shard processors to stop
		for k, v := range w.shardProcessors {
			if lt[k] == nil {
				stop = append(stop, v)
			}
		}
		// Make liveness visibile to go routines processing shards
		w.mut.Lock()
		w.lastHeartbeatTime = time.Now()
		w.mut.Unlock()
		// Start new processors
		for _, p := range start {
			w.shardProcessors[p.Assignment.ShardID] = p
			w.logger.Info("starting new shard processor", zap.String("shard-id", p.Assignment.ShardID), zap.Int64("assignment-id", p.Assignment.ID))
			go w.processShard(p)
		}
		// Restart processors
		for _, p := range restart {
			current := w.shardProcessors[p.Assignment.ShardID]
			w.shardProcessors[p.Assignment.ShardID] = p
			go func() {
				w.logger.Info("restarting shard processor", zap.String("shard-id", p.Assignment.ShardID), zap.Int64("old-assignment-id", p.Assignment.ID), zap.Int64("new-assignment-id", p.Assignment.ID))
				close(current.Stop)
				<-current.Done
				w.processShard(p)
			}()
		}
		// Stop processors no longer owned
		for _, s := range stop {
			w.logger.Info("stopping shard processor", zap.String("shard id", s.Assignment.ShardID), zap.Int64("assignment-id", s.Assignment.ID))
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
	return nil
}

func (w *WorkerService) processShard(p *shardProcessor) {
	defer close(p.Done)
	sequenceNumber := ""
	okToSubscribe := true
	assignment := p.Assignment

	if assignment.SequenceNumber == "" {
		w.logger.Fatal("assignment with an empty sequence number", zap.String("shard-id", assignment.ShardID))
	}

	for okToSubscribe {
		if sequenceNumber == "CLOSED" {
			w.logger.Fatal("worker must not continue or assigned after shard is closed", zap.String("shard-id", assignment.ShardID))
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
				w.logger.Info("failed to subscribe to shard", zap.String("shard-id", assignment.ShardID), zap.String("sequence-number", *startingPosition.SequenceNumber), zap.Error(err))
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
	lastCheckpointTime := w.clock()
	for {
		select {
		case event, ok := <-eventStream:

			if !ok {
				log.Printf("event stream closed for shard %s", assignment.ShardID)
				return true, sn // ok to resubscribe
			}

			if event == nil {
				log.Printf("error in shard subscription %s", assignment.ShardID)
				return false, ""
			}

			evt := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
			if evt == nil {
				log.Printf("unexpected event type")
				return false, ""
			}

			now := w.clock()

			// Process records
			for _, record := range evt.Value.Records {
				w.cfg.ProcessFn(record)
			}

			// Checkpoint after processing records
			if evt.Value.ContinuationSequenceNumber != nil {
				sn = *evt.Value.ContinuationSequenceNumber
			} else {
				sn = "CLOSED"
				w.logger.Info("shard closed", zap.String("shard-id", assignment.ShardID))
			}

			if sn != "CLOSED" && now.Sub(lastCheckpointTime) < w.cfg.CheckpointInterval() {
				continue
			}
			checkpointReq := CheckpointRequest{
				AssignmentID:   assignment.ID,
				WorkerID:       w.cfg.ID,
				ShardID:        assignment.ShardID,
				SequenceNumber: sn,
			}

			reqBody, err := json.Marshal(checkpointReq)
			if err != nil {
				panic(err)
			}

			resp, err := http.Post(fmt.Sprintf("%s/checkpoint/", w.currentManager()), "application/json", bytes.NewBuffer(reqBody))
			if err != nil {
				w.logger.Error("failed to send checkpoint request", zap.Error(err))
				continue
			}

			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				panic(fmt.Errorf("unexpected http response status for checkpoint request: %d", resp.StatusCode))
			}

			if resp.StatusCode != 200 {
				w.logger.Info("unexpected http response status for checkpoint request", zap.String("shard-id", assignment.ShardID), zap.Int64("assignment-id", assignment.ID), zap.Int("status-code", resp.StatusCode))
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
				panic(fmt.Errorf("failed to unmarshal checkpoint response: %w", err))
			}

			if checkpointResp.OwnershipChanged {
				w.logger.Info("ownership changed", zap.String("shard-id", assignment.ShardID), zap.Int64("assignment-id", assignment.ID))
				eventStream = nil
				break
			}

			if sn == "CLOSED" {
				w.logger.Info("shard closed", zap.String("shard-id", assignment.ShardID), zap.Int64("assignment-id", assignment.ID))
				eventStream = nil
				break
			}

			if checkpointResp.NotInService {
				w.logger.Info("manager not in service, stopping processing of shard", zap.String("ShardID", assignment.ShardID), zap.Int64("assignment-id", assignment.ID))
				return false, sn
			}

			lastCheckpointTime = now
			w.logger.Info("checkpoint completed", zap.String("shard-id", assignment.ShardID), zap.String("SequenceNumber", sn), zap.Int64("assignment-id", assignment.ID))
		case <-p.Stop:
			w.logger.Info("stopped shard processor", zap.String("ShardID", assignment.ShardID), zap.Int64("assignment-id", assignment.ID))
			return false, ""
		}
	}
}
