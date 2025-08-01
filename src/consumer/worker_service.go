package consumer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/buddhike/pebble/aws"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

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
	shardProcessors   map[string]*ShardProcessor
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
		shardProcessors: make(map[string]*ShardProcessor),
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
					p.Stop()
				}
				for _, p := range w.shardProcessors {
					p.Done()
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
		start := make([]*ShardProcessor, 0)
		stop := make([]*ShardProcessor, 0)
		restart := make([]*ShardProcessor, 0)
		// Evaluate what we need to start
		for _, a := range assignResp.Assignments {
			lt[a.ShardID] = &a
			p := w.shardProcessors[a.ShardID]
			if p == nil {
				start = append(start, NewShardProcessor(w.cfg, w.currentManager(), w.logger, w.kds, &a))
			}
			// We check if a shard processor already exists for the assignment (p != nil)
			// and if the assignment id has changed (p.Assignment.ID < a.ID).
			// this means the assignment for the shard has changed (e.g., due to reassignment or failover),
			// so we need to stop the old processor and start a new one with the updated assignment.
			// this ensures that the worker is always processing the correct assignment for each shard,
			// and prevents processing with stale or incorrect assignment information.
			if p != nil && p.assignment.ID < a.ID {
				restart = append(restart, NewShardProcessor(w.cfg, w.currentManager(), w.logger, w.kds, &a))
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
			w.shardProcessors[p.assignment.ShardID] = p
			w.logger.Info("starting new shard processor", zap.String("shard-id", p.assignment.ShardID), zap.Int64("assignment-id", p.assignment.ID))
			go p.Start()
		}
		// Restart processors
		for _, p := range restart {
			current := w.shardProcessors[p.assignment.ShardID]
			w.shardProcessors[p.assignment.ShardID] = p
			go func() {
				w.logger.Info("restarting shard processor", zap.String("shard-id", p.assignment.ShardID), zap.Int64("old-assignment-id", current.assignment.ID), zap.Int64("new-assignment-id", p.assignment.ID))
				current.Stop()
				current.Done()
				p.Start()
			}()
		}
		// Stop processors no longer owned
		for _, s := range stop {
			w.logger.Info("stopping shard processor", zap.String("shard id", s.assignment.ShardID), zap.Int64("assignment-id", s.assignment.ID))
			delete(w.shardProcessors, s.assignment.ShardID)
			go func() {
				s.Stop()
				s.Done()
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
