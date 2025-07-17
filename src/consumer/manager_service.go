package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/buddhike/pebble/primitives"
	"go.uber.org/zap"
)

// ManagerService implements the logic for assigning shards to workers.
// Work assignment protocol
// Workers invoke assign method periodically
// Manager maintains a list of unassigned shards and assigns one or more shards
// to the worker
// Manager keeps track of the number of shards assigned to the worker during
// its last assignment
//
// Number of shards a manager assigns to a given worker grows exponentially by
// assigning twice as many as shards than what's assigned during the last iteration
// Workers add a smaller jitter to the frequency of Assign calls to randomise the
// concurrent calls made by multiple workers
// Jitter should be configurable and the default value should be 100ms
// For example, if the frequency of Assign heartbeat is set to 100ms and jitter is also
// 100ms, Assign call should happen sometime between 100-200ms since the last call
//
// When there are no available shards to assign, manager uses assign invocation as an opportutnity
// to ensure that shards are assigned evenly across all available workers
// It first calculates what's the distribution should be and works out the ideal number of shards
// that should have been assigned to the worker invoking assign call
// It then performs the stealing logic as described below
//
// manager first checks to see if all workers are heartbeating as expected via assign call
// If there are shards assigned to inactive workers, they are reassigned first
// When there are no available shards to assign, manager attempts to balance the
// distribution by stealing shards from existing workers
//
// Shard stealing is two phase. In phase 1, manager marks some shards for releasing.
// Next time when the worker attempts to checkpoint, manager records the checkpoint
// then notify the worker that it no logner owns the shard
// The shard is then added to the list of unassigned shards
// Each shard that is stolen maintains the original worker id and excludes it
// from getting re-assigned to the same worker
// Each stolen shard also has a TTL associated. If they are not assigned before
// TTL is expired, they become available for assignement for any worker
//
// Internal State Management Data Structures
// WorkerHeartBeats
// Heap storing oldest worker heartbeat in the root
// Used to find out the worker with oldest heartbeat
//
// WorkerLoad
// Priority queue where the worker with most number of shards assigned is on top
// Used to find out the worker with most number of shards assigned
// They are prioritised when stealing work
//
// StolenShards
// Heap storing all shards stolen for load shedding. Oldest shard appears on top
//

type ManagerService struct {
	mut                *sync.Mutex
	inService          bool
	kds                aws.Kinesis
	kvs                KVS
	cfg                *ConsumerConfig
	shards             []types.Shard
	unassignedShards   []*types.Shard
	checkpoints        map[string]string
	done               chan struct{}
	stop               chan struct{}
	workers            map[string]*workerData
	workerHeartbeats   *primitives.PriorityQueue[string]
	workerShardCount   *primitives.PriorityQueue[string]
	healthcheckTimeout time.Duration
	logger             *zap.Logger
}

type Status struct {
	NotInService bool
}

type workerData struct {
	workerID       string
	lastHeartbeat  time.Time
	assignedShards map[string]*types.Shard
}

type Assignment struct {
	ShardID        string
	SequenceNumber string
}

type CheckpointRequest struct {
	WorkerID       string
	ShardID        string
	SequenceNumber string
}

type CheckpointResponse struct {
	Status
	OwnershipChanged bool
}

type AssignRequest struct {
	WorkerID  string
	MaxShards int
}

type AssignResponse struct {
	Status
	Assignments []Assignment
}

func NewManagerService(cfg *ConsumerConfig, kds aws.Kinesis, kvs KVS, stop chan struct{}, logger *zap.Logger) *ManagerService {
	return &ManagerService{
		cfg:                cfg,
		mut:                &sync.Mutex{},
		kds:                kds,
		kvs:                kvs,
		done:               make(chan struct{}),
		stop:               stop,
		workers:            make(map[string]*workerData),
		workerHeartbeats:   primitives.NewPriorityQueue[string](false),
		workerShardCount:   primitives.NewPriorityQueue[string](true),
		healthcheckTimeout: time.Second * time.Duration(cfg.HealthcheckTimeoutSeconds),
		logger:             logger.Named("managerservice").With(zap.Int("managerid", cfg.ManagerID)),
	}
}

func (m *ManagerService) Start() error {
	url, err := m.cfg.GetManagerListenUrl()
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%s", url.Hostname(), url.Port()),
		Handler: http.DefaultServeMux,
	}

	go func() {
		http.HandleFunc("/checkpoint/", m.Checkpoint)
		http.HandleFunc("/assign/", m.Assign)
		http.HandleFunc("/status/", m.Status)
		http.HandleFunc("/health/", m.Health)
		server.ListenAndServe()
		close(m.done)
	}()

	go func() {
		<-m.stop
		server.Close()
	}()

	return nil
}

func (m *ManagerService) Checkpoint(w http.ResponseWriter, r *http.Request) {
	m.mut.Lock()
	defer m.mut.Unlock()
	defer r.Body.Close()

	if !m.ensureInService(w) {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.logger.Error("error reading checkpoint request body", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request CheckpointRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		m.logger.Error("error unmashaling checkpoint request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	worker := m.workers[request.WorkerID]
	if worker == nil || worker.assignedShards[request.ShardID] == nil {
		// Worker doesn't exist, ownership changed
		response := CheckpointResponse{
			OwnershipChanged: true,
		}
		res, _ := json.Marshal(response)
		w.WriteHeader(http.StatusOK)
		w.Write(res)
		return
	}

	// Store checkpoint in KVS
	_, err = m.kvs.Put(context.Background(), request.ShardID, request.SequenceNumber)
	if err != nil {
		m.logger.Error("error putting checkpoint in etcd", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Worker owns the shard, update checkpoint
	m.checkpoints[request.ShardID] = request.SequenceNumber

	response := CheckpointResponse{
		Status:           Status{NotInService: false},
		OwnershipChanged: false,
	}
	res, err := json.Marshal(response)
	if err != nil {
		m.logger.Error("error marshaling checkpoint response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (m *ManagerService) Assign(w http.ResponseWriter, r *http.Request) {
	m.mut.Lock()
	defer m.mut.Unlock()
	defer r.Body.Close()

	if !m.ensureInService(w) {
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.logger.Error("error reading assign request body", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request AssignRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		m.logger.Error("error unmarshaling assign request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	worker := m.workers[request.WorkerID]
	if worker == nil {
		worker = &workerData{
			workerID:       request.WorkerID,
			assignedShards: make(map[string]*types.Shard),
		}
		m.workers[request.WorkerID] = worker
	}
	worker.lastHeartbeat = time.Now()
	m.workerHeartbeats.Push(request.WorkerID, float64(worker.lastHeartbeat.UnixMilli()))

	if request.MaxShards == 0 {
		response := AssignResponse{}

		res, err := json.Marshal(response)
		if err != nil {
			m.logger.Error("error marshaling assign response", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(res)
		return
	}

	// Ensure that shards assigned to inactive workers are made available
	// for assignment
	for {
		leastActiveWorkerID, _ := m.workerHeartbeats.Peek()
		leastActiveWorker := m.workers[leastActiveWorkerID]
		if time.Since(leastActiveWorker.lastHeartbeat) < time.Second*5 {
			break
		}
		m.workerHeartbeats.Pop()
		for _, a := range m.workers[leastActiveWorkerID].assignedShards {
			m.unassignedShards = append(m.unassignedShards, a)
		}
		leastActiveWorker.assignedShards = make(map[string]*types.Shard)
	}

	var assignments []Assignment
	if len(m.unassignedShards) >= request.MaxShards {
		count := min(request.MaxShards, len(m.unassignedShards))
		for _, s := range m.unassignedShards[0:count] {
			worker.assignedShards[*s.ShardId] = s
			sn := m.checkpoints[*s.ShardId]
			assignments = append(assignments, Assignment{ShardID: *s.ShardId, SequenceNumber: sn})
		}
		m.unassignedShards = m.unassignedShards[count:]
	}

	m.workerShardCount.Push(request.WorkerID, float64(len(worker.assignedShards)))

	response := AssignResponse{
		Assignments: assignments,
	}

	res, err := json.Marshal(response)
	if err != nil {
		m.logger.Error("error marshaling assign response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (m *ManagerService) Status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if m.inService {
		w.Write([]byte("leader"))
	} else {
		w.Write([]byte("follower"))
	}
}

func (m *ManagerService) ensureInService(w http.ResponseWriter) bool {
	if !m.inService {
		r, _ := json.Marshal(CheckpointResponse{Status: Status{NotInService: true}})
		w.WriteHeader(http.StatusOK)
		w.Write(r)
	}
	return m.inService
}

func (m *ManagerService) SetInService(inService bool) {
	if !inService {
		m.mut.Lock()
		m.inService = inService
		m.mut.Unlock()
		return
	}

	// Prepare everything we need before we acquire the lock to update inService
	// status. This will prevent other API calls trying to acquire the same lock
	// getting blocked until shards are discovered.
	// We aim to not perform any long running IO operations while holding the
	// global state mut.
	input := &kinesis.ListShardsInput{
		StreamName: &m.cfg.StreamName,
	}
	// AWS SDK client has built-in retry logic
	out, err := m.kds.ListShards(context.Background(), input)
	if err != nil {
		m.logger.Error("failed to list shards", zap.Error(err))
		// TODO: Instead of panicing here, return the error to caller
		// Caller can release leadership
		panic(err)
	}
	m.shards = out.Shards
	m.workers = make(map[string]*workerData)
	m.checkpoints = make(map[string]string)
	m.workerHeartbeats = primitives.NewPriorityQueue[string](false)
	m.workerShardCount = primitives.NewPriorityQueue[string](true)

	for _, s := range m.shards {
		m.unassignedShards = append(m.unassignedShards, &s)
	}
	for _, s := range m.unassignedShards {
		// ETCD client v3 has built-in retry logic
		cp, err := m.kvs.Get(context.Background(), *s.ShardId)
		if err != nil {
			m.logger.Error("failed to get checkpoint from kvs", zap.Error(err))
			panic(err)
		}
		if cp.Count > 0 {
			m.checkpoints[*s.ShardId] = string(cp.Kvs[0].Value)
		} else {
			m.checkpoints[*s.ShardId] = "LATEST"
		}
	}

	m.mut.Lock()
	m.inService = true
	m.mut.Unlock()
}

func (m *ManagerService) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), m.healthcheckTimeout)
	_, err := m.kvs.Get(ctx, "/healthcheck")
	cancel()
	if err != nil {
		m.logger.Error("etcdserver healthcheck failed", zap.Error(err))
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("unhealthy-%s-%d: %v", m.cfg.Name, m.cfg.ManagerID, err)))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("healthy-%s-%d", m.cfg.Name, m.cfg.ManagerID)))
}
