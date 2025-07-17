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
	unassignedShards   []types.Shard
	assignmentData     map[string]*assignmentData
	checkpoints        map[string]string
	done               chan struct{}
	stop               chan struct{}
	workerHeartbeats   map[string]time.Time
	healthcheckTimeout time.Duration
	logger             *zap.Logger
}

type Status struct {
	NotInService bool
}

type assignmentData struct {
	shards              map[string]types.Shard
	lastAssignmentCount int
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
	WorkerID string
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
		workerHeartbeats:   make(map[string]time.Time),
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request CheckpointRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Check if worker owns the shard
	ad, exists := m.assignmentData[request.WorkerID]
	if !exists || ad == nil {
		// Worker doesn't exist, ownership changed
		response := CheckpointResponse{
			Status:           Status{NotInService: false},
			OwnershipChanged: true,
		}
		res, _ := json.Marshal(response)
		w.WriteHeader(http.StatusOK)
		w.Write(res)
		return
	}

	// Check if worker owns this specific shard
	_, ownsShard := ad.shards[request.ShardID]
	if !ownsShard {
		// Worker doesn't own this shard, ownership changed
		response := CheckpointResponse{
			Status:           Status{NotInService: false},
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request AssignRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.workerHeartbeats[request.WorkerID] = time.Now()

	var remove []string
	for k, v := range m.assignmentData {
		timeSinceLastHeartbeat := time.Since(m.workerHeartbeats[request.WorkerID])
		if timeSinceLastHeartbeat > time.Second*5 {
			remove = append(remove, k)
			for _, v := range v.shards {
				m.unassignedShards = append(m.unassignedShards, v)
			}
		}
		delete(m.workerHeartbeats, k)
	}
	for _, k := range remove {
		delete(m.assignmentData, k)
	}

	var ad *assignmentData
	var assignments []Assignment
	ad = m.assignmentData[request.WorkerID]
	if ad == nil {
		ad = &assignmentData{
			shards: make(map[string]types.Shard),
		}
		m.assignmentData[request.WorkerID] = ad
	}

	numberOfShardsToAssign := 1
	if ad.lastAssignmentCount > 0 {
		numberOfShardsToAssign = ad.lastAssignmentCount * 2
	}
	numberOfShardsToAssign = min(len(m.unassignedShards), numberOfShardsToAssign)
	for _, s := range m.unassignedShards[:numberOfShardsToAssign] {
		assignments = append(assignments, Assignment{
			ShardID:        *s.ShardId,
			SequenceNumber: m.checkpoints[*s.ShardId],
		})
		ad.shards[*s.ShardId] = s
	}
	m.unassignedShards = m.unassignedShards[numberOfShardsToAssign:]

	response := AssignResponse{
		Assignments: assignments,
	}

	res, err := json.Marshal(response)
	if err != nil {
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
	m.mut.Lock()
	defer m.mut.Unlock()
	m.inService = inService

	if m.inService {
		if m.shards == nil {
			input := &kinesis.ListShardsInput{
				StreamName: &m.cfg.StreamName,
			}
			out, err := m.kds.ListShards(context.Background(), input)
			if err != nil {
				m.logger.Error("failed to list shards", zap.Error(err))
				panic(err)
			}
			m.shards = out.Shards
		}
		m.unassignedShards = m.shards
		m.assignmentData = make(map[string]*assignmentData)
		m.checkpoints = make(map[string]string)

		for _, s := range m.unassignedShards {
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
	}
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
