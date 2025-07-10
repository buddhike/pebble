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
)

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

func NewManagerService(cfg *ConsumerConfig, kds aws.Kinesis, kvs KVS, stop chan struct{}) *ManagerService {
	return &ManagerService{
		cfg:                cfg,
		mut:                &sync.Mutex{},
		kds:                kds,
		kvs:                kvs,
		done:               make(chan struct{}),
		stop:               stop,
		workerHeartbeats:   make(map[string]time.Time),
		healthcheckTimeout: time.Second * time.Duration(cfg.HealthcheckTimeoutSeconds),
	}
}

func (m *ManagerService) Start() error {
	url, err := m.cfg.GetManagerListenUrl()
	if err != nil {
		return err
	}
	go func() {
		http.HandleFunc("/checkpoint/", m.Checkpoint)
		http.HandleFunc("/assign/", m.Assign)
		http.HandleFunc("/status/", m.Status)
		http.HandleFunc("/health/", m.Health)
		http.ListenAndServe(fmt.Sprintf("%s:%s", url.Hostname(), url.Port()), nil)
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
		fmt.Println("EtcdServer Healthcheck failed", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("unhealthy-%s-%d: %v", m.cfg.Name, m.cfg.ManagerID, err)))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("healthy-%s-%d", m.cfg.Name, m.cfg.ManagerID)))
}
