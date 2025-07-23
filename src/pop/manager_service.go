package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/buddhike/pebble/messages"
	"github.com/buddhike/pebble/primitives"
	"go.uber.org/zap"
)

type ManagerService struct {
	mut                  *sync.Mutex
	inService            bool
	term                 int64
	kds                  aws.Kinesis
	kvs                  KVS
	cfg                  *PopConfig
	shards               []types.Shard
	unassignedShards     []*types.Shard
	checkpoints          map[string]string
	done                 chan struct{}
	stop                 chan struct{}
	workers              map[string]*workerInfo
	workerHeartbeats     *primitives.PriorityQueue[string]
	workerShardCount     *primitives.PriorityQueue[*workerInfo]
	logger               *zap.Logger
	clock                func() time.Time
	nextAssignmentOffset int64
}

type reassignmentOffer struct {
	shards    map[*workerInfo][]*types.Shard
	createdAt time.Time
}

type workerInfo struct {
	workerID          string
	lastHeartbeat     time.Time
	assignments       map[string]*assignment
	reassignmentOffer *reassignmentOffer
	activeShardCount  int
}

type assignment struct {
	id                  int64
	shard               *types.Shard
	reassignmentRequest *reassignmentRequest
	worker              *workerInfo
}

type reassignmentRequest struct {
	createdAt  time.Time
	shardState *assignment
}

func NewManagerService(cfg *PopConfig, kds aws.Kinesis, kvs KVS, stop chan struct{}, logger *zap.Logger) *ManagerService {
	return &ManagerService{
		cfg:              cfg,
		mut:              &sync.Mutex{},
		kds:              kds,
		kvs:              kvs,
		done:             make(chan struct{}),
		stop:             stop,
		workers:          make(map[string]*workerInfo),
		workerHeartbeats: primitives.NewPriorityQueue[string](false),
		workerShardCount: primitives.NewPriorityQueue[*workerInfo](true),
		logger:           logger.Named("managerservice").With(zap.Int("managerid", cfg.PopID)),
		clock:            func() time.Time { return time.Now() },
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
		http.HandleFunc("/state/", m.State)
		server.ListenAndServe()
		close(m.done)
	}()

	go func() {
		<-m.stop
		server.Close()
	}()

	return nil
}

func (m *ManagerService) Assign(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.logger.Error("error reading assign request body", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request messages.AssignRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		m.logger.Error("error unmarshaling assign request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response := m.handleAssignRequest(&request)

	res, err := json.Marshal(response)
	if err != nil {
		m.logger.Error("error marshaling assign response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (m *ManagerService) handleAssignRequest(request *messages.AssignRequest) *messages.AssignResponse {
	now := m.clock()
	m.mut.Lock()
	defer m.mut.Unlock()

	status := m.ensureInService()
	if status.NotInService {
		return &messages.AssignResponse{Status: status}
	}

	m.releaseInactiveWorkers(now)
	worker := m.resolveActiveWorker(request.WorkerID, now)

	// If worker has no capacity, don't assign anything
	// TODO: Review this design
	if request.MaxShards == 0 {
		return &messages.AssignResponse{}
	}

	oldActiveShardCount := worker.activeShardCount
	// Process due reassignment offers
	noOfReassignments := 0
	if worker.reassignmentOffer != nil && now.Sub(worker.reassignmentOffer.createdAt) >= m.cfg.ShardReleaseTimeout() {
		for oldWorker, shards := range worker.reassignmentOffer.shards {
			for _, shard := range shards {
				delete(oldWorker.assignments, *shard.ShardId)
				worker.assignments[*shard.ShardId] = &assignment{id: m.getNextAssignmentID(), shard: shard, worker: worker}
				worker.activeShardCount++
				noOfReassignments++
			}
		}
		worker.reassignmentOffer = nil
	}

	// Fulfill any additional capacity using unassigned pool
	remaining := max(request.MaxShards-noOfReassignments, 0)
	count := min(remaining, len(m.unassignedShards))
	for _, s := range m.unassignedShards[0:count] {
		worker.assignments[*s.ShardId] = &assignment{id: m.getNextAssignmentID(), shard: s, worker: worker}
		worker.activeShardCount++
	}
	m.unassignedShards = m.unassignedShards[count:]
	m.workerShardCount.Push(worker, float64(worker.activeShardCount))

	// if no shards were assigned in this round, attempt to rebalance by stealing shards from overloaded workers
	if oldActiveShardCount == worker.activeShardCount && worker.reassignmentOffer == nil {
		// attempt to rebalance shards by redistributing from overloaded workers to this worker, aiming for an even shard distribution
		ideal := len(m.shards) / len(m.workers)
		target := ideal - worker.activeShardCount
		target = max(target, 0)
		if target > 0 {
			m.logger.Info("reassigning", zap.String("worker", worker.workerID), zap.Int("ideal", ideal), zap.Int("target", target))
		}
		ro := &reassignmentOffer{
			shards:    make(map[*workerInfo][]*types.Shard),
			createdAt: now,
		}
		for target != 0 {
			// stop if there are no eligible workers to steal from, if the worker with the most shards is the requester,
			// or if the worker with the most shards does not exceed the ideal count (nothing to steal)
			w, _ := m.workerShardCount.Peek()
			if w == nil || w.workerID == request.WorkerID || w.activeShardCount <= ideal {
				break
			}
			for _, v := range w.assignments {
				if v.reassignmentRequest == nil {
					v.reassignmentRequest = &reassignmentRequest{createdAt: time.Now(), shardState: v}
					ro.shards[w] = append(ro.shards[w], v.shard)
					target--
					w.activeShardCount--
					m.workerShardCount.Push(w, float64(w.activeShardCount))
					break
				}
			}
		}
		if len(ro.shards) > 0 {
			worker.reassignmentOffer = ro
		}
	}

	var assignments []messages.Assignment
	for _, a := range worker.assignments {
		sn := m.checkpoints[*a.shard.ShardId]
		assignments = append(assignments, messages.Assignment{ID: a.id, ShardID: *a.shard.ShardId, SequenceNumber: sn})
	}

	return &messages.AssignResponse{
		Assignments: assignments,
	}
}

// move shards from workers that have not sent a heartbeat within the timeout back to the unassigned pool
func (m *ManagerService) releaseInactiveWorkers(at time.Time) {
	releasedSet := make(map[string]struct{})
	for {
		leastActiveWorkerID, _ := m.workerHeartbeats.Peek()
		leastActiveWorker := m.workers[leastActiveWorkerID]
		if leastActiveWorker == nil || at.Sub(leastActiveWorker.lastHeartbeat) < m.cfg.WorkerInactivityTimeout() {
			break
		}
		for _, a := range leastActiveWorker.assignments {
			_, released := releasedSet[*a.shard.ShardId]
			if a.reassignmentRequest == nil && !released {
				m.unassignedShards = append(m.unassignedShards, a.shard)
				releasedSet[*a.shard.ShardId] = struct{}{}
			}
		}
		if leastActiveWorker.reassignmentOffer != nil {
			for _, shards := range leastActiveWorker.reassignmentOffer.shards {
				for _, s := range shards {
					_, released := releasedSet[*s.ShardId]
					if !released {
						m.unassignedShards = append(m.unassignedShards, s)
						releasedSet[*s.ShardId] = struct{}{}
					}
				}
			}
		}
		// cleanup worker from all data structures
		m.workerHeartbeats.Pop()
		m.workerShardCount.Remove(leastActiveWorker)
		delete(m.workers, leastActiveWorkerID)
	}
}

func (m *ManagerService) resolveActiveWorker(workerID string, at time.Time) *workerInfo {
	worker := m.workers[workerID]
	if worker == nil {
		worker = &workerInfo{
			workerID:    workerID,
			assignments: make(map[string]*assignment),
		}
		m.workers[workerID] = worker
	}
	worker.lastHeartbeat = at
	m.workerHeartbeats.Push(workerID, float64(worker.lastHeartbeat.UnixMilli()))
	return worker
}

func (m *ManagerService) getNextAssignmentID() int64 {
	offset := m.nextAssignmentOffset
	return m.term + offset
}

func (m *ManagerService) Checkpoint(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.logger.Error("error reading checkpoint request body", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var request messages.CheckpointRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		m.logger.Error("error unmashaling checkpoint request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response, err := m.handleCheckpointRequest(&request)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("checkpoint failed"))
		return
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

func (m *ManagerService) handleCheckpointRequest(request *messages.CheckpointRequest) (*messages.CheckpointResponse, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	status := m.ensureInService()
	if status.NotInService {
		return &messages.CheckpointResponse{Status: status}, nil
	}

	worker := m.workers[request.WorkerID]
	if worker == nil || worker.assignments[request.ShardID] == nil {
		// Worker doesn't exist, ownership changed
		return &messages.CheckpointResponse{OwnershipChanged: true}, nil
	}

	// Store checkpoint in KVS
	_, err := m.kvs.Put(context.Background(), request.ShardID, request.SequenceNumber)
	if err != nil {
		m.logger.Error("error putting checkpoint in etcd", zap.Error(err))
		return nil, err
	}

	// Worker owns the shard, update checkpoint
	m.checkpoints[request.ShardID] = request.SequenceNumber

	// Release the shard is it has been requested
	assignment := worker.assignments[request.ShardID]
	if assignment.reassignmentRequest != nil {
		return &messages.CheckpointResponse{OwnershipChanged: true}, nil
	}
	return &messages.CheckpointResponse{}, nil
}

func (m *ManagerService) Status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if m.inService {
		w.Write([]byte("leader"))
	} else {
		w.Write([]byte("follower"))
	}
}

func (m *ManagerService) ensureInService() messages.Status {
	s := messages.Status{}
	if !m.inService {
		s.NotInService = true
	}
	return s
}

func (m *ManagerService) SetToInService(term int64) {
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
	m.workers = make(map[string]*workerInfo)
	m.checkpoints = make(map[string]string)
	m.workerHeartbeats = primitives.NewPriorityQueue[string](false)
	m.workerShardCount = primitives.NewPriorityQueue[*workerInfo](true)

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
	m.term = term
	m.mut.Unlock()
}

func (m *ManagerService) SetToOutOfService() {
	m.mut.Lock()
	m.inService = false
	m.mut.Unlock()
}

func (m *ManagerService) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.HealthcheckTimeout())
	_, err := m.kvs.Get(ctx, "/healthcheck")
	cancel()
	if err != nil {
		m.logger.Error("etcdserver healthcheck failed", zap.Error(err))
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(fmt.Sprintf("unhealthy-%s-%d: %v", m.cfg.Name, m.cfg.PopID, err)))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("healthy-%s-%d", m.cfg.Name, m.cfg.PopID)))
}

func (m *ManagerService) State(w http.ResponseWriter, r *http.Request) {
	res := m.handleStateRequest()
	buf, err := json.Marshal(res)
	if err != nil {
		m.logger.Error("error when marshaling state response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(buf)
}

func (m *ManagerService) handleStateRequest() *messages.StateResponse {
	m.mut.Lock()
	defer m.mut.Unlock()

	now := m.clock()
	status := m.ensureInService()
	if status.NotInService {
		return &messages.StateResponse{Status: status}
	}

	shards := make([]messages.ShardState, 0)
	for _, s := range m.shards {
		var wd *workerInfo
		for _, v := range m.workers {
			if v.assignments[*s.ShardId] != nil {
				wd = v
				break
			}
		}
		var workerID string
		var lhb string
		if wd != nil {
			workerID = wd.workerID
			lhb = now.Sub(wd.lastHeartbeat).String()
		}
		shards = append(shards, messages.ShardState{ShardID: *s.ShardId, WorkerID: workerID, LastHeartbeat: lhb})
	}

	workers := make([]messages.WorkerState, 0)
	for k, w := range m.workers {
		workers = append(workers, messages.WorkerState{WorkerID: k, NumberOfAssignedShards: w.activeShardCount, AssignmentsLength: len(w.assignments)})
	}

	slices.SortFunc(shards, func(a, b messages.ShardState) int {
		if a.ShardID < b.ShardID {
			return -1
		}
		if a.ShardID > b.ShardID {
			return 1
		}
		return 0
	})

	slices.SortFunc(workers, func(a, b messages.WorkerState) int {
		if a.WorkerID < b.WorkerID {
			return -1
		}
		if a.WorkerID > b.WorkerID {
			return 1
		}
		return 0
	})

	return &messages.StateResponse{
		Shards:  shards,
		Workers: workers,
	}
}
