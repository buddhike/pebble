package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/aws"
	"github.com/buddhike/pebble/middleware"
	"github.com/buddhike/pebble/primitives"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type ManagerService struct {
	mut              *sync.Mutex
	inService        bool
	term             int64
	kds              aws.Kinesis
	kvs              KVS
	discovery        shardDiscovery
	cfg              *PopConfig
	unassignedShards []*types.Shard
	checkpoints      map[string]string
	closed           []string
	done             chan struct{}
	stop             chan struct{}
	// The following fields must always be updated together and only while holding mut:
	// - workers
	// - workerHeartbeats
	// - workerShardCount
	// Any addition or removal of a worker must update all three atomically under the lock.
	workers              map[string]*workerInfo
	workerHeartbeats     *primitives.PriorityQueue[string]
	workerShardCount     *primitives.PriorityQueue[*workerInfo]
	openShardCount       int
	logger               *zap.Logger
	clock                func() time.Time
	nextAssignmentOffset int64
}

type shardDiscovery interface {
	GetAll() []*types.Shard
	GetRoots() []*types.Shard
	GetChildren(string) []*types.Shard
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

func NewManagerService(cfg *PopConfig, kds aws.Kinesis, kvs KVS, shardDiscovery shardDiscovery, stop chan struct{}, logger *zap.Logger) *ManagerService {
	return &ManagerService{
		cfg:              cfg,
		mut:              &sync.Mutex{},
		kds:              kds,
		kvs:              kvs,
		discovery:        shardDiscovery,
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
		http.HandleFunc("/checkpoint/", middleware.StateCriticalRoute(m.Checkpoint, m.logger))
		http.HandleFunc("/assign/", middleware.StateCriticalRoute(m.Assign, m.logger))
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

	var request AssignRequest
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

func (m *ManagerService) handleAssignRequest(request *AssignRequest) *AssignResponse {
	now := m.clock()
	m.mut.Lock()
	defer m.mut.Unlock()

	status := m.ensureInService()
	if status.NotInService {
		return &AssignResponse{Status: status}
	}

	m.releaseInactiveWorkers(now)
	m.processClosedShards()
	worker := m.resolveActiveWorker(request.WorkerID, now)

	// If worker has no capacity, don't assign anything
	// TODO: Review this design
	if request.MaxShards == 0 {
		return &AssignResponse{}
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
		ideal := m.openShardCount / len(m.workers)
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

	var assignments []Assignment
	for _, a := range worker.assignments {
		sn := m.checkpoints[*a.shard.ShardId]
		assignments = append(assignments, Assignment{ID: a.id, ShardID: *a.shard.ShardId, SequenceNumber: sn})
	}

	return &AssignResponse{
		Assignments: assignments,
	}
}

// Move shards from workers that have not sent a heartbeat within the timeout back to the unassigned pool
func (m *ManagerService) releaseInactiveWorkers(at time.Time) {
	// Must update workers, workerHeartbeats, and workerShardCount together under lock
	releasedSet := make(map[string]struct{})
	for {
		leastActiveWorkerID, empty := m.workerHeartbeats.Peek()
		leastActiveWorker := m.workers[leastActiveWorkerID]
		if !empty && leastActiveWorker == nil {
			panic(fmt.Sprintf("workers and workerHeartbeats are out of sync: %s", leastActiveWorkerID))
		}
		if empty || at.Sub(leastActiveWorker.lastHeartbeat) < m.cfg.WorkerInactivityTimeout() {
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

func (m *ManagerService) processClosedShards() {
	var unresolved []string
	for len(m.closed) > 0 {
		shardID := m.closed[0]
		m.closed = m.closed[1:]
		for _, worker := range m.workers {
			if worker.assignments[shardID] != nil {
				delete(worker.assignments, shardID)
				worker.activeShardCount--
				m.workerShardCount.Push(worker, float64(worker.activeShardCount))
				break
			}
		}
		children := m.discovery.GetChildren(shardID)
		if len(children) == 0 {
			// Discovery service may not have fully caught-up with shard change,
			// try again during next assignment process
			unresolved = append(unresolved, shardID)
			continue
		}
		for _, child := range children {
			if m.checkpoints[*child.ParentShardId] == "CLOSED" && (child.AdjacentParentShardId == nil || m.checkpoints[*child.AdjacentParentShardId] == "CLOSED") {
				if m.checkpoints[*child.ShardId] != "CLOSED" {
					m.unassignedShards = append(m.unassignedShards, child)
					m.openShardCount++
				} else {
					m.closed = append(m.closed, *child.ShardId)
				}
			}
		}
	}
	m.closed = unresolved
}

func (m *ManagerService) resolveActiveWorker(workerID string, at time.Time) *workerInfo {
	// Must add to workers and workerHeartbeats together under lock
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

	var request CheckpointRequest
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

func (m *ManagerService) handleCheckpointRequest(request *CheckpointRequest) (*CheckpointResponse, error) {
	res := m.ensureCheckpointOwnership(request)
	if res != nil {
		return res, nil
	}

	// Store checkpoint in KVS
	// This is performed without holding the lock to prevent holding up
	// assignments and other checkpoints
	txn := m.kvs.Txn(context.Background())

	cmps := make([]clientv3.Cmp, 2)
	assignmentKey := fmt.Sprintf("%s-assignment-id", request.ShardID)
	assignmentID := fmt.Sprintf("%d", request.AssignmentID)
	cmps[0] = clientv3.Compare(clientv3.CreateRevision(assignmentKey), "=", 0)
	cmps[1] = clientv3.Compare(clientv3.Value(assignmentKey), "=", assignmentID)

	putAssignmentID := clientv3.OpPut(assignmentKey, assignmentID)
	putCheckpoint := clientv3.OpPut(request.ShardID, request.SequenceNumber)

	_, err := txn.If(cmps...).Then(putAssignmentID, putCheckpoint).Commit()
	if err != nil {
		m.logger.Error("error putting checkpoint in etcd", zap.Error(err))
		return nil, err
	}

	// Now that we have completed updating kvs with checkpoint, update
	// the local state to reflect that.
	// There's a chance that manager is not in service at this point making the
	// following actions redundant but safe
	m.mut.Lock()
	defer m.mut.Unlock()

	worker := m.workers[request.WorkerID]
	if worker == nil || worker.assignments[request.ShardID] == nil || worker.assignments[request.ShardID].id != request.AssignmentID {
		// Worker doesn't exist, ownership changed
		return &CheckpointResponse{OwnershipChanged: true}, nil
	}

	// Worker owns the shard, update checkpoint
	m.checkpoints[request.ShardID] = request.SequenceNumber
	if request.SequenceNumber == "CLOSED" {
		m.closed = append(m.closed, request.ShardID)
		m.openShardCount--
	}

	// Release the shard is it has been requested
	assignment := worker.assignments[request.ShardID]
	if assignment.reassignmentRequest != nil {
		return &CheckpointResponse{OwnershipChanged: true}, nil
	}
	return &CheckpointResponse{}, nil
}

func (m *ManagerService) ensureCheckpointOwnership(request *CheckpointRequest) *CheckpointResponse {
	m.mut.Lock()
	defer m.mut.Unlock()
	status := m.ensureInService()
	if status.NotInService {
		return &CheckpointResponse{Status: status}
	}
	worker := m.workers[request.WorkerID]
	if worker == nil {
		// Worker doesn't exist
		return &CheckpointResponse{OwnershipChanged: true}
	}

	assignment := worker.assignments[request.ShardID]
	if assignment == nil || assignment.id != request.AssignmentID {
		// Assignment doesn't exist or ownership changed
		return &CheckpointResponse{OwnershipChanged: true}
	}
	return nil
}

func (m *ManagerService) Status(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if m.inService {
		w.Write([]byte("leader"))
	} else {
		w.Write([]byte("follower"))
	}
}

func (m *ManagerService) ensureInService() Status {
	s := Status{}
	if !m.inService {
		s.NotInService = true
	}
	return s
}

func (m *ManagerService) SetToInService(term int64) {
	var unassignedShards []*types.Shard
	checkpoints := make(map[string]string)
	var closed []string
	allShards := m.discovery.GetAll()
	for _, shard := range allShards {
		// ETCD client v3 has built-in retry logic
		cp, err := m.kvs.Get(context.Background(), *shard.ShardId)
		if err != nil {
			m.logger.Error("failed to get checkpoint from kvs", zap.Error(err))
			// TODO: Review this
			panic(err)
		}
		if cp.Count > 0 {
			checkpoints[*shard.ShardId] = string(cp.Kvs[0].Value)
		} else {
			checkpoints[*shard.ShardId] = "LATEST"
		}
	}

	roots := m.discovery.GetRoots()
	for _, root := range roots {
		if checkpoints[*root.ShardId] != "CLOSED" {
			unassignedShards = append(unassignedShards, root)
		} else {
			closed = append(closed, *root.ShardId)
		}
	}

	// Finally update the state and put manager into in service mode
	m.mut.Lock()

	m.workers = make(map[string]*workerInfo)
	m.workerHeartbeats = primitives.NewPriorityQueue[string](false)
	m.workerShardCount = primitives.NewPriorityQueue[*workerInfo](true)
	m.unassignedShards = unassignedShards
	m.checkpoints = checkpoints
	m.openShardCount = len(unassignedShards)
	m.closed = closed

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

func (m *ManagerService) handleStateRequest() *StateResponse {
	m.mut.Lock()
	defer m.mut.Unlock()

	now := m.clock()
	status := m.ensureInService()
	if status.NotInService {
		return &StateResponse{Status: status}
	}

	workers := make([]WorkerState, 0)
	for k, w := range m.workers {
		var shards []ShardState
		for _, v := range w.assignments {
			shards = append(shards, ShardState{ShardID: *v.shard.ShardId})
		}
		workers = append(workers, WorkerState{
			WorkerID:               k,
			NumberOfAssignedShards: w.activeShardCount,
			AssignmentsLength:      len(w.assignments),
			Shards:                 shards,
			LastHeartbeat:          now.Sub(w.lastHeartbeat).String(),
		})
	}

	slices.SortFunc(workers, func(a, b WorkerState) int {
		if a.WorkerID < b.WorkerID {
			return -1
		}
		if a.WorkerID > b.WorkerID {
			return 1
		}
		return 0
	})

	return &StateResponse{
		Workers: workers,
	}
}
