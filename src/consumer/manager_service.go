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
	term      int64
	kds       aws.Kinesis
	kvs       KVS
	discovery shardDiscovery
	cfg       *PopConfig
	closed    []*closeNotification
	done      chan struct{}
	stop      chan struct{}
	logger    *zap.Logger
	clock     func() time.Time
	// All fields below this point the mutable state
	// and must be syncrhonized via mut
	mut *sync.Mutex
	// Indicates whether Manager is in service or not
	inService bool
	// Current checkpoints by shard id
	checkpoints map[string]string
	// Unassigned shards
	// Initialised with all root shards in a stream
	// Root shard is a shard without a parent,
	// or an expired parent and check point is
	// not CLOSED.
	// Checkpoint is CLOSED when all records
	// in the shard are processed and it has
	// either split or merged
	unassignedShards []*types.Shard
	// Live shards are shards that are currently
	// assigned or unassigned
	// Live shards indicate the size of total work
	// to be done
	liveShards map[string]*types.Shard
	// Keeps track of workers by their ID
	workers map[string]*workerInfo
	// Used to find the worker with the oldest heartbeat
	workerHeartbeats *primitives.PriorityQueue[string]
	// Used to find worker with most number of active shards
	// Active shard is a shard assigned to a worker and it
	// does not have an associated reassignment request
	workerShardCount *primitives.PriorityQueue[*workerInfo]
	// Monotonically increasing assignment offset
	// Assignment ID is calculated by adding this offset to
	// manager's term
	// Manager's term is also monotonically increasing in the
	// cluster during leader election
	nextAssignmentOffset int64
}

type shardDiscovery interface {
	GetAll() []*types.Shard
	GetRoots() []*types.Shard
	GetChildren(string) []*types.Shard
}

// Used to track a list of shards reassigned to
// a worker
type reassignmentOffer struct {
	// Reassigned shards keyed by current owner
	Shards map[*workerInfo][]*types.Shard
	// Used to check if reassignment is due
	// When a reassignment is due, assignments
	// are removed from the old worker and
	// assigned to current worker
	CreatedAt time.Time
}

type workerInfo struct {
	WorkerID          string
	LastHeartbeat     time.Time
	Assignments       map[string]*assignment
	ReassignmentOffer *reassignmentOffer
	ActiveShardCount  int
}

type assignment struct {
	ID                  int64
	Shard               *types.Shard
	ReassignmentRequest *reassignmentRequest
	Worker              *workerInfo
}

type reassignmentRequest struct {
	CreatedAt  time.Time
	ShardState *assignment
}

type closeNotification struct {
	ShardID string
	Worker  *workerInfo
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
		liveShards:       make(map[string]*types.Shard),
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

	response := m.assignOnce(&request)

	res, err := json.Marshal(response)
	if err != nil {
		m.logger.Error("error marshaling assign response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(res)
}

func (m *ManagerService) assignOnce(request *AssignRequest) *AssignResponse {
	currentTime := m.clock()
	m.mut.Lock()
	defer m.mut.Unlock()

	status := m.ensureInService()
	if status.NotInService {
		return &AssignResponse{Status: status}
	}

	m.releaseInactiveWorkers(currentTime)
	m.processClosedShards()
	worker := m.resolveActiveWorker(request.WorkerID, currentTime)

	if request.MaxShards == 0 {
		return &AssignResponse{}
	}

	oldActiveShardCount := worker.ActiveShardCount
	noOfReassignments := m.acquireDueReassignments(worker, currentTime)

	// Fulfill any additional capacity using unassigned pool
	remaining := max(request.MaxShards-noOfReassignments, 0)
	count := min(remaining, len(m.unassignedShards))
	m.assignFromUnassignedPool(count, worker)
	m.workerShardCount.Push(worker, float64(worker.ActiveShardCount))

	// if no shards were assigned in this round, attempt to rebalance by stealing shards from overloaded workers
	if oldActiveShardCount == worker.ActiveShardCount && worker.ReassignmentOffer == nil {
		m.rebalance(worker, currentTime)
	}

	var assignments []Assignment
	for _, a := range worker.Assignments {
		sn := m.checkpoints[*a.Shard.ShardId]
		assignments = append(assignments, Assignment{ID: a.ID, ShardID: *a.Shard.ShardId, SequenceNumber: sn})
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
		if empty || at.Sub(leastActiveWorker.LastHeartbeat) < m.cfg.WorkerInactivityTimeout() {
			break
		}
		// Move shards assigned to this worker back to unassigned pool
		// Skip if there's a re-assignment request because reassignment
		// will capture the shard
		for _, a := range leastActiveWorker.Assignments {
			_, released := releasedSet[*a.Shard.ShardId]
			if a.ReassignmentRequest == nil && !released {
				m.unassignedShards = append(m.unassignedShards, a.Shard)
				releasedSet[*a.Shard.ShardId] = struct{}{}
			}
		}
		// Move any shards offerred as part of a reassignment offer
		// back to unassigned pool
		if leastActiveWorker.ReassignmentOffer != nil {
			for _, shards := range leastActiveWorker.ReassignmentOffer.Shards {
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
	var unresolved []*closeNotification
	for len(m.closed) > 0 {
		n := m.closed[0]
		m.closed = m.closed[1:]
		if n.Worker != nil {
			delete(n.Worker.Assignments, n.ShardID)
			delete(m.liveShards, n.ShardID)
			n.Worker.ActiveShardCount--
			m.workerShardCount.Push(n.Worker, float64(n.Worker.ActiveShardCount))
			// Clear the worker so that in case n is unresolved,
			// we would not try adjust state more than once
			n.Worker = nil
		}
		children := m.discovery.GetChildren(n.ShardID)
		if len(children) == 0 {
			// Discovery service may not have fully caught-up with shard change,
			// try again during next assignment process
			unresolved = append(unresolved, n)
			continue
		}
		for _, child := range children {
			if m.checkpoints[*child.ParentShardId] == "CLOSED" && (child.AdjacentParentShardId == nil || m.checkpoints[*child.AdjacentParentShardId] == "CLOSED") {
				childCheckpoint := m.checkpoints[*child.ShardId]
				if childCheckpoint != "CLOSED" && m.liveShards[*child.ShardId] == nil {
					if childCheckpoint == "" {
						m.checkpoints[*child.ShardId] = "LATEST"
					}
					m.unassignedShards = append(m.unassignedShards, child)
					m.liveShards[*child.ShardId] = child
				} else {
					m.closed = append(m.closed, &closeNotification{ShardID: *child.ShardId})
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
			WorkerID:    workerID,
			Assignments: make(map[string]*assignment),
		}
		m.workers[workerID] = worker
	}
	worker.LastHeartbeat = at
	m.workerHeartbeats.Push(workerID, float64(worker.LastHeartbeat.UnixMilli()))
	return worker
}

func (m *ManagerService) acquireDueReassignments(worker *workerInfo, currentTime time.Time) int {
	noOfReassignments := 0
	if worker.ReassignmentOffer != nil && currentTime.Sub(worker.ReassignmentOffer.CreatedAt) >= m.cfg.ShardReleaseTimeout() {
		for oldWorker, shards := range worker.ReassignmentOffer.Shards {
			for _, shard := range shards {
				delete(oldWorker.Assignments, *shard.ShardId)
				worker.Assignments[*shard.ShardId] = &assignment{ID: m.getNextAssignmentID(), Shard: shard, Worker: worker}
				worker.ActiveShardCount++
				noOfReassignments++
			}
		}
		worker.ReassignmentOffer = nil
	}
	return noOfReassignments
}

func (m *ManagerService) assignFromUnassignedPool(count int, worker *workerInfo) {
	for _, s := range m.unassignedShards[0:count] {
		worker.Assignments[*s.ShardId] = &assignment{ID: m.getNextAssignmentID(), Shard: s, Worker: worker}
		worker.ActiveShardCount++
	}
	m.unassignedShards = m.unassignedShards[count:]
}

func (m *ManagerService) rebalance(worker *workerInfo, now time.Time) {
	// attempt to rebalance shards by redistributing from overloaded workers to this worker, aiming for an even shard distribution
	ideal := len(m.liveShards) / len(m.workers)
	target := ideal - worker.ActiveShardCount
	target = max(target, 0)
	if target > 0 {
		m.logger.Info("reassigning", zap.String("worker", worker.WorkerID), zap.Int("ideal", ideal), zap.Int("target", target))
	}
	ro := &reassignmentOffer{
		Shards:    make(map[*workerInfo][]*types.Shard),
		CreatedAt: now,
	}
	for target != 0 {
		// stop if there are no eligible workers to steal from, if the worker with the most shards is the requester,
		// or if the worker with the most shards does not exceed the ideal count (nothing to steal)
		w, _ := m.workerShardCount.Peek()
		if w == nil || w.WorkerID == worker.WorkerID || w.ActiveShardCount <= ideal {
			break
		}
		for _, v := range w.Assignments {
			if v.ReassignmentRequest == nil {
				v.ReassignmentRequest = &reassignmentRequest{CreatedAt: time.Now(), ShardState: v}
				ro.Shards[w] = append(ro.Shards[w], v.Shard)
				target--
				w.ActiveShardCount--
				m.workerShardCount.Push(w, float64(w.ActiveShardCount))
				break
			}
		}
	}
	if len(ro.Shards) > 0 {
		worker.ReassignmentOffer = ro
	}
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

	ctx := context.Background()

	assignmentKey := fmt.Sprintf("assignments/%s-assignment-id", request.ShardID)
	assignmentID := fmt.Sprintf("%020d", request.AssignmentID)
	firstAssignment := clientv3.Compare(clientv3.CreateRevision(assignmentKey), "=", 0)
	currentAssignment := clientv3.Compare(clientv3.Value(assignmentKey), "=", assignmentID)
	newAssignment := clientv3.Compare(clientv3.Value(assignmentKey), "<", assignmentID)
	conds := []clientv3.Cmp{currentAssignment, firstAssignment, newAssignment}

	putAssignmentID := clientv3.OpPut(assignmentKey, assignmentID)
	checkpointKey := fmt.Sprintf("checkpoints/%s", request.ShardID)
	putCheckpoint := clientv3.OpPut(checkpointKey, request.SequenceNumber)

	var checkpointSucceeded bool
	for _, cond := range conds {
		txn := m.kvs.Txn(ctx)
		resp, err := txn.If(cond).Then(putAssignmentID, putCheckpoint).Commit()
		if err != nil {
			m.logger.Error("error putting checkpoint in etcd", zap.Error(err))
			return nil, err
		}
		checkpointSucceeded = resp.Succeeded
		if checkpointSucceeded {
			break
		}
	}
	if !checkpointSucceeded {
		m.logger.Error("checkpoint failed", zap.String("reason", "fencing token mismatch"), zap.String("assignment-key", assignmentKey), zap.String("assignment-id", assignmentID))
		return &CheckpointResponse{OwnershipChanged: true}, nil
	}

	// Now that we have completed updating kvs with checkpoint, update
	// the local state to reflect that.
	// There's a chance that manager is not in service at this point making the
	// following actions redundant but safe
	m.mut.Lock()
	defer m.mut.Unlock()

	worker := m.workers[request.WorkerID]
	if worker == nil || worker.Assignments[request.ShardID] == nil || worker.Assignments[request.ShardID].ID != request.AssignmentID {
		// Worker doesn't exist, ownership changed
		return &CheckpointResponse{OwnershipChanged: true}, nil
	}

	// Worker owns the shard, update checkpoint
	m.checkpoints[request.ShardID] = request.SequenceNumber
	if request.SequenceNumber == "CLOSED" {
		m.logger.Info("shard closed notification received", zap.String("shard-id", request.ShardID))
		m.closed = append(m.closed, &closeNotification{ShardID: request.ShardID, Worker: worker})
	}

	// Release the shard is it has been requested
	assignment := worker.Assignments[request.ShardID]
	if assignment.ReassignmentRequest != nil {
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
	if worker == nil || worker.Assignments[request.ShardID] == nil || worker.Assignments[request.ShardID].ID != request.AssignmentID {
		// Worker doesn't exist, ownership changed
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
	var closed []*closeNotification
	allShards := m.discovery.GetAll()
	for _, shard := range allShards {
		// ETCD client v3 has built-in retry logic
		checkpointKey := fmt.Sprintf("checkpoints/%s", *shard.ShardId)
		cp, err := m.kvs.Get(context.Background(), checkpointKey)
		if err != nil {
			m.logger.Error("failed to get checkpoint from kvs", zap.Error(err))
			// TODO: Review this
			panic(err)
		}
		if len(cp.Kvs) > 0 {
			checkpoints[*shard.ShardId] = string(cp.Kvs[0].Value)
		} else {
			checkpoints[*shard.ShardId] = "LATEST"
		}
		m.logger.Info("checkpoint initialised", zap.String("shard-id", *shard.ShardId), zap.String("sequence-number", checkpoints[*shard.ShardId]))
	}

	roots := m.discovery.GetRoots()
	liveShards := make(map[string]*types.Shard)
	for _, root := range roots {
		if checkpoints[*root.ShardId] != "CLOSED" {
			m.logger.Info("unassigned root", zap.String("shard-id", *root.ShardId))
			unassignedShards = append(unassignedShards, root)
			liveShards[*root.ShardId] = root
		} else {
			m.logger.Info("closed root", zap.String("shard-id", *root.ShardId))
			closed = append(closed, &closeNotification{ShardID: *root.ShardId})
		}
	}

	// Finally update the state and put manager into in service mode
	m.mut.Lock()

	m.workers = make(map[string]*workerInfo)
	m.workerHeartbeats = primitives.NewPriorityQueue[string](false)
	m.workerShardCount = primitives.NewPriorityQueue[*workerInfo](true)
	m.unassignedShards = unassignedShards
	m.checkpoints = checkpoints
	m.closed = closed
	m.liveShards = liveShards

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
		for _, v := range w.Assignments {
			shards = append(shards, ShardState{ShardID: *v.Shard.ShardId})
		}
		workers = append(workers, WorkerState{
			WorkerID:               k,
			NumberOfAssignedShards: w.ActiveShardCount,
			AssignmentsLength:      len(w.Assignments),
			Shards:                 shards,
			LastHeartbeat:          now.Sub(w.LastHeartbeat).String(),
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
