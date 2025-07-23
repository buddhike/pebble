package messages

type Status struct {
	NotInService bool
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

type ShardState struct {
	ShardID       string
	WorkerID      string
	LastHeartbeat string
}

type WorkerState struct {
	WorkerID               string
	AssignmentsLength      int
	NumberOfAssignedShards int
}

type StateResponse struct {
	Status
	Shards  []ShardState
	Workers []WorkerState
}
