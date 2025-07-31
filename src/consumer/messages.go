package consumer

type Status struct {
	NotInService bool
}

type Assignment struct {
	ID             int64
	ShardID        string
	SequenceNumber string
}

type CheckpointRequest struct {
	AssignmentID   int64
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

type AssignmentState struct {
	ShardID      string
	AssignmentID int64
}

type WorkerState struct {
	WorkerID               string
	AssignmentsLength      int
	NumberOfAssignedShards int
	Assignments            []AssignmentState
	LastHeartbeat          string
}

type StateResponse struct {
	Status
	Workers []WorkerState
}
