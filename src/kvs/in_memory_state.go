package kvs

import (
	"fmt"

	"github.com/buddhike/pebble/kvs/pb"
	"github.com/google/uuid"
)

type session struct {
	lastSequence  int64
	lastTimestamp int64
	operation     pb.Op
	response      *pb.ProposeResponse
}

type inMemoryMap struct {
	sessions map[uuid.UUID]session
	i        map[string][]byte
}

func (m *inMemoryMap) Apply(entry *pb.Entry) *pb.ProposeResponse {
	switch entry.Operation {
	case pb.Op_Noop:
		return &pb.ProposeResponse{
			Accepted: true,
			Error:    pb.Error_NoError,
		}
	case pb.Op_CreateSession:
		return m.createSession(entry)
	case pb.Op_Set:
		return m.applyMutation(entry)
	case pb.Op_Del:
		return m.applyMutation(entry)
	case pb.Op_KeepAlive:
		return m.applyMutation(entry)
	}
	panic(fmt.Sprintf("unknown op %d", entry.Operation))
}

func (m *inMemoryMap) Read(key []byte) []byte {
	return []byte(m.i[string(key)])
}

func (m *inMemoryMap) createSession(entry *pb.Entry) *pb.ProposeResponse {
	sid, err := uuid.FromBytes(entry.SessionID)
	if err != nil {
		return &pb.ProposeResponse{
			Accepted: false,
			Error:    pb.Error_ClientError,
			Value:    []byte(fmt.Sprintf("createSession: unable to parse session id: %v", err)),
		}
	}
	if _, ok := m.sessions[sid]; ok {
		return &pb.ProposeResponse{
			Accepted: false,
			Error:    pb.Error_ClientError,
			Value:    []byte(fmt.Sprintf("session id is in use: %v", sid)),
		}
	}
	m.sessions[sid] = session{
		lastTimestamp: entry.Timestamp,
		operation:     entry.Operation,
	}
	return &pb.ProposeResponse{
		Accepted: true,
		Error:    pb.Error_NoError,
		Value:    entry.SessionID,
	}
}

func (m *inMemoryMap) applyMutation(entry *pb.Entry) *pb.ProposeResponse {
	sid, err := uuid.FromBytes(entry.SessionID)
	if err != nil {
		return &pb.ProposeResponse{
			Accepted: false,
			Error:    pb.Error_ClientError,
			Value:    []byte(fmt.Sprintf("applyMutation: unable to parse session id: %v", err)),
		}
	}
	if s, ok := m.sessions[sid]; ok {
		if s.lastSequence == entry.Sequence {
			if s.operation != entry.Operation {
				return &pb.ProposeResponse{
					Accepted: false,
					Error:    pb.Error_ClientError,
					Value:    []byte(fmt.Sprintf("applyMutation: sequence number is operation mismatch %d %d %d ", entry.Sequence, s.operation, entry.Operation)),
				}
			}
			return s.response
		}
		if s.lastSequence > entry.Sequence {
			return &pb.ProposeResponse{
				Accepted: false,
				Error:    pb.Error_ClientError,
				Value:    []byte(fmt.Sprintf("applyMutation: supplied sequence number %d is lower than last sequence number %d", entry.Sequence, s.lastSequence)),
			}
		}
		switch entry.Operation {
		case pb.Op_Set:
			m.i[string(entry.Key)] = entry.Value
		case pb.Op_Del:
			delete(m.i, string(entry.Key))
		}
		response := &pb.ProposeResponse{
			Accepted: true,
			Error:    pb.Error_NoError,
		}
		s.lastSequence = entry.Sequence
		s.lastTimestamp = entry.Timestamp
		s.response = response
		return response
	}
	return &pb.ProposeResponse{
		Accepted: false,
		Error:    pb.Error_SessionNotFound,
	}
}

func newInMemoryMap() *inMemoryMap {
	return &inMemoryMap{
		sessions: make(map[uuid.UUID]session),
		i:        make(map[string][]byte),
	}
}
