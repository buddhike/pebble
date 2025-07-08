package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type Worker struct {
	workerID          string
	port              int
	streamConsumerArn string
	kds               Kinesis
}

func NewWorker(workerID string, port int, streamConsumerArn string, kds Kinesis) *Worker {
	return &Worker{
		workerID:          workerID,
		port:              port,
		streamConsumerArn: streamConsumerArn,
		kds:               kds,
	}
}

func (w *Worker) Start() {
	for {
		// Create assign request
		assignReq := AssignRequest{
			WorkerID: w.workerID,
		}

		// Marshal request to JSON
		reqBody, err := json.Marshal(assignReq)
		if err != nil {
			log.Printf("Failed to marshal assign request: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Make HTTP request to manager
		resp, err := http.Post("http://localhost:9000/assign/", "application/json", bytes.NewBuffer(reqBody))
		if err != nil {
			log.Printf("Failed to make assign request: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Read response body
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Failed to read response body: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Unmarshal response
		var assignResp AssignResponse
		err = json.Unmarshal(respBody, &assignResp)
		if err != nil {
			log.Printf("Failed to unmarshal assign response: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Check if we got any assignments
		if len(assignResp.Assignments) > 0 {
			log.Printf("Received %d shard assignments", len(assignResp.Assignments))
			for _, assignment := range assignResp.Assignments {
				w.processShard(assignment)
			}
		} else {
			log.Printf("No shard assignments received")
		}

		// Wait before next request
		time.Sleep(5 * time.Second)
	}

}

func (w *Worker) processShard(assignment Assignment) {
	// Determine starting position for subscription
	var startingPosition types.StartingPosition
	if assignment.SequenceNumber == "LATEST" {
		startingPosition = types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		}
	} else {
		startingPosition = types.StartingPosition{
			Type:           types.ShardIteratorTypeAtSequenceNumber,
			SequenceNumber: &assignment.SequenceNumber,
		}
	}

	// Create subscription input
	subscribeInput := &kinesis.SubscribeToShardInput{
		ConsumerARN:      &w.streamConsumerArn,
		ShardId:          &assignment.ShardID,
		StartingPosition: &startingPosition,
	}

	// Subscribe to shard
	output, err := w.kds.SubscribeToShard(context.Background(), subscribeInput)
	if err != nil {
		log.Printf("Failed to subscribe to shard %s: %v", assignment.ShardID, err)
		return
	}

	// Process events from the subscription
	for event := range output.GetStream().Events() {
		if event == nil {
			log.Printf("Error in shard subscription %s", assignment.ShardID)
			return
		}

		evt := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
		if evt == nil {
			log.Printf("Unexpected event type")
			return
		}

		// Process records
		for _, record := range evt.Value.Records {
			log.Printf("Processing record from shard %s: key: %s value: %s", assignment.ShardID, *record.PartitionKey, string(record.Data))
		}

		// Checkpoint after processing records
		if len(evt.Value.Records) > 0 {
			lastRecord := evt.Value.Records[len(evt.Value.Records)-1]
			checkpointReq := CheckpointRequest{
				WorkerID:       w.workerID,
				ShardID:        assignment.ShardID,
				SequenceNumber: *lastRecord.SequenceNumber,
			}

			reqBody, err := json.Marshal(checkpointReq)
			if err != nil {
				log.Printf("Failed to marshal checkpoint request: %v", err)
				continue
			}

			resp, err := http.Post("http://localhost:9000/checkpoint/", "application/json", bytes.NewBuffer(reqBody))
			if err != nil {
				log.Printf("Failed to make checkpoint request: %v", err)
				continue
			}

			respBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Printf("Failed to read checkpoint response: %v", err)
				continue
			}

			var checkpointResp CheckpointResponse
			err = json.Unmarshal(respBody, &checkpointResp)
			if err != nil {
				log.Printf("Failed to unmarshal checkpoint response: %v", err)
				continue
			}

			if checkpointResp.OwnershipChanged {
				log.Printf("Ownership of shard %s changed, stopping processing", assignment.ShardID)
				return
			}

			if checkpointResp.NotInService {
				log.Printf("Manager not in service, stopping processing of shard %s", assignment.ShardID)
				return
			}

			log.Printf("Successfully checkpointed shard %s at sequence number %s", assignment.ShardID, *lastRecord.SequenceNumber)
		}
	}

}
