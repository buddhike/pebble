#!/bin/bash

# Configuration
export AWS_PAGER=""
STREAM_NAME="test"
DATA="test"
TOTAL_RECORDS=10000  # Total number of records to send
PAUSE_INTERVAL=1000  # Pause after this many records
PAUSE_DURATION=1     # Pause duration in seconds

# Counter for tracking records sent
count=0

echo "Starting to send data to Kinesis stream: $STREAM_NAME"
echo "Will pause for $PAUSE_DURATION second(s) after every $PAUSE_INTERVAL records"

for ((i=1; i<=$TOTAL_RECORDS; i++))
do
    # Generate a random partition key
    PARTITION_KEY=$RANDOM$RANDOM  # Combining two $RANDOM calls for more randomness
    
    # Send record to Kinesis
    aws kinesis put-record \
        --stream-name "$STREAM_NAME" \
        --data "$DATA" \
        --cli-binary-format raw-in-base64-out \
        --partition-key "$PARTITION_KEY" 

    
    # Increment counter
    count=$((count + 1))
    
    # Display progress
    if [ $((i % 100)) -eq 0 ]; then
        echo "Sent $i records..."
    fi
    
    # Pause after sending PAUSE_INTERVAL records
    if [ $((count % PAUSE_INTERVAL)) -eq 0 ]; then
        echo "Pausing for $PAUSE_DURATION second(s) after sending $count records..."
        sleep $PAUSE_DURATION
    fi
done

echo "Completed sending $TOTAL_RECORDS records to Kinesis stream: $STREAM_NAME"
