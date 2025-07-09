package main

import (
	"flag"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/consumer"
)

var (
	stream            string
	streamConsumerArn string
)

func init() {
	flag.StringVar(&stream, "stream", "", "Name of KDS stream")
	flag.StringVar(&streamConsumerArn, "stream-consumer-arn", "", "EFO consumer ARN")
}

func main() {
	flag.Parse()

	c := consumer.NewConsumer("my-consumer", stream, streamConsumerArn, func(record types.Record) {
		fmt.Printf("Processing record: %s\n", *record.PartitionKey)
	})

	// Start the consumer
	err := c.Start()
	if err != nil {
		panic(err)
	}

	// Wait for the consumer to finish
	<-c.Done()
}
