package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/buddhike/pebble/consumer"
)

var (
	stream            string
	streamConsumerArn string
	managerID         int
)

func init() {
	flag.StringVar(&stream, "stream", "", "Name of KDS stream")
	flag.StringVar(&streamConsumerArn, "stream-consumer-arn", "", "EFO consumer ARN")
	flag.IntVar(&managerID, "manager-id", 1, "Manager ID for clustering")
}

func main() {
	flag.Parse()

	processFn := func(record types.Record) {
		fmt.Printf("Processing record: %s\n", *record.PartitionKey)
	}
	c := consumer.NewDevelopmentConsumer("my-consumer", stream, streamConsumerArn, processFn, consumer.WithManagerID(managerID), consumer.WithManagerUrls("http://localhost:13001,http://localhost:13002,http://localhost:13003"), consumer.WithEtcdClientUrls("http://localhost:12001,http://localhost:12002,http://localhost:12003"), consumer.WithEtcdPeerUrls("http://localhost:11001,http://localhost:11002,http://localhost:11003"))

	// Start the consumer
	err := c.Start()
	if err != nil {
		panic(err)
	}

	notifyChan := make(chan os.Signal, 1)
	signal.Notify(notifyChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-notifyChan
		c.Stop()
	}()

	// Wait for the consumer to finish
	<-c.Done()
}
