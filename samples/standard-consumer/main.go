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
	popURLs           string
	streamName        string
	streamConsumerArn string
)

func init() {
	flag.StringVar(&popURLs, "pop-urls", "", "pop urls")
	flag.StringVar(&streamName, "stream-name", "", "stream name")
	flag.StringVar(&streamConsumerArn, "stream-consumer-arn", "", "efo consumer ARN")
}

func main() {
	flag.Parse()

	processFn := func(record types.Record) {
		fmt.Printf("Processing record: %s\n", *record.PartitionKey)
	}

	c := consumer.MustNewConsumer("my-consumer", streamName, streamConsumerArn, popURLs, processFn)

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
