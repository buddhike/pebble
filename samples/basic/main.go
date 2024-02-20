package main

import (
	"context"
	"fmt"
	"sync"

	vegas "github.com/buddhike/vegas/client"
	"github.com/buddhike/vegas/client/pb"
)

func main() {
	streamName := "test"
	p, err := vegas.NewProducer(streamName, vegas.DefaultProducerConfig)
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		efo := "arn:aws:kinesis:ap-southeast-2:767660010185:stream/test/consumer/python-consumer:1686199962"
		c, err := vegas.NewConsumer(streamName, efo, func(ur *pb.UserRecord) error {
			fmt.Println("PK: ", ur.PartitionKey, " Value: ", string(ur.Data))
			return nil
		})
		if err != nil {
			panic(err)
		}
		wg.Done()
		<-c.Done()
	}()
	wg.Wait()
	go func() {
		// Make this continuous
		// Use a random generator for data
		// Simulate normal use case and duplicates both
		p.Send(context.TODO(), "a", []byte("a"))
		p.Send(context.TODO(), "a", []byte("a"))
		p.Send(context.TODO(), "b", []byte("b"))
	}()
	fmt.Println("Producer started")
	<-p.Done()
}
