package main

import (
	"context"
	"fmt"

	"github.com/buddhike/vegas"
	"github.com/buddhike/vegas/pb"
)

func main() {
	streamName := "test"
	p, err := vegas.NewProducer(streamName, 100, 10, 1000)
	if err != nil {
		panic(err)
	}
	go func() {
		p.Send(context.TODO(), "a", []byte("a"))
		p.Send(context.TODO(), "a", []byte("a"))
	}()
	go func() {
		c, err := vegas.NewConsumer(streamName, "", func(ur *pb.UserRecord) error {
			fmt.Println("PK: ", ur.PartitionKey, " Value: ", string(ur.Data))
			return nil
		})
		if err != nil {
			panic(err)
		}
		<-c.Done()
	}()
	fmt.Println("Producer started")
	<-p.Done()
}
