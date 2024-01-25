package main

import (
	"fmt"

	"github.com/buddhike/vegas"
)

func main() {
	p, err := vegas.NewProducer("test", 100, 10)
	if err != nil {
		panic(err)
	}
	fmt.Println("Producer started")
	<-p.Done()
}
