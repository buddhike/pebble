package main

import "C"

import (
	"context"
	"sync"
	"unsafe"

	vegas "github.com/buddhike/vegas/client"
)

var producers []*vegas.Producer
var consumers []*vegas.Consumer
var mut *sync.Mutex

func init() {
	producers = make([]*vegas.Producer, 0)
	consumers = make([]*vegas.Consumer, 0)
	mut = &sync.Mutex{}
}

//export NewProducer
func NewProducer(streamName *C.char, bufferSize, batchSize, batchTimeoutMS int) int {
	mut.Lock()
	defer mut.Unlock()
	p, err := vegas.NewProducer(C.GoString(streamName), bufferSize, batchSize, batchTimeoutMS)
	if err != nil {
		return 0
	}
	producers = append(producers, p)
	return len(producers) - 1
}

//export Send
func Send(producer int, partitionKey *C.char, data *C.char, n C.int) int {
	mut.Lock()
	defer mut.Unlock()
	p := producers[producer]
	err := p.Send(context.TODO(), C.GoString(partitionKey), C.GoBytes(unsafe.Pointer(data), n))
	if err != nil {
		return 1
	}
	return 0
}

// cgo requirement
func main() {}
