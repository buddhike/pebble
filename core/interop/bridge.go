package main

/*
typedef struct {
    int bufferSize;
    int batchSize;
    int batchTimeoutMS;
}ProducerConfig;

typedef struct {
	char* partitionKey;
	char* data;
	int length;
}UserRecord;

typedef void Callback(UserRecord r, void* h);
static void CallbackBridge(Callback* cb, UserRecord r, void* h) {
	cb(r, h);
}
*/
import "C"

import (
	"context"
	"sync"
	"unsafe"

	vegas "github.com/buddhike/vegas/client"
	"github.com/buddhike/vegas/client/pb"
)

var producers []*vegas.Producer
var consumers []*vegas.Consumer
var mut *sync.Mutex

func init() {
	producers = make([]*vegas.Producer, 0)
	consumers = make([]*vegas.Consumer, 0)
	mut = &sync.Mutex{}
}

//export NewProducerConfig
func NewProducerConfig() C.ProducerConfig {
	c := vegas.DefaultProducerConfig
	return C.ProducerConfig{
		bufferSize:     C.int(c.BufferSize),
		batchSize:      C.int(c.BatchSize),
		batchTimeoutMS: C.int(c.BatchTimeoutMS),
	}
}

//export NewProducer
func NewProducer(streamName *C.char, cfg C.ProducerConfig) int {
	mut.Lock()
	defer mut.Unlock()
	p, err := vegas.NewProducer(C.GoString(streamName), vegas.ProducerConfig{
		BufferSize:     int(cfg.bufferSize),
		BatchSize:      int(cfg.batchSize),
		BatchTimeoutMS: int(cfg.batchTimeoutMS),
	})
	if err != nil {
		return -1
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

//export NewConsumer
func NewConsumer(streamName, efoARN *C.char, callback *C.Callback, handle unsafe.Pointer) int {
	mut.Lock()
	defer mut.Unlock()

	c, err := vegas.NewConsumer(C.GoString(streamName), C.GoString(efoARN), func(ur *pb.UserRecord) error {
		cur := C.UserRecord{
			partitionKey: C.CString(ur.PartitionKey),
			data:         (*C.char)(unsafe.Pointer(&ur.Data[0])),
			length:       C.int(len(ur.Data)),
		}
		C.CallbackBridge(callback, cur, handle)
		return nil
	})
	if err != nil {
		return -1
	}
	consumers = append(consumers, c)
	return len(consumers) - 1
}

//export WaitForConsumer
func WaitForConsumer(consumer int) {
	mut.Lock()
	c := consumers[consumer]
	mut.Unlock()
	<-c.Done()
}

//export ReleaseProducer
func ReleaseProducer(i int) {
	mut.Lock()
	defer mut.Unlock()
	producers = append(producers[0:i], producers[i+1:]...)
}

// cgo requirement
func main() {}
