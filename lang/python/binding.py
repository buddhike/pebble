import cffi

ffi = cffi.FFI()
ffi.cdef("""
typedef struct {
    int bufferSize;
    int batchSize;
    int batchTimeoutMS;
}ProducerConfig;

typedef signed char GoInt8;
typedef unsigned char GoUint8;
typedef short GoInt16;
typedef unsigned short GoUint16;
typedef int GoInt32;
typedef unsigned int GoUint32;
typedef long long GoInt64;
typedef unsigned long long GoUint64;
typedef GoInt64 GoInt;
typedef GoUint64 GoUint;
typedef size_t GoUintptr;
typedef float GoFloat32;
typedef double GoFloat64;
typedef float _Complex GoComplex64;
typedef double _Complex GoComplex128;
extern ProducerConfig NewProducerConfig();
extern GoInt NewProducer(char* streamName, ProducerConfig cfg);
extern GoInt Send(GoInt producer, char* partitionKey, char* data, int n);
         """)

lib = ffi.dlopen("../../core/build/libvegas.so")

class ProducerConfig:
    def __init__(self):
        c = lib.NewProducerConfig()
        self._buffer_size = c.bufferSize
        self._batch_size = c.batchSize
        self._batch_timeout_ms = c.batchTimeoutMS

    @property
    def buffer_size(self):
        return self._buffer_size

    @buffer_size.setter
    def buffer_size(self, v):
        self._buffer_size = v

    @property
    def batch_size(self):
        return self._batch_size

    @batch_size.setter
    def batch_size(self, v):
        self._batch_size = v

    @property
    def batch_timeout_ms(self):
        return self._batch_timeout_ms

    @batch_timeout_ms.setter
    def batch_timeout_ms(self, v):
        self._batch_timeout_ms = v

class Producer:
    def __init__(self, streamName: str, config: ProducerConfig):
        c = lib.NewProducerConfig()
        c.bufferSize = config.buffer_size
        c.batchSize = config.batch_size
        c.batchTimeoutMS = config.batch_timeout_ms
        self._instanceID = lib.NewProducer(streamName.encode(), c)
        if self._instanceID < 0:
            raise Exception("Failed to initialise producer")
        

    def send(self, partitionKey: str, data: bytes):
        lib.Send(self._instanceID, partitionKey.encode(), data, len(data))
