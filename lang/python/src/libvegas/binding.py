import cffi
import platform
from os import path

ffi = cffi.FFI()
ffi.cdef("""
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
extern char* NewProducer(char* streamName, ProducerConfig cfg, int* id);
extern char* Send(GoInt producer, char* partitionKey, char* data, int n);
extern char* NewConsumer(char* streamName, char* efoARN, Callback* callback, void* handle, int* id);
extern void WaitForConsumer(GoInt consumer);
extern void ReleaseProducer(GoInt i);

    """)

os = platform.system().lower() 
arch = platform.machine().lower()

if arch == "x86_64": 
    arch = "amd64"

lib_name = f'{os}_{arch}_libvegas.so'
lib_path = path.join(path.dirname(__file__), "libs", lib_name)
lib = ffi.dlopen(lib_path)

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
    def __init__(self, streamName: str, config: ProducerConfig = None):
        c = lib.NewProducerConfig()
        if not config == None:
            c.bufferSize = config.buffer_size
            c.batchSize = config.batch_size
            c.batchTimeoutMS = config.batch_timeout_ms
        id = ffi.new("int *")    
        err = lib.NewProducer(streamName.encode(), c, id)
        if err != ffi.NULL:
            raise Exception(ffi.string(err))
        self._instanceID = int(id[0])
        

    def send(self, partitionKey: str, data: bytes):
        lib.Send(self._instanceID, partitionKey.encode(), data, len(data))

    def close(self):
        lib.ReleaseProducer(self._instanceID)


@ffi.callback("void(UserRecord, void*)")
def consumer_callback(ur, handle):
    ffi.from_handle(handle)._callback(ur)

class UserRecord:
    def __init__(self, partitionKey, data):
        self._partitionKey = partitionKey
        self._data = data
    
    @property
    def partitionKey(self):
        return self._partitionKey

    @property
    def data(self):
        return self._data

class Consumer:
    def __init__(self, stream_name: str, efo_arn: str, cb):
        handle = ffi.new_handle(self)
        self._handle = handle
        self._cb = cb
        id = ffi.new("int*")
        err = lib.NewConsumer(stream_name.encode(), efo_arn.encode(), consumer_callback, handle, id)
        if err != ffi.NULL:
            raise Exception(ffi.string(err))
        self._id = int(id[0])

    def join(self):
        lib.WaitForConsumer(self._id)

    def close(self):
        pass

    def _callback(self, ur):
        self._cb(UserRecord(ffi.string(ur.partitionKey), ffi.buffer(ur.data, ur.length)[:]))