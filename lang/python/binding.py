import cffi

ffi = cffi.FFI()
ffi.cdef("""

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
extern GoInt NewProducer(char* streamName, GoInt bufferSize, GoInt batchSize, GoInt batchTimeoutMS);
extern GoInt Send(GoInt producer, char* partitionKey, char* data, int n);

         """)

lib = ffi.dlopen("../../core/build/libvegas.so")
id = lib.NewProducer(b"test", 1, 1, 10)
print(id)
lib.Send(id, b"python", b"data", 4)