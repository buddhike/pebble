import time
from libvegas import Producer, Consumer

def process_record(r):
    s = float(r.data.decode("utf-8"))
    e = time.time()
    print("Propagation delay: ", e - s, " seconds")

stream = "test"
efo = "arn:aws:kinesis:ap-southeast-2:767660010185:stream/test/consumer/python-consumer:1686199962"
c = Consumer(stream, efo, process_record)

p = Producer("test")
for i in range(10):
    p.send("time:" + str(i), str(time.time()).encode())

input("Press ctrl+c to stop\n")
