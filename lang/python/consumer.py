import vegas 

def callback(ur):
    print(str(ur.partitionKey) + ":" + ur.data.decode("utf-8"))

stream = "test"
efo = "arn:aws:kinesis:ap-southeast-2:767660010185:stream/test/consumer/python-consumer:1686199962"
c = vegas.Consumer(stream, efo, callback)
c.join()