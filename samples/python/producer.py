from libvegas import Producer

p = Producer("test")
for i in range(10):
    p.send("python-client-" + str(i), b"hello from python")
