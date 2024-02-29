import vegas

p = vegas.Producer("test")
for i in range(10):
    p.send("python-client-" + str(i), b"hello from python")
